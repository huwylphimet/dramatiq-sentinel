import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Thread
from typing import Callable, Dict, List

from dramatiq import Middleware, Worker, get_logger
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import SkipMessage
from redis import Redis, RedisError
from redis import Sentinel as RedisSentinel
from redis.sentinel import MasterNotFoundError


class Sentinel(Middleware):
    MIN_BACKOFF = 3.0  # Delay before restarting Redis Sentinel events listener on connection error
    MAX_BACKOFF = 60.0
    BACKOFF_FACTOR = 2.0
    WORKER_PAUSING_TIMEOUT_BEFORE_EXIT = 600.0  # 10 minutes

    def __init__(
        self,
        sentinels: list[tuple[str, int]] = [("localhost", 26379)],
        sentinel_password: str = None,
        socket_timeout: float = 0.5,
        service_name: str = "redis",
        min_backoff: float = MIN_BACKOFF,
        max_backoff: float = MAX_BACKOFF,
        backoff_factor: float = BACKOFF_FACTOR,
        periodic_master_check: float | None = None,
        worker_pausing_timeout_before_exit: float = WORKER_PAUSING_TIMEOUT_BEFORE_EXIT,
        nat: dict[tuple[str, int], tuple[str, int]] = {},  # usefull in case redis instances are behind an other network (e.g. docker)
    ) -> None:
        super().__init__()

        self.broker: RedisBroker = None
        self.worker: Worker = None

        self.logger = get_logger(__name__.replace("_sentinel", ""))

        self._event_listeners: Dict[str, List[Callable]] = {}

        # Create a Redis connection to each Sentinel
        self.sentinels = {}
        for s in sentinels:
            self.sentinels[s] = Redis(host=s[0], port=s[1], password=sentinel_password) if sentinel_password else Redis(host=s[0], port=s[1])
        self.socket_timeout = max(float(socket_timeout), 0.1)
        self.sentinels_service_name = service_name
        self.min_backoff = max(float(min_backoff), 1.0)
        self.max_backoff = min(float(max_backoff), 86400.0)
        self.backoff_factor = max(float(backoff_factor), 1.1)
        self.periodic_master_check = None
        if periodic_master_check:
            self.periodic_master_check = max(float(periodic_master_check), 0.5)

        # Allow at least 2 minutes waiting time for pausing worker before exit
        self.worker_pausing_timeout_before_exit = max(float(worker_pausing_timeout_before_exit), 120.0)

        self.nat = nat

        # Initialize Sentinel with all configured Sentinels
        self.sentinel = RedisSentinel(self.sentinels.keys(), sentinel_kwargs={"password": sentinel_password}, socket_timeout=self.socket_timeout)
        self.last_discovered_redis_master = None

        self.log_redis_status()
        self.log_sentinel_status()

        self._switching_master = False
        self._start_sentinel_listener_threads()
        if self.periodic_master_check:
            self._start_periodic_master_check_thread()

    def before_worker_boot(self, broker, worker):
        """Called before the worker process starts up."""
        if not self.broker:
            self.broker = broker

        self.worker = worker

    def after_declare_queue(self, broker, queue_name):
        """
        Called after a queue has been declared.
        Store broker here too in case no worker is used.
        """
        if not self.broker:
            self.broker = broker

    def before_enqueue(self, broker, message, delay):
        """Called before a message is enqueued."""
        if not self.last_discovered_redis_master:
            return

        if self.get_redis_broker_host(broker) != self.last_discovered_redis_master:
            self.logger.debug(f"Resetting Redis broker connection before enqueueing message {message} ...")
            self.reset_borker_connection(broker, self.last_discovered_redis_master)

    def before_delay_message(self, broker, message):
        """Called before a message has been delayed in worker memory."""
        if not self.last_discovered_redis_master:
            return

        if self.get_redis_broker_host(broker) != self.last_discovered_redis_master:
            self.logger.debug(f"Resetting Redis broker connection before delaying message {message} ...")
            self.reset_borker_connection(broker, self.last_discovered_redis_master)

    def before_ack(self, broker, message):
        """
        Called before a message is acknowledged.
        Reset broker from the worker post_process_message() loop on Redis broker host change.
        """
        if not self.last_discovered_redis_master:
            return

        if self.get_redis_broker_host(broker) != self.last_discovered_redis_master:
            self.logger.debug(f"Resetting Redis broker connection before acknowledging message {message} ...")
            self.reset_borker_connection(broker, self.last_discovered_redis_master)

    def before_nack(self, broker, message):
        """
        Called before a message is rejected.
        Reset broker from the worker post_process_message() loop on Redis broker host change.
        """
        if not self.last_discovered_redis_master:
            return

        if self.get_redis_broker_host(broker) != self.last_discovered_redis_master:
            self.logger.debug(f"Resetting Redis broker connection before rejecting message {message} ...")
            self.reset_borker_connection(broker, self.last_discovered_redis_master)

    def before_process_message(self, broker, message):
        """Called before a message is processed.

        Raises:
            SkipMessage: If the current message should be skipped.  When
            this is raised, ``after_skip_message`` is emitted instead
            of ``after_process_message``.
        """
        if self._switching_master:
            raise SkipMessage

    # def before_delay_message(self, broker, message):
    #     """Called before a message has been delayed in worker memory."""
    #     if self._switching_master:
    #         raise SkipMessage

    @property
    def switching_master(self) -> bool:
        return self._switching_master

    def subscribe_to_event(self, event_name: str, callback: Callable) -> None:
        """Subscribe a callback function to an event."""
        if event_name not in self._event_listeners:
            self._event_listeners[event_name] = []
        self._event_listeners[event_name].append(callback)

    def emit_event(self, event_name: str, *args, **kwargs) -> None:
        """Emit an event and notify all subscribed listeners."""
        if event_name in self._event_listeners:
            for callback in self._event_listeners[event_name]:
                callback(*args, **kwargs)

    def get_redis_master(self, username: str = None, password: str = None, database=0, max_iterations: int = None) -> Redis:
        """
        Sets up a Redis client connected to the master discovered via multiple Sentinels.
        Includes all Sentinel instances in the setup and retries if no master is discovered initially.
        This method can be used for instantiating a RedisBroker before calling dramatiq.set_broker().

        Args:
            max_iterations (int, optional): Maximum number of retry iterations for discovering a master.
                                            If None, will retry indefinitely.

        Returns:
            Redis: The Redis client connected to the current master.
        """
        delay = self.min_backoff  # Initial delay for retry
        iterations = 0
        while not self._has_reached_max_iterations(iterations, max_iterations):
            redis_master = self.discover_redis_master()
            if redis_master:
                try:
                    # Use the successfully discovered Redis master
                    redis_client = Redis(host=redis_master[0], port=redis_master[1], db=int(database), username=username, password=password)
                    if redis_client.ping():
                        return redis_client  # Exit loop if master is successfully discovered and connected

                except Exception as e:
                    self.logger.error(f"Failed to connect to the discovered Redis master {self.stringify_socket_tuple(redis_master)}: {e}")

            # Exponential backoff before retrying
            self.logger.warning("No Sentinels were successfully connected or no Redis master found. Retrying in %.2f seconds...", delay)
            time.sleep(delay)
            delay = min(delay * self.backoff_factor, self.max_backoff)

            iterations = self._increment_iterations(iterations, max_iterations)

    def discover_redis_master(self) -> tuple | None:
        """Attempts to discover the Redis master through all Sentinels."""
        try:
            self.last_discovered_redis_master = self.get_nat(self.sentinel.discover_master(self.sentinels_service_name))
            self.logger.debug(
                f"Successfully discovered Redis master {self.stringify_socket_tuple(self.last_discovered_redis_master)}"
                + f" from Sentinels (service name: '{self.sentinels_service_name}')"
            )
            return self.last_discovered_redis_master

        except MasterNotFoundError as e:
            self.logger.warning(f"{e}")

        except Exception as e:
            self.logger.error(f"Unexpected Sentinel error: {e}")

    def _start_sentinel_listener_threads(self):
        """
        Starts a separate thread to listen for Redis Sentinel events on each Sentinel instance.
        """
        self.logger.info(f"Starting Redis Sentinel listeners ({len(self.sentinels)})...")
        self.broker_update_lock = Lock()
        for sentinel_host, sentinel_port in self.sentinels.keys():
            Thread(
                daemon=True,
                target=self._listen_to_sentinel,
                args=(
                    sentinel_host,
                    sentinel_port,
                    self.min_backoff,
                    self.max_backoff,
                    self.backoff_factor,
                ),
                name=f"SentinelListenerThread({self.stringify_socket_tuple((sentinel_host, sentinel_port))})",
            ).start()

    def _start_periodic_master_check_thread(self):
        self.logger.info(f"Starting periodic Redis master check (delay: {self.periodic_master_check} seconds)...")
        Thread(
            daemon=True,
            target=self._periodic_master_check,
            args=(self.periodic_master_check,),
            name="PeriodicMasterCheckThread",
        ).start()

    def _listen_to_sentinel(
        self,
        host: str,
        port: int,
        min_backoff: float = 2.0,
        max_backoff: float = 60.0,
        backoff_factor: float = 2.0,
        max_iterations: int = None,  # for unit tests
    ) -> None:
        """
        Listen to Redis Sentinel events in a separate thread and updates the Redis master connection
        in the Dramatiq broker if a master switch is detected.

        This method connects to the specified Redis Sentinel instance and subscribes to the `+switch-master`
        channel. When a master switch event occurs, it updates the Redis connection used by the Dramatiq broker
        to ensure continued operation with the new master.

        Args:
            host (str): The hostname or IP address of the Redis Sentinel instance.
            port (int): The port number of the Redis Sentinel instance.
            min_backoff (float): The initial delay before restarting the listener after a connection failure.
            max_backoff (float): The maximum delay for the exponential backoff.
            backoff_factor (float): The factor by which the delay increases after each retry.
            max_iterations (int, optional): Maximum number of iterations for the loop, used in testing.

        Returns:
            None: This method does not return a value. It runs indefinitely in its own thread.

        Side Effects:
            - Logs significant events, such as connection issues or master switches.
            - Updates the Redis master connection in the Dramatiq broker upon detecting a switch-master event.
        """
        delay = min_backoff
        iterations = 0

        while not self._has_reached_max_iterations(iterations, max_iterations):
            try:
                _, pubsub = self._connect_and_subscribe_to_sentinel(host, port)
                delay = min_backoff  # Reset delay after successful connection

                for message in pubsub.listen():
                    channel = message.get("channel", b"").decode("utf-8")
                    if message.get("type") == "message" and channel == "+switch-master":
                        self._handle_sentinel_master_switch_message((host, port), message)
                    elif message.get("type") == "subscribe":
                        self.logger.info(f"Subscribed to '{channel}' on Redis Sentinel {self.stringify_socket_tuple((host, port))}, waiting for events...")

                iterations = self._increment_iterations(iterations, max_iterations)

            except RedisError as e:
                quorum = self._check_sentinel_quorum()
                self.logger.warning(
                    f"Error in Redis Sentinel event listener {self.stringify_socket_tuple((host, port))}"
                    + f" (quorum: {quorum.get("quorums")}/{quorum.get("available_sentinels")}): {e}"
                )

                self.logger.info(f"Restarting Redis Sentinel event listener for {self.stringify_socket_tuple((host, port))} in %0.2f seconds.", delay)
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_backoff)
                iterations = self._increment_iterations(iterations, max_iterations)
                self.log_sentinel_status()

            except Exception as e:
                self.logger.critical(f"Unexpected error in Redis Sentinel event listener {self.stringify_socket_tuple((host, port))}: {e}", exc_info=True)
                break  # Exit loop on unexpected errors

    def _periodic_master_check(self, delay: float = 10.0, max_iterations: int = None):  # for unit tests
        iterations = 0
        last_check = time.time()
        time.sleep(delay)
        while not self._has_reached_max_iterations(iterations, max_iterations):
            if time.time() - last_check >= delay:
                last_check = time.time()
                self.logger.debug("Periodic checking Redis master...")
                self._check_and_update_sentinel_redis_master()

            time.sleep(delay)
            iterations = self._increment_iterations(iterations, max_iterations)

    def _has_reached_max_iterations(self, iterations: int, max_iterations: int) -> bool:
        """Checks if the maximum number of iterations has been reached."""
        return max_iterations is not None and iterations >= max_iterations

    def _increment_iterations(self, iterations: int, max_iterations: int) -> int:
        """Increments the iteration count if max_iterations is set."""
        if max_iterations:
            iterations += 1
        return iterations

    def _connect_and_subscribe_to_sentinel(self, host: str, port: int):
        """Connects to the Sentinel instance and subscribes to the '+switch-master' channel."""
        self.logger.debug(f"Subscribing to '+switch-master' channel on Redis Sentinel {self.stringify_socket_tuple((host, port))}...")
        sentinel_instance = self.sentinels.get((host, port))
        if not sentinel_instance:
            return
        pubsub = sentinel_instance.pubsub()
        pubsub.subscribe("+switch-master")
        return sentinel_instance, pubsub

    def _handle_sentinel_master_switch_message(self, sentinel: tuple[str, int], message: dict[str, str]) -> bool:
        data = message.get("data", b"").decode("utf-8").split()
        if len(data) < 5:
            self.logger.warning(f"Invalid Redis Sentinel event, message: {message}")
            return False

        old_master = self.get_nat((data[1], int(data[2])))
        new_master = self.get_nat((data[3], int(data[4])))
        self.logger.warning(
            f"Redis master switch detected from Sentinel {self.stringify_socket_tuple(sentinel)}!"
            + f" [old master: {self.stringify_socket_tuple(old_master)}, new master: {self.stringify_socket_tuple(new_master)}]"
        )

        self.emit_event(
            "redis_master_switch_detected",
            self.stringify_socket_tuple(old_master),
            self.stringify_socket_tuple(new_master),
            self.stringify_socket_tuple(sentinel),
        )

        current_redis = self.get_redis_broker_host(self.broker)
        if current_redis != new_master:
            self._check_and_update_sentinel_redis_master(True)
        else:
            self.logger.info(f"Current Redis host already set to {self.stringify_socket_tuple(current_redis)}, switching Redis master not needed.")
        return True

    def _check_and_update_sentinel_redis_master(self, is_sentinel_event=False) -> bool:
        """Get the current Redis master and update the Redis broker accordingly."""
        if not self.broker or not isinstance(self.broker, RedisBroker):
            self.logger.warning(f"Broker not yet set or not a {RedisBroker.__qualname__}")
            return False

        with self.broker_update_lock:
            # (Re-)call discover_master() after lock has been acquired
            discovered_master = self.discover_redis_master()
            if not discovered_master:
                return False

            # Update the broker if master has changed
            current_redis = self.get_redis_broker_host(self.broker)
            if current_redis == discovered_master:
                msg = f"Current Redis host already set to {self.stringify_socket_tuple(current_redis)}, switching Redis master not needed."
                if is_sentinel_event:
                    self.logger.info(msg)
                else:
                    self.logger.debug(msg)
                return True

            self.log_redis_status()

            self._switching_master = True
            self.logger.warning(f"Switching Redis from {self.stringify_socket_tuple(current_redis)} to {self.stringify_socket_tuple(discovered_master)}...")
            if not self.pause_worker(self.worker_pausing_timeout_before_exit, exit_on_timeout=True):
                return False

            old_host = self.get_redis_broker_host(self.broker)
            self.emit_event("before_redis_connection_reset", self.stringify_socket_tuple(old_host), self.stringify_socket_tuple(discovered_master))
            self.logger.debug(f"Resetting Redis connection to {self.stringify_socket_tuple(discovered_master)}...")
            self.reset_borker_connection(self.broker, discovered_master)
            self.emit_event("after_redis_connection_reset", self.stringify_socket_tuple(old_host), self.stringify_socket_tuple(discovered_master))

            ping = self.redis_ping()
            if ping:
                self.logger.warning(f"Dramatiq broker updated to new Redis master {self.stringify_socket_tuple(discovered_master)}.")
                self.resume_worker()
            self._switching_master = False
            return ping

    def redis_ping(self) -> bool:
        if not self.broker or not isinstance(self.broker.client, Redis):
            return False

        host = self.get_redis_broker_host(self.broker)
        try:
            ping = self.broker.client.ping()
            if ping:
                self.logger.info(f"Redis host {self.stringify_socket_tuple(host)} PING successful")
            else:
                self.logger.warning(f"Redis host {self.stringify_socket_tuple(host)} PING unsuccessful")
            return ping
        except Exception as e:
            self.logger.error(f"Redis host {self.stringify_socket_tuple(host)} PING unsuccessful: {e}")
            return False

    def _check_sentinel_quorum(self) -> dict:
        available_sentinels = 0
        nb_quorums = 0
        for sentinel in self.sentinels.values():
            try:
                resp = sentinel.execute_command(f"SENTINEL CKQUORUM {self.sentinels_service_name}")
                available_sentinels += 1
                if "OK" in resp.decode("utf-8"):
                    nb_quorums += 1

            except Exception:
                continue

        return {"quorums": nb_quorums, "available_sentinels": available_sentinels}

    def log_redis_status(self, sentinel_hosts: list[tuple[str, int]] = []):
        redis_hosts_status = self.get_sentinel_redis_hosts_status(sentinel_hosts)
        self.logger.info(f"Redis hosts available: {redis_hosts_status.get("total_hosts")}")
        self.logger.debug(f"Redis hosts status: {redis_hosts_status.get("hosts_status")}")

    def log_sentinel_status(self):
        sentinels_status = self.get_sentinels_status()
        self.logger.info(f"Sentinel hosts available: {sentinels_status.get("total_sentinels")}")
        self.logger.debug(f"Sentinel hosts status: {sentinels_status.get("sentinels_status")}")

    def get_sentinel_redis_hosts_status(self, sentinel_hosts: list[tuple[str, int]] = []) -> dict:
        """Get detailed status of Redis master and replicas."""
        available_hosts = 0

        sentinel_hosts = sentinel_hosts or self.sentinels.keys()
        filtered_sentinels: set[Redis] = (self.sentinels[key] for key in sentinel_hosts if key in self.sentinels)
        master_status = {}
        for sentinel in filtered_sentinels:
            try:
                # Get the current master information
                master_info = sentinel.sentinel_master(self.sentinels_service_name)

                status = master_info.get("flags")
                if status:
                    available_hosts += 1
                host = self.get_nat((master_info["ip"], int(master_info["port"])))
                master_status = {
                    "host": host[0],
                    "port": host[1],
                    "role": "master",
                    "status": "available" if status else "unavailable",
                    "last_ok_ping_reply": master_info.get("last-ok-ping-reply", "N/A"),
                    "num-slaves": master_info.get("num-slaves", "N/A"),
                    "num-other-sentinels": master_info.get("num-other-sentinels", "N/A"),
                    "quorum": master_info.get("quorum", "N/A"),
                    "down_after_milliseconds": master_info.get("down-after-milliseconds", "N/A"),
                    "failover-timeout": master_info.get("failover-timeout", "N/A"),
                }
                break  # Get info from first available Sentinel

            except Exception:
                continue  # Ignore sentinels that are not available

        replicas_status = []
        treated_replicas = set()
        for sentinel in filtered_sentinels:
            try:
                # Get the replicas information
                replicas_info = sentinel.sentinel_slaves(self.sentinels_service_name)

                for replica in replicas_info:
                    if replica.get("runid") in treated_replicas:
                        continue
                    treated_replicas.add(replica.get("runid"))
                    status = "s_down" not in replica["flags"] and "disconnected" not in replica["flags"]
                    if status:
                        available_hosts += 1

                    host = self.get_nat((replica["ip"], int(replica["port"])))
                    replicas_status.append(
                        {
                            "host": host[0],
                            "port": host[1],
                            "role": "replica",
                            "status": "available" if status else "unavailable",
                            "last_ok_ping_reply": replica.get("last-ok-ping-reply", "N/A"),
                            "down_after_milliseconds": replica.get("down-after-milliseconds", "N/A"),
                            "master-link-status": replica.get("master-link-status", "N/A"),
                        }
                    )

                break  # Get info from first available Sentinel

            except Exception:
                continue  # Ignore sentinels that are not available

        # Combine master and replica statuses
        all_hosts_status = [master_status] + replicas_status

        return (
            {
                "total_hosts": f"{available_hosts}/{len(all_hosts_status)}",
                "hosts_status": all_hosts_status,
            }
            if available_hosts
            else {}
        )

    def get_sentinels_status(self) -> dict:
        """Get status of each Sentinel instance."""
        treated_sentinels = set()
        sentinels_status = []
        available_sentinels = 0

        for sentinel in self.sentinels.values():
            try:
                sentinels_info = sentinel.sentinel_sentinels(self.sentinels_service_name)

                for sentinel_info in sentinels_info:
                    if sentinel_info.get("runid") in treated_sentinels:
                        continue

                    status = "s_down" not in sentinel_info["flags"] and "disconnected" not in sentinel_info["flags"]
                    if status:
                        available_sentinels += 1
                    host = self.get_nat((sentinel_info["ip"], int(sentinel_info["port"])))
                    sentinels_status.append(
                        {
                            "host": host[0],
                            "port": host[1],
                            "status": "available" if status else "unavailable",
                            "last_ok_ping_reply": sentinel_info.get("last-ok-ping-reply", "N/A"),
                            "down_after_milliseconds": sentinel_info.get("down-after-milliseconds", "N/A"),
                        }
                    )
                    treated_sentinels.add(sentinel_info["runid"])

                # Do not break here and combine info from all available Sentinels

            except Exception:
                continue  # Ignore sentinels that are not available

        return (
            {
                "total_sentinels": f"{available_sentinels}/{len(sentinels_status)}",
                "sentinels_status": sentinels_status,
            }
            if available_sentinels
            else {}
        )

    def pause_worker(self, timeout, exit_on_timeout=False) -> bool:
        if not self.worker:
            return True

        with ThreadPoolExecutor() as executor:
            self.logger.info(f"Waiting ({timeout} seconds) for all worker threads to be paused...")
            future = executor.submit(self.worker.pause)
            try:
                future.result(timeout=timeout)
                self.logger.debug("All worker threads have been paused.")
                return True

            except TimeoutError as e:
                self.logger.warning(f"Timeout occured while pausing worker threads (after {timeout} seconds)! {e}")
                if exit_on_timeout:
                    self.logger.critical(f"Unable to pause worker threads after {self.worker_pausing_timeout_before_exit} seconds, shutting down!")
                    self.worker.stop(10000)  # Give 10s worker threads to shutdown
                    os._exit(69)  # Service Unavailable
                return False

            except Exception as err:
                self.logger.exception(f"{err}", exc_info=True)
                return False

    def resume_worker(self) -> None:
        if self.worker:
            self.logger.info("Resuming worker threads...")
            self.worker.resume()
            self.logger.debug("All worker threads have been resumed.")

    @staticmethod
    def reset_borker_connection(broker: RedisBroker, host: tuple[str, int]) -> None:
        """Reset Redis connection to provided host."""
        broker.client.connection_pool.connection_kwargs["host"] = host[0]
        broker.client.connection_pool.connection_kwargs["port"] = int(host[1])
        broker.client.connection_pool.reset()

    @staticmethod
    def get_redis_broker_host(broker) -> tuple[str, int]:
        return (broker.client.connection_pool.connection_kwargs["host"], int(broker.client.connection_pool.connection_kwargs["port"]))

    def get_nat(self, host: tuple[str, int]) -> tuple[str, int]:
        return self.nat.get(host, host)

    @staticmethod
    def stringify_socket_tuple(host: tuple[str, int]) -> str:
        return f"{host[0]}:{host[1]}"  # noqa: E231
