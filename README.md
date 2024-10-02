# dramatiq-sentinel

Support of [Redis Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) setup with the [dramatiq](https://dramatiq.io/) distributed task processing library for Python.  

## Software Design
This dramatiq middleware will monitor Redis master switch events and automatically reconnect to the new master selected by Redis Sentinels.  
The middlware listens for (subscribe to) `+switch-master` events published by the Sentinel instances but also (optionally) periodically checks for the current selected Redis master (in case of any missed event).  
When Redis master switch is detected, the service will first pause all dramatiq workers, then reset/update the dramatiq Redis broker connection and finally resume the dramatiq workers.  
> Note: if pausing dramatiq workers timeouts, the service will forcefully exit to avoid any dead locks.

## Installation

`pip install git+https://github.com/huwylphimet/dramatiq-sentinel.git@v1.0.1`

## Quickstart

```python
import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq_sentinel.middleware.sentinel import Sentinel as DramatiqSentinel

broker = RedisBroker()

# Setup dramatiq-sentinel middleware
dramatiq_sentinel = DramatiqSentinel(
    sentinels=[                                  # List of Sentinel instances to connect to.
        ("localhost", 26379),                    # Redis Sentinel 1
        ("localhost", 26380),                    # Redis Sentinel 2
        ("localhost", 26381),                    # Redis Sentinel 3
    ],
    sentinel_password="sentinel-passsword",      # Password used for authentication with the Sentinel servers
    socket_timeout=0.5,                          # Timeout (in seconds) for socket operations (like connecting or reading from the server), default 0.5 seconds
    service_name="redis-sentinel-service-name",  # Name of the Redis master service that the Sentinel cluster is monitoring (default "redis")
    min_backoff=3.0,                             # Min delay (in seconds) before restarting Redis Sentinel events listener on connection error (default 3.0 seconds)
    max_backoff=60.0,                            # Max delay (in seconds) before restarting Redis Sentinel events listener on connection error (default 60.0 seconds)
    backoff_factor=2.0,                          # Multiplier factor used to increase the backoff time after each failed attempt to connect to the Redis Sentinel server (default 2.0 seconds)
    periodic_master_check=60.0,                  # Optional: Interval (in seconds) at which the system periodically (additionally to listeners) checks the master status of the Redis cluster using Redis Sentinel (default None = disabled)
    worker_pausing_timeout_before_exit=600.0,    # Maximum amount of time (in seconds) that the service will wait for all Dramatiq workers to pause during a Redis master switch event before forcefully exiting (deault 10 minutes)
    nat={                                        # Optional: Address mapping for Redis and Sentinel instances when the service and the Redis components are running on different networks,
                                                 # particularly in cases where Network Address Translation (NAT) is involved (e.g. localhost + Docker containers)
        "localhost:6379": "172.20.0.2:6379",     # Redis1
        "localhost:6380": "172.20.0.3:6379",     # Redis2
        "localhost:6381": "172.20.0.4:6379",     # Redis3
        "localhost:26379": "172.20.0.5:26379",   # Redis Sentinel 1
        "localhost:26380": "172.20.0.6:26379",   # Redis Sentinel 2
        "localhost:26381": "172.20.0.7:26379",   # Redis Sentinel 3
    },
)

broker.add_middleware(dramatiq_sentinel)
dramatiq.set_broker(broker)


@dramatiq.actor
def my_distributed_task():
    pass


if __name__ == "__main__":
    message = my_distributed_task.send()

```
