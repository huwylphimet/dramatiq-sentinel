import time
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest
from dramatiq.brokers.redis import RedisBroker
from redis import ConnectionError as RedisConnectionError
from redis import Redis
from redis import Sentinel as RedisSentinel

from dramatiq_sentinel.middleware.sentinel import Sentinel as DramatiqSentinel


@pytest.fixture
def mock_broker():
    # Mock the Redis connection pool
    connection_pool_mock = MagicMock()
    connection_pool_mock.connection_kwargs = {"host": "127.0.0.1", "port": 6379}

    # Mock the Redis client with the connection pool attached
    redis_client_mock = MagicMock(spec=Redis)
    redis_client_mock.connection_pool = connection_pool_mock

    # Create the RedisBroker mock with the Redis client mock
    broker = MagicMock(spec=RedisBroker)
    broker.client = redis_client_mock

    return broker


@pytest.fixture
def dramatiq_sentinel(mocker, mock_broker):

    # Mock the methods that start threads to prevent them from running
    mocker.patch("dramatiq_sentinel.middleware.sentinel.Sentinel._start_sentinel_listener_threads")
    mocker.patch("dramatiq_sentinel.middleware.sentinel.Sentinel._start_periodic_master_check_thread")

    middleware = DramatiqSentinel(
        sentinels=[("localhost", 26379)],
        sentinel_password="password",
        service_name="redis-service",
    )

    # Mock logger to prevent actual logging during tests
    middleware.logger = MagicMock()

    middleware.broker = mock_broker
    middleware.broker_update_lock = MagicMock()  # Mock threading Lock to avoid blocking

    # Mock worker instance
    middleware.worker = MagicMock()

    return middleware


def test_get_redis_master_success(dramatiq_sentinel):
    with patch.object(RedisSentinel, "discover_master", return_value=("localhost", 6379)), patch.object(Redis, "ping", return_value=True) as mock_ping:
        redis_client = dramatiq_sentinel.get_redis_master()
        assert redis_client is not None
        mock_ping.assert_called_once()


def test_get_redis_master_failure(dramatiq_sentinel, mocker):
    # Mock the internal method to always return None (simulating no master found)
    dramatiq_sentinel.discover_redis_master = MagicMock(return_value=None)

    # Mock time.sleep to skip the actual sleep
    mocker.patch("time.sleep", return_value=None)

    # Call the discover_redis_master method with max_iterations=1 to limit the retry
    result = dramatiq_sentinel.get_redis_master(max_iterations=1)

    # Assert that the result is None, indicating no master was found
    assert result is None

    # Assert that the logger warned about no Redis master being found
    dramatiq_sentinel.logger.warning.assert_any_call(
        "No Sentinels were successfully connected or no Redis master found. Retrying in %.2f seconds...", dramatiq_sentinel.min_backoff
    )

    # Assert that time.sleep was called (simulating the retry backoff)
    time.sleep.assert_called_once_with(dramatiq_sentinel.min_backoff)


def test_listen_to_sentinel_success(dramatiq_sentinel, mocker):
    """Test _listen_to_sentinel with a mocked iterator for pubsub.listen()."""

    # Create a mock Redis instance
    mock_redis_instance = MagicMock()

    # Mock the pubsub() method to return a mock pubsub object
    mock_pubsub = MagicMock()
    mock_redis_instance.pubsub.return_value = mock_pubsub

    # Mock the listen() method to return an iterator
    mock_listen = iter(
        [
            {"type": "subscribe", "channel": b"+switch-master"},
            {"type": "message", "channel": b"+switch-master", "data": b"master mymaster 127.0.0.1 6379 127.0.0.2 6380"},
        ]
    )
    mock_pubsub.listen.return_value = mock_listen

    dramatiq_sentinel.sentinels[("localhost", 26379)] = mock_redis_instance

    # Mock _handle_sentinel_master_switch_message to monitor its calls
    handle_message_mock = mocker.patch.object(dramatiq_sentinel, "_handle_sentinel_master_switch_message", return_value=True)

    # Run the method in a thread since it blocks on listen()
    thread = Thread(target=dramatiq_sentinel._listen_to_sentinel, args=("localhost", 26379), kwargs={"max_iterations": 1}, daemon=True)
    thread.start()
    thread.join(timeout=1)

    # Ensure that the message handling method was called once
    handle_message_mock.assert_called_once()

    # Verify that the pubsub.listen() was called on our mock
    mock_pubsub.listen.assert_called_once()


def test_listen_to_sentinel_connection_error(dramatiq_sentinel, mocker):
    """Test connection error and retry logic in _listen_to_sentinel."""

    # Simulate a connection error on Redis connection
    mock_redis_instance = MagicMock()
    mock_redis_instance.pubsub.side_effect = RedisConnectionError
    dramatiq_sentinel.sentinels[("localhost", 26379)] = mock_redis_instance

    # Mock time.sleep to avoid waiting during the test
    mocker.patch("time.sleep", return_value=None)

    # Run the method in a thread since it's a blocking method
    thread = Thread(target=dramatiq_sentinel._listen_to_sentinel, args=("localhost", 26379, 1.0, 5.0, 2.0, 1), daemon=True)
    thread.start()
    thread.join(timeout=1)

    dramatiq_sentinel.logger.warning.assert_called_with("Error in Redis Sentinel event listener localhost:26379 (quorum: 0/1): ")


def test_listen_to_sentinel_unexpected_error(dramatiq_sentinel):
    """Test unexpected error in _listen_to_sentinel."""

    # Simulate an unexpected error
    mock_redis_instance = MagicMock()
    mock_redis_instance.pubsub.side_effect = Exception("Unexpected error")
    dramatiq_sentinel.sentinels[("localhost", 26379)] = mock_redis_instance

    # Run the method in a thread since it's a blocking method
    thread = Thread(target=dramatiq_sentinel._listen_to_sentinel, args=("localhost", 26379, 1.0, 5.0, 2.0, 1), daemon=True)
    thread.start()
    thread.join(timeout=1)

    dramatiq_sentinel.logger.critical.assert_called_once_with(
        "Unexpected error in Redis Sentinel event listener localhost:26379: Unexpected error", exc_info=True
    )


def test_check_and_update_sentinel_redis_master_no_broker(dramatiq_sentinel):
    """Test when no broker is set or broker is not an instance of RedisBroker."""
    dramatiq_sentinel.broker = None  # No broker is set
    result = dramatiq_sentinel._check_and_update_sentinel_redis_master()
    assert not result
    dramatiq_sentinel.logger.warning.assert_called_with("Broker not yet set or not a RedisBroker")


@patch.object(DramatiqSentinel, "discover_redis_master", return_value=None)
def test_check_and_update_sentinel_redis_master_no_master_discovered(mock_discover, dramatiq_sentinel):
    """Test when no master is discovered."""
    dramatiq_sentinel.broker = MagicMock(spec=RedisBroker)  # Mock broker
    result = dramatiq_sentinel._check_and_update_sentinel_redis_master()
    assert not result
    mock_discover.assert_called_once()
    dramatiq_sentinel.logger.warning.assert_not_called()  # No warning since a broker is set


@patch.object(DramatiqSentinel, "discover_redis_master", return_value=("127.0.0.1", 6379))
def test_check_and_update_sentinel_redis_master_master_same(mock_discover, dramatiq_sentinel):
    """Test when the master is already the same."""
    result = dramatiq_sentinel._check_and_update_sentinel_redis_master()
    assert result
    mock_discover.assert_called_once()
    dramatiq_sentinel.logger.debug.assert_called_with("Current Redis host already set to 127.0.0.1:6379, switching Redis master not needed.")


@patch.object(DramatiqSentinel, "discover_redis_master", return_value=("127.0.0.2", 6380))
@patch.object(DramatiqSentinel, "pause_worker", return_value=True)
@patch.object(DramatiqSentinel, "resume_worker", return_value=None)
@patch.object(DramatiqSentinel, "reset_borker_connection", return_value=None)
@patch.object(DramatiqSentinel, "redis_ping", return_value=True)
def test_check_and_update_sentinel_redis_master_master_changed(
    mock_redis_ping, mock_reset_connection, mock_resume, mock_pause, mock_discover, dramatiq_sentinel
):
    """Test when the master has changed."""
    result = dramatiq_sentinel._check_and_update_sentinel_redis_master()
    assert result
    mock_discover.assert_called_once()
    mock_pause.assert_called_once()
    mock_reset_connection.assert_called_once_with(dramatiq_sentinel.broker, ("127.0.0.2", 6380))
    mock_redis_ping.assert_called_once()
    mock_resume.assert_called_once()
    dramatiq_sentinel.logger.warning.assert_any_call("Switching Redis from 127.0.0.1:6379 to 127.0.0.2:6380...")
    dramatiq_sentinel.logger.warning.assert_any_call("Dramatiq broker updated to new Redis master 127.0.0.2:6380.")


@patch.object(DramatiqSentinel, "discover_redis_master", return_value=("127.0.0.2", 6380))
@patch.object(DramatiqSentinel, "pause_worker", return_value=False)
def test_check_and_update_sentinel_redis_master_pause_failed(mock_pause, mock_discover, dramatiq_sentinel):
    """Test when pausing consumers fails."""
    result = dramatiq_sentinel._check_and_update_sentinel_redis_master()
    assert not result
    mock_discover.assert_called_once()
    mock_pause.assert_called_once()


def test_redis_ping_success(dramatiq_sentinel):
    """Test when Redis ping is successful."""
    mock_ping = MagicMock()
    mock_ping.return_value = True  # Simulate successful ping
    dramatiq_sentinel.broker.client.ping = mock_ping

    result = dramatiq_sentinel.redis_ping()
    assert result is True
    mock_ping.assert_called_once()
    dramatiq_sentinel.logger.warning.assert_not_called()


def test_redis_ping_connection_error(dramatiq_sentinel):
    """Test when Redis connection error occurs."""
    mock_ping = MagicMock()
    mock_ping.side_effect = RedisConnectionError("Connection failed")  # Simulate ConnectionError
    dramatiq_sentinel.broker.client.ping = mock_ping

    result = dramatiq_sentinel.redis_ping()
    assert result is False
    mock_ping.assert_called_once()
    dramatiq_sentinel.logger.error.assert_called_with("Redis host 127.0.0.1:6379 PING unsuccessful: Connection failed")

    mock_ping = MagicMock()
    mock_ping.return_value = False  # Simulate unsuccessful ping
    dramatiq_sentinel.broker.client.ping = mock_ping

    result = dramatiq_sentinel.redis_ping()
    assert result is False
    mock_ping.assert_called_once()
    dramatiq_sentinel.logger.warning.assert_called_with("Redis host 127.0.0.1:6379 PING unsuccessful")


def test_check_sentinel_quorum_success(dramatiq_sentinel):
    # Create a mock for the sentinel object
    mock_sentinel_1 = MagicMock()
    mock_sentinel_2 = MagicMock()

    # Mock the responses to simulate a successful quorum check
    mock_sentinel_1.execute_command.return_value = b"OK"
    mock_sentinel_2.execute_command.return_value = b"OK"

    # Assign the mock sentinels to the instance's sentinels attribute
    dramatiq_sentinel.sentinels = {"sentinel_1": mock_sentinel_1, "sentinel_2": mock_sentinel_2}

    # Call the method and assert the expected result
    result = dramatiq_sentinel._check_sentinel_quorum()
    assert result == {"quorums": 2, "available_sentinels": 2}


def test_check_sentinel_quorum_failure(dramatiq_sentinel):
    # Create a mock for the sentinel object
    mock_sentinel_1 = MagicMock()
    mock_sentinel_2 = MagicMock()

    # Mock the responses to simulate a failure in quorum check
    mock_sentinel_1.execute_command.side_effect = Exception("Connection error")
    mock_sentinel_2.execute_command.return_value = b"NOQUORUM"

    # Assign the mock sentinels to the instance's sentinels attribute
    dramatiq_sentinel.sentinels = {"sentinel_1": mock_sentinel_1, "sentinel_2": mock_sentinel_2}

    # Call the method and assert the expected result
    result = dramatiq_sentinel._check_sentinel_quorum()
    assert result == {"quorums": 0, "available_sentinels": 1}


def test_reset_borker_connection(dramatiq_sentinel, mock_broker):
    dramatiq_sentinel.reset_borker_connection(mock_broker, ("localhost", 6381))
    assert mock_broker.client.connection_pool.connection_kwargs["host"] == "localhost"
    assert mock_broker.client.connection_pool.connection_kwargs["port"] == 6381
    mock_broker.client.connection_pool.reset.assert_called_once()


def test_pause_worker_success(dramatiq_sentinel):
    """Test pause_worker successfully pauses worker threads."""
    # Mock worker.pause to complete without exceptions
    with patch("dramatiq_sentinel.middleware.sentinel.ThreadPoolExecutor") as mock_executor:
        future = MagicMock()
        future.result.return_value = True  # Simulate successful result within timeout
        mock_executor.return_value.__enter__.return_value.submit.return_value = future

        result = dramatiq_sentinel.pause_worker(timeout=5)
        assert result is True
        dramatiq_sentinel.logger.info.assert_called_with("Waiting (5 seconds) for all worker threads to be paused...")
        dramatiq_sentinel.logger.debug.assert_called_with("All worker threads have been paused.")


def test_pause_worker_timeout(dramatiq_sentinel):
    """Test pause_worker fails due to timeout."""
    # Mock worker.pause to raise a TimeoutError
    with patch("dramatiq_sentinel.middleware.sentinel.ThreadPoolExecutor") as mock_executor:
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        future = MagicMock()
        future.result.side_effect = TimeoutError("Timeout occured")  # Simulate TimeoutError exception
        mock_executor_instance.submit.return_value = future

        result = dramatiq_sentinel.pause_worker(timeout=5, exit_on_timeout=False)
        assert result is False
        dramatiq_sentinel.logger.warning.assert_called_with("Timeout occured while pausing worker threads (after 5 seconds)! Timeout occured")


def test_pause_worker_exception(dramatiq_sentinel):
    """Test pause_worker handles an unexpected exception."""
    # Mock worker.pause to raise an unexpected exception
    with patch("dramatiq_sentinel.middleware.sentinel.ThreadPoolExecutor") as mock_executor:
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        future = MagicMock()
        future.result.side_effect = Exception("Unexpected error")  # Simulate a generic exception
        mock_executor_instance.submit.return_value = future

        result = dramatiq_sentinel.pause_worker(timeout=5)
        assert result is False
        dramatiq_sentinel.logger.exception.assert_called()
        dramatiq_sentinel.logger.exception.assert_called_with("Unexpected error", exc_info=True)


def test_pause_worker_timeout_with_exit(dramatiq_sentinel):
    """Test pause_worker fails due to timeout and exits."""
    # Mock worker.pause to raise a TimeoutError and mock os._exit
    with patch("dramatiq_sentinel.middleware.sentinel.ThreadPoolExecutor") as mock_executor, patch("os._exit") as mock_exit:
        mock_executor_instance = MagicMock()
        mock_executor.return_value.__enter__.return_value = mock_executor_instance

        future = MagicMock()
        future.result.side_effect = TimeoutError  # Simulate TimeoutError exception
        mock_executor_instance.submit.return_value = future

        dramatiq_sentinel.pause_worker(timeout=5, exit_on_timeout=True)
        mock_exit.assert_called_once_with(69)
        dramatiq_sentinel.logger.critical.assert_called_with("Unable to pause worker threads after 600.0 seconds, shutting down!")


def test_resume_worker_success(dramatiq_sentinel):
    """Test resume_worker successfully resumes worker threads."""
    dramatiq_sentinel.resume_worker()
    dramatiq_sentinel.logger.info.assert_called_with("Resuming worker threads...")
    dramatiq_sentinel.worker.resume.assert_called_once()
    dramatiq_sentinel.logger.debug.assert_called_with("All worker threads have been resumed.")


def test_resume_worker_no_worker(dramatiq_sentinel):
    """Test resume_worker when no worker is set."""
    dramatiq_sentinel.worker = None  # No worker present
    dramatiq_sentinel.resume_worker()

    dramatiq_sentinel.logger.info.assert_not_called()
    dramatiq_sentinel.logger.debug.assert_not_called()


def test_get_redis_nat():
    dramatiq_sentinel = DramatiqSentinel(nat={("example.com", 8000): ("target.com", 9000)})

    assert dramatiq_sentinel.get_nat(("example.com", 8000)) == ("target.com", 9000)
    assert dramatiq_sentinel.get_nat(("other.com", 7000)) == ("other.com", 7000)
