from logging import getLogger
from types import TracebackType
from typing import Final, Generator, ContextManager, Type
from queue import Queue, PriorityQueue
import time
import threading

from .base import BaseRateLimiter
from .utils.priority_queue_container import PriorityQueueContainer
from ..common.errors import RateLimitCancelError, RateLimitedError
from ..common.status import RateLimitStatus

_logger = getLogger(__name__)


class TimesPerRateLimiter(BaseRateLimiter):
    """
    A rate limiter which allows in ``times`` things per ``per`` seconds.

    Parameters:
        limit:
            The amount of times the rate limiter can be used
        per:
            How often this resets in seconds

    **Example usage**

    .. code:: python3

        from slowstack.synchronous.times_per import TimesPerRateLimiter

        times_per = TimesPerRateLimiter(1, 1)

        with times_per.acquire():
            ... # Do things locked by the rate limiter here

    Attributes:
        limit:
            The amount of times the rate limiter can be used
        per:
            How often this resets in seconds
        reset_offset_seconds:
            How much the resetting should be offset to account for processing/networking delays.

            This will be added to the reset time, so for example a offset of ``1`` will make resetting 1 second slower.
    """

    def __init__(self, limit: int, per: float) -> None:
        self.limit: Final[int] = limit
        self.per: Final[float] = per
        self._remaining: int = limit
        self.reset_offset_seconds: float = 0
        self._pending: PriorityQueue[PriorityQueueContainer] = PriorityQueue()
        self._status_update_queues: list[Queue[RateLimitStatus]] = []
        self._in_progress: int = 0
        self._pending_reset: bool = False
        self._acquire_lock: threading.Lock = threading.Lock()

    def status(self) -> RateLimitStatus:
        """
        Get if there is a spot ready in the rate limit.

        If the return value is :attr:`RateLimitStatus.UNLOCKED` you should be able to immediately pass.
        """
        calculated_remaining = self._remaining - self._in_progress
        if calculated_remaining == 0:
            return RateLimitStatus.LOCKED
        return RateLimitStatus.UNLOCKED

    def status_feed(self) -> Generator[RateLimitStatus, None, None]:
        """
        A feed of the :meth:`status` changing.

        **Example usage**

        .. code:: python3

            from slowstack.synchronous.times_per import TimesPerRateLimiter

            times_per = TimesPerRateLimiter(1, 1)

            for status in times_per.status_feed():
                print(f"rate limiter is now: {status}")
        """

        queue: Queue[RateLimitStatus] = Queue()
        self._status_update_queues.append(queue)

        yield self.status()

        try:
            while True:
                yield queue.get()
        finally:
            self._status_update_queues.remove(queue)

    @property
    def remaining(self) -> int:
        """
        How many more times this rate limiter can be acquired without waiting.

        .. note::
            This may go up partially to account for cancelled uses
        """
        return self._remaining - self._in_progress

    def acquire(self, *, priority: int = 0, wait: bool = True) -> ContextManager[None]:
        """
        Use a spot in the rate-limit.

        Parameters:
            priority:
                The priority. **Lower** number means it will be requested earlier.
            wait:
                Wait for a spot in the rate limit.

                If this is :data:`False`, this will raise :exc:`RateLimitedError` instead.

        Raises:
            RateLimitedError
                ``wait`` was set to :data:`False` and there was no more spots in the rate limit.
        Returns:
            typing.AsyncContextManager[None]:
                A context manager that will wait in ``__aenter__`` until you have locked the rate limiter.

        **Example usage**
            .. code:: python3

                from slowstack.synchronous.times_per import TimesPerRateLimiter

                times_per = TimesPerRateLimiter(1, 1)

                with times_per.acquire():
                    ... # Do things locked by the rate limiter here
        """
        return _TimesPerContextManager(self, priority=priority, wait=wait)

    def _reset(self) -> None:
        self._pending_reset = False

        with self._acquire_lock:
            # Status calc
            old_calculated_remaining = self.remaining

            self._remaining = self.limit

            to_release = min(self._pending.qsize(), self.remaining)
            _logger.debug("Releasing %s requests", to_release)
            for _ in range(to_release):
                container = self._pending.get_nowait()
                future = container.event

                # Mark it as completed, good practice (and saves a bit of memory due to a infinitly expanding int)
                self._pending.task_done()

                # Release it and allow further requests
                future.set()
            if self._pending.qsize() > 0:
                self._pending_reset = True

                self._reset_after(self.per + self.reset_offset_seconds)

            # Status calc
            new_calculated_remaining = self.remaining
            if old_calculated_remaining == 0 and new_calculated_remaining > 0:
                self._switch_status(RateLimitStatus.UNLOCKED)

    def _reset_after_blocking(self, after_seconds: float) -> None:
        """
        Reset the rate limiter after x seconds

        .. note::
            This is the blocking version of :meth:`_reset_after`

        Parameters:
            after_seconds:
                How many seconds to reset it after
        """
        time.sleep(after_seconds)
        self._reset()

    def _reset_after(self, after_seconds: float) -> None:
        """
        Reset the rate limiter after x seconds

        .. note::
            This does not block.

        Parameters:
            after_seconds:
                How many seconds to reset it after
        """
        reset_thread = threading.Thread(target=self._reset_after_blocking, args=(after_seconds,))
        reset_thread.daemon = True
        reset_thread.start()

    # Utils
    def _switch_status(self, new_state: RateLimitStatus) -> None:
        """
        Switches the state machine to another state and dispatches it to all listeners

        .. note::
            Updating to the same state will not dispatch anything

        Parameters:
            new_state:
                The state to switch to
        """
        _logger.debug("switched to status %s", new_state)
        self._state = new_state
        for queue in self._status_update_queues:
            queue.put_nowait(new_state)


class _TimesPerContextManager:
    def __init__(self, instance: TimesPerRateLimiter, *, priority: int = 0, wait: bool = True) -> None:
        self._instance: Final[TimesPerRateLimiter] = instance
        self._priority: Final[int] = priority
        self._wait: Final[bool] = wait

    def __enter__(self) -> None:
        self._instance._acquire_lock.acquire()
        calculated_remaining = self._instance.remaining

        if calculated_remaining == 0:
            try:
                if not self._wait:
                    raise RateLimitedError()

                # Wait for a spot
                event: threading.Event = threading.Event()
                item = PriorityQueueContainer(self._priority, event)

                self._instance._pending.put_nowait(item)
            finally:
                self._instance._acquire_lock.release()

            _logger.debug("added request to queue with priority %s", self._priority)
            event.wait()
            _logger.debug("out of queue, doing request")
        else:
            self._instance._acquire_lock.release()

        self._instance._in_progress += 1

        # Status calc
        with self._instance._acquire_lock:
            new_calculated_remaining = self._instance.remaining
            if new_calculated_remaining == 0:
                self._instance._switch_status(RateLimitStatus.LOCKED)

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, traceback

        with self._instance._acquire_lock:
            self._instance._in_progress -= 1

            if isinstance(exception, RateLimitCancelError):
                _logger.debug("rate limit cancel!")
                # A cancel request occured. This will not take from the rate-limit, and as so we have to re-allow a request to run
                if self._instance._pending.qsize() != 0:
                    container = self._instance._pending.get_nowait()

                    # Release it and allow further requests
                    container.event.set()

                    # Mark it as completed, good practice (and saves a bit of memory due to a infinitly expanding int)
                    self._instance._pending.task_done()

                # Status calc
                _logger.debug("updating status due to rate limit cancel")
                new_calculated_remaining = self._instance.remaining
                if new_calculated_remaining == 0:
                    self._instance._switch_status(RateLimitStatus.LOCKED)
                elif new_calculated_remaining > 0:
                    self._instance._switch_status(RateLimitStatus.UNLOCKED)

                raise exception  # Re-raise exception

            # Start a reset task
            if not self._instance._pending_reset:
                self._instance._pending_reset = True
                self._instance._reset_after(self._instance.per + self._instance.reset_offset_seconds)

            # This only gets called if no exception got raised.
            self._instance._remaining -= 1
