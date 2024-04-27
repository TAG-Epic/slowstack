from types import TracebackType
from typing import AsyncContextManager, Final, AsyncGenerator, Type
import asyncio
from logging import getLogger

from ..common.status import RateLimitStatus
from ..common.errors import RateLimitedError, RateLimitCancelError
from .base import BaseRateLimiter
from .utils.priority_queue_container import PriorityQueueContainer

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

        from slowstack.asynchronous.times_per import TimesPerRateLimiter

        times_per = TimesPerRateLimiter(1, 1)

        async with times_per.acquire():
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

    __slots__ = ("limit", "per", "_remaining", "reset_offset_seconds", "_pending", "_in_progress", "_pending_reset")

    def __init__(self, limit: int, per: float) -> None:
        self.limit: Final[int] = limit
        self.per: Final[float] = per
        self._remaining: int = limit
        self.reset_offset_seconds: float = 0
        self._pending: asyncio.PriorityQueue[PriorityQueueContainer] = asyncio.PriorityQueue()
        self._status_update_queues: list[asyncio.Queue[RateLimitStatus]] = []
        self._in_progress: int = 0
        self._pending_reset: bool = False

    async def status(self) -> RateLimitStatus:
        """
        Get if there is a spot ready in the rate limit.

        If the return value is :attr:`RateLimitStatus.UNLOCKED` you should be able to immediately pass.
        """
        calculated_remaining = self._remaining - self._in_progress
        if calculated_remaining == 0:
            return RateLimitStatus.LOCKED
        return RateLimitStatus.UNLOCKED

    async def status_feed(self) -> AsyncGenerator[RateLimitStatus, None]:
        """
        A feed of the :meth:`status` changing.

        **Example usage**

        .. code:: python3

            from slowstack.asynchronous.times_per import TimesPerRateLimiter

            times_per = TimesPerRateLimiter(1, 1)

            async for status in times_per.status_feed():
                print(f"rate limiter is now: {status}")
        """
        yield await self.status()

        queue: asyncio.Queue[RateLimitStatus] = asyncio.Queue()
        self._status_update_queues.append(queue)

        try:
            while True:
                status = await queue.get()
                yield status
                queue.task_done()
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

    def acquire(self, *, priority: int = 0, wait: bool = True) -> AsyncContextManager[None]:
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

                from slowstack.asynchronous.times_per import TimesPerRateLimiter

                times_per = TimesPerRateLimiter(1, 1)

                async with times_per.acquire():
                    ... # Do things locked by the rate limiter here
        """
        return _TimesPerContext(self, priority=priority, wait=wait)

    def _reset(self) -> None:
        self._pending_reset = False

        # Status calc
        old_calculated_remaining = self.remaining

        self._remaining = self.limit

        to_release = min(self._pending.qsize(), self.remaining)
        _logger.debug("Releasing %s requests", to_release)
        for _ in range(to_release):
            container = self._pending.get_nowait()
            future = container.future

            # Mark it as completed, good practice (and saves a bit of memory due to a infinitly expanding int)
            self._pending.task_done()

            # Release it and allow further requests
            future.set_result(None)
        if self._pending.qsize() > 0:
            self._pending_reset = True

            loop = asyncio.get_running_loop()
            loop.call_later(self.per + self.reset_offset_seconds, self._reset)

        # Status calc
        new_calculated_remaining = self.remaining
        if old_calculated_remaining == 0 and new_calculated_remaining > 0:
            self._switch_status(RateLimitStatus.UNLOCKED)

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

    async def close(self) -> None:
        """Cleanup this instance.

        This should be done when this instance is never going to be used anymore

        .. warning::
            Continued use of this instance will result in instability
        """
        while not self._pending.empty():
            request = self._pending.get_nowait()
            request.future.set_exception(asyncio.CancelledError)
            self._pending.task_done()


class _TimesPerContext:
    def __init__(self, instance: TimesPerRateLimiter, *, priority: int = 0, wait: bool = True) -> None:
        self._instance: TimesPerRateLimiter = instance
        self._priority: int = priority
        self._wait: bool = wait

    async def __aenter__(self) -> None:
        _logger.debug("something is trying to acquire")
        calculated_remaining = self._instance.remaining

        if calculated_remaining == 0:
            if not self._wait:
                raise RateLimitedError()

            # Wait for a spot
            future: asyncio.Future[None] = asyncio.Future()
            item = PriorityQueueContainer(self._priority, future)

            self._instance._pending.put_nowait(item)

            _logger.debug("added request to queue with priority %s", self._priority)
            try:
                await future
            except asyncio.CancelledError:
                _logger.debug("cancelled .acquire, removing from queue.")
                # Theres sadly no remove method so we use this to remove instead
                self._instance._pending._queue.remove(item)
                self._instance._pending.task_done()
                return
            _logger.debug("out of queue, doing request")

        self._instance._in_progress += 1

        # Status calc
        new_calculated_remaining = self._instance.remaining
        if new_calculated_remaining == 0:
            self._instance._switch_status(RateLimitStatus.LOCKED)

    async def __aexit__(
        self,
        exception_type: Type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exception_type, traceback

        self._instance._in_progress -= 1

        if isinstance(exception, RateLimitCancelError):
            _logger.debug("rate limit cancel!")
            # A cancel request occured. This will not take from the rate-limit, and as so we have to re-allow a request to run
            if self._instance._pending.qsize() != 0:
                container = self._instance._pending.get_nowait()
                future = container.future

                # Mark it as completed, good practice (and saves a bit of memory due to a infinitly expanding int)
                self._instance._pending.task_done()

                # Release it and allow further requests
                future.set_result(None)

            # Status calc
            new_calculated_remaining = self._instance.remaining
            if new_calculated_remaining == 0:
                self._instance._switch_status(RateLimitStatus.LOCKED)
            elif new_calculated_remaining > 0:
                self._instance._switch_status(RateLimitStatus.UNLOCKED)

            raise exception  # Re-raise exception

        # Start a reset task
        if not self._instance._pending_reset:
            self._instance._pending_reset = True
            loop = asyncio.get_running_loop()
            loop.call_later(self._instance.per + self._instance.reset_offset_seconds, self._instance._reset)

        # This only gets called if no exception got raised.
        self._instance._remaining -= 1
