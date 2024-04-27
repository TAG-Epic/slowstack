from enum import StrEnum
from types import TracebackType
from typing import Any, AsyncContextManager, AsyncGenerator, AsyncIterator, Final, Type, TypeVar
import asyncio
from logging import getLogger

from ..common.errors import RateLimitedError
from ..common.status import RateLimitStatus
from .utils.safe_cancel import cancel_task
from .base import BaseRateLimiter

_logger = getLogger(__name__)

T = TypeVar("T")


class AnyRateLimiter(BaseRateLimiter):
    def __init__(self, rate_limiters: list[BaseRateLimiter]) -> None:
        """
        A rate limiter that will use the first ready rate limiter

        Parameters:
            rate_limiters:
                The rate limiters to choose between

                .. note::
                    If no rate limiters are provided, this will instantly return.
        """
        self._rate_limiters: list[BaseRateLimiter] = rate_limiters

    def __repr__(self) -> str:
        return f"<AnyRateLimiter rate_limiters=[{', '.join([repr(rate_limiter) for rate_limiter in self._rate_limiters])}]>"

    async def status(self) -> RateLimitStatus:
        """
        Gets if the rate limiter can be used right now.
        """
        status_tasks = [asyncio.create_task(rate_limiter.status()) for rate_limiter in self._rate_limiters]

        for task in asyncio.as_completed(status_tasks):
            # Let error propagate to the user of this function
            status = await task

            if status == RateLimitStatus.UNLOCKED:
                return RateLimitStatus.UNLOCKED

        return RateLimitStatus.LOCKED

    async def global_status_feed(
        self,
    ) -> AsyncGenerator[tuple[BaseRateLimiter, RateLimitStatus], None]:
        """
        A combination feed of all rate limiter's :meth:`BaseRateLimiter.status_feed`

        Returns:
            AsyncGenerator[tuple[BaseRateLimiter, RateLimitStatus, None]:
                Yields a tuple of RateLimiter and RateLimitStatus
        """
        global_feed: asyncio.Queue[tuple[BaseRateLimiter, RateLimitStatus]] = asyncio.Queue()

        # Start up consumers
        consumer_tasks: list[asyncio.Task[None]] = []
        for rate_limiter in self._rate_limiters:
            consumer_tasks.append(asyncio.create_task(self._global_status_feed_consumer(rate_limiter, global_feed)))

        try:
            while True:
                # TODO: raise failures in comsumer tasks!
                yield await global_feed.get()
        finally:
            for consumer_task in consumer_tasks:
                consumer_task.cancel()

    async def _global_status_feed_consumer(
        self,
        rate_limiter: BaseRateLimiter,
        global_feed: asyncio.Queue[tuple[BaseRateLimiter, RateLimitStatus]],
    ) -> None:
        """
        A consumer for :meth:`_global_status_feed` for one spesific rate limiter

        Parameters:
            rate_limiter:
                The rate limiter to produce events from
            global_feed:
                The feed to insert into
        """
        async for status in rate_limiter.status_feed():
            global_feed.put_nowait((rate_limiter, status))

    async def status_feed(self) -> AsyncGenerator[RateLimitStatus, None]:
        """
        A feed of when this rate limiter is ready to be acquired
        """
        rate_limiter_to_status: dict[BaseRateLimiter, RateLimitStatus] = {}

        last_sent_status: RateLimitStatus | None = None

        async for rate_limiter, status in self.global_status_feed():
            rate_limiter_to_status[rate_limiter] = status

            if all([status == RateLimitStatus.UNLOCKED for status in rate_limiter_to_status.values()]):
                new_status = RateLimitStatus.UNLOCKED
            else:
                new_status = RateLimitStatus.LOCKED
            if last_sent_status is None or new_status == last_sent_status:
                yield new_status
                last_sent_status = new_status

    def acquire(self, *, wait: bool = True, **rate_limiter_kwargs) -> AsyncContextManager[None]:
        """
        Acquire the first sub-ratelimiter which is :attr:`RateLimitStatus.UNLOCKED`

        Parameters:
            wait:
                Wait for a rate limiter to be :attr:`RateLimitStatus.UNLOCKED`

                If this is set to :data:`False`, this will raise :exc:`RateLimitedError`
            kwargs:
                Kwargs to pass to the sub-ratelimiters.

        Returns:
            typing.AsyncContextManager[None]:
                A context manager that will wait in ``__aenter__`` until you have locked the rate limiter.

        **Example usage**

        .. code:: python3


            from slowstack.asynchrounous.any import AnyRateLimiter
            from slowstack.asynchronous.times_per import TimesPerRateLimiter

            times_per_1 = TimesPerRateLimiter(1, 1)
            times_per_2 = TimesPerRateLimiter(5, 5)
            any_rate_limiter = AnyRateLimiter([times_per_1, times_per_2])

            async with any_rate_limiter.acquire():
                ... # Do things locked by the rate limiter
        """
        return _AnyContextManager(self, wait=wait, **rate_limiter_kwargs)


class _AnyContextManagerState(StrEnum):
    WAITING = "waiting"
    ACQUIRING = "acquiring"
    ACQUIRED = "acquired"


class _AnyContextManager:
    def __init__(self, instance: AnyRateLimiter, *, wait: bool, **rate_limiter_kwargs: Any) -> None:
        self._instance: Final[AnyRateLimiter] = instance
        self._wait: Final[bool] = wait
        self._rate_limiter_kwargs: Final[dict[str, Any]] = rate_limiter_kwargs
        self._state: _AnyContextManagerState = _AnyContextManagerState.WAITING
        self._state_update_queues: list[asyncio.Queue[_AnyContextManagerState]] = []
        self._chosen_context: AsyncContextManager[None] | None = None

    async def __aenter__(self) -> None:
        await self._rate_limiters_status_watcher()

    async def __aexit__(
        self,
        exception_type: Type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._chosen_context is not None:
            await self._chosen_context.__aexit__(exception_type, exception, traceback)
        else:
            if exception is not None:
                raise exception

    # Tasks
    async def _rate_limiters_status_watcher(self) -> None:
        """
        Watches the rate limiters and keeps the state updated to reflect it.
        """
        tasks: list[asyncio.Task[Any]] = []

        if len(self._instance._rate_limiters) == 0:
            # Skip forward as the state machine below doesn't support returning immidiately.
            self._switch_state(_AnyContextManagerState.ACQUIRED)
            return

        try:
            rate_limiter_to_status: dict[BaseRateLimiter, RateLimitStatus] = {}

            global_status_feed_iterator = self._instance.global_status_feed()
            global_status_feed_task: asyncio.Task[tuple[BaseRateLimiter, RateLimitStatus]] = asyncio.create_task(
                self._anext_but_coro(global_status_feed_iterator)
            )
            tasks.append(global_status_feed_task)

            acquire_attempt_data: tuple[BaseRateLimiter, AsyncContextManager[None], asyncio.Task[None]] | None = None
            while True:
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

                if global_status_feed_task.done():
                    rate_limiter, status = await global_status_feed_task
                    rate_limiter_to_status[rate_limiter] = status

                    if status == RateLimitStatus.UNLOCKED and acquire_attempt_data is None:
                        try:
                            context = rate_limiter.acquire(wait=False)
                        except RateLimitedError:
                            continue
                        task = asyncio.create_task(context.__aenter__())
                        tasks.append(task)
                        acquire_attempt_data = (rate_limiter, context, task)
                        self._switch_state(_AnyContextManagerState.ACQUIRING)

                    # Add a new anext task
                    tasks.remove(global_status_feed_task)
                    global_status_feed_task = asyncio.create_task(self._anext_but_coro(global_status_feed_iterator))
                    tasks.append(global_status_feed_task)

                if acquire_attempt_data is not None:
                    rate_limiter, context, task = acquire_attempt_data

                    if task.done():
                        try:
                            await task
                        except RateLimitedError:
                            # Acquire another rate limiter
                            acquire_attempt_data = None
                            for rate_limiter, status in rate_limiter_to_status.items():
                                if status == RateLimitStatus.UNLOCKED:
                                    context = rate_limiter.acquire(wait=False)
                                    aenter_task = asyncio.create_task(context.__aenter__())
                                    tasks.append(aenter_task)
                                    acquire_attempt_data = (rate_limiter, context, aenter_task)
                                    break
                            else:
                                self._switch_state(_AnyContextManagerState.WAITING)

                        else:
                            self._chosen_context = context
                            self._switch_state(_AnyContextManagerState.ACQUIRED)
                            return
                        finally:
                            tasks.remove(task)
        finally:
            for task in tasks:
                try:
                    await cancel_task(task)
                except RateLimitedError:
                    pass # I don't understand why this gets here but it does.

    # Utils
    async def _anext_but_coro(self, iterator: AsyncIterator[T]) -> T:
        """
        Make :meth:`anext` a coroutine instead of a Awaitable.

        This is due to Awaitable not being supported by asyncio.create_task, so you can't run it in the background.
        """
        return await anext(iterator)

    async def _wait_for_state_change(self, desired_state: _AnyContextManagerState | None = None) -> None:
        """
        Wait for the state to switch to the desired state

        Parameters:
            desired_state:
                The state to wait for. If this is :data:`None` any state change will trigger this
        """
        if self._state == desired_state:
            return
        queue: asyncio.Queue[_AnyContextManagerState] = asyncio.Queue()
        self._state_update_queues.append(queue)
        try:
            while True:
                state = await queue.get()

                if desired_state is None:
                    break
                if state == desired_state:
                    break
                _logger.debug("waiting for state %s but got %s", desired_state, state)
        finally:
            self._state_update_queues.remove(queue)

    def _switch_state(self, new_state: _AnyContextManagerState) -> None:
        """
        Switches the state machine to another state and dispatches it to all listeners

        .. note::
            Updating to the same state will not dispatch anything

        Parameters:
            new_state:
                The state to switch to
        """
        if self._state == new_state:
            return
        _logger.debug("switched to state %s", new_state)
        self._state = new_state
        for queue in self._state_update_queues:
            queue.put_nowait(new_state)
