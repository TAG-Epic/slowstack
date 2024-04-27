from enum import StrEnum
from types import TracebackType
from typing import Any, AsyncContextManager, AsyncGenerator, Final, Type
import asyncio
from logging import getLogger

from .utils.safe_cancel import cancel_task
from ..common.status import RateLimitStatus
from ..common.errors import RateLimitedError
from .base import BaseRateLimiter

_logger = getLogger(__name__)


class AllRateLimiter(BaseRateLimiter):
    """
    A rate limiter that requires all rate limiters to be acquired at the same timeto be acquired.

    Parameters:
        rate_limiters:
            The rate limiters that all need to be acquired

    **Example usage**

    .. code:: python3

        from slowstack.asynchronous.all import AllRateLimiter
        from slowstack.asynchronous.times_per import TimesPerRateLimiter

        times_per_1 = TimesPerRateLimiter(1, 1)
        times_per_2 = TimesPerRateLimiter(5, 5)
        all_rate_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

        async with all_rate_limiter.acquire():
            ... # Do things locked by the rate limiter
    """

    def __init__(self, rate_limiters: set[BaseRateLimiter]) -> None:
        self._rate_limiters: Final[set[BaseRateLimiter]] = rate_limiters

    async def status(self) -> RateLimitStatus:
        """
        Whether a call to :meth:`acquire` will instantly pass

        This checks if all rate limiters is ready.
        """
        get_status_tasks: list[asyncio.Task[RateLimitStatus]] = [
            asyncio.create_task(rate_limiter.status()) for rate_limiter in self._rate_limiters
        ]

        while len(get_status_tasks) != 0:
            done, pending = await asyncio.wait(get_status_tasks, return_when=asyncio.FIRST_COMPLETED)
            del pending

            assert len(done) == 1, "done wasnt 1 task"
            task = done.pop()
            get_status_tasks.remove(task)

            status = await task
            if status == RateLimitStatus.LOCKED:
                return RateLimitStatus.LOCKED

        return RateLimitStatus.UNLOCKED

    async def status_feed(self) -> AsyncGenerator[RateLimitStatus, None]:
        """
        A live feed of when all rate limiters is ready to be :meth:`BaseRateLimiter.acquire` d

        **Example usage**

        .. code:: python3

            from slowstack.asynchrounous.all import AllRateLimiter
            from slowstack.asynchronous.times_per import TimesPerRateLimiter

            times_per_1 = TimesPerRateLimiter(1, 1)
            times_per_2 = TimesPerRateLimiter(5, 5)
            all_rate_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

            async for status in all_rate_limiter.status_feed():
                print(f"all rate limiters is now: {status}")
        """
        rate_limiter_statuses: dict[BaseRateLimiter, RateLimitStatus] = {}
        current_status: RateLimitStatus | None = None

        while True:
            async for rate_limiter, status in self.global_status_feed():
                rate_limiter_statuses[rate_limiter] = status

                is_initialized = len(rate_limiter_statuses) == len(self._rate_limiters)

                if all(status == RateLimitStatus.UNLOCKED for status in rate_limiter_statuses.values()):
                    if is_initialized and (current_status is None or current_status == RateLimitStatus.LOCKED):
                        current_status = RateLimitStatus.UNLOCKED
                        yield current_status
                else:
                    if current_status is None or current_status == RateLimitStatus.UNLOCKED:
                        current_status = RateLimitStatus.LOCKED
                        yield current_status

    async def global_status_feed(
        self,
    ) -> AsyncGenerator[tuple[BaseRateLimiter, RateLimitStatus], None]:
        """
        A combination feed of all rate limiter's :meth:`BaseRateLimiter.status_feed`

        Returns:
            AsyncGenerator[tuple[BaseRateLimiter, RateLimitStatus], None]:
                Yields a tuple of RateLimiter and RateLimitStatus

        **Example usage**

        .. code:: python3

            from slowstack.asynchrounous.all import AllRateLimiter
            from slowstack.asynchronous.times_per import TimesPerRateLimiter

            times_per_1 = TimesPerRateLimiter(1, 1)
            times_per_2 = TimesPerRateLimiter(5, 5)
            all_rate_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

            async for rate_limiter, status in all_rate_limiter.global_status_feed():
                print(f"{rate_limiter} is now: {status}")
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

    def acquire(self, *, wait: bool = True, **kwargs: Any) -> AsyncContextManager[None]:
        """
        Acquire all the rate limiters

        Parameters:
            wait:
                Whether to wait for the rate limiters to be unlocked if locked.

                If this is set to :data:`False`, this will raise :exc:`RateLimitedError`
            kwargs:
                Kwargs to pass to all the rate limiters
        Returns:
            typing.AsyncContextManager[None]:
                A context manager that will wait in ``__aenter__`` until you have locked the rate limiter.

        **Example usage**

        .. code:: python3


            from slowstack.asynchrounous.all import AllRateLimiter
            from slowstack.asynchronous.times_per import TimesPerRateLimiter

            times_per_1 = TimesPerRateLimiter(1, 1)
            times_per_2 = TimesPerRateLimiter(5, 5)
            all_rate_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

            async with all_rate_limiter.acquire():
                ... # Do things locked by the rate limiter
        """
        return _AllContextManager(self, self._rate_limiters, wait=wait, **kwargs)


class _AllContextManagerState(StrEnum):
    WAITING = "waiting"
    ACQUIRING = "acquiring"
    ACQUIRED = "acquired"
    CLEAN_UP = "clean_up"
    CLEANED_UP = "cleaned_up"


class _AllContextManager:
    def __init__(
        self,
        all_instance: AllRateLimiter,
        rate_limiters: set[BaseRateLimiter],
        *,
        wait: bool = True,
        **rate_limiter_kwargs: Any,
    ):
        self._all_ratelimiter: AllRateLimiter = all_instance
        self._rate_limiters: set[BaseRateLimiter] = rate_limiters
        self._rate_limiters_acquired: dict[BaseRateLimiter, bool] = {
            rate_limiter: False for rate_limiter in rate_limiters
        }
        self._rate_limiter_to_context: dict[BaseRateLimiter, AsyncContextManager[None]] = {}
        self._tasks: list[asyncio.Task[None]] = []

        # State handling
        self._state: _AllContextManagerState = _AllContextManagerState.WAITING
        self._state_update_queues: list[asyncio.Queue[_AllContextManagerState]] = []

        # Options
        self._wait: bool = wait
        self._rate_limiter_kwargs: dict[str, Any] = rate_limiter_kwargs

    async def __aenter__(self) -> None:
        try:
            ready_state_watcher_task = asyncio.create_task(self._rate_limiters_ready_state_watcher())
            self._tasks.append(ready_state_watcher_task)
            self._tasks.append(asyncio.create_task(self._try_to_acquire_all_rate_limiters()))

            # Wait for first one to end
            wait_for_internal_exception_task = asyncio.create_task(
                asyncio.wait(self._tasks, return_when=asyncio.FIRST_EXCEPTION)
            )
            wait_for_state_change_task = asyncio.create_task(
                self._wait_for_state_change(_AllContextManagerState.ACQUIRED)
            )
            await asyncio.wait(
                [wait_for_state_change_task, wait_for_internal_exception_task], return_when=asyncio.FIRST_COMPLETED
            )
            await cancel_task(wait_for_state_change_task)
            await cancel_task(wait_for_internal_exception_task)

            if ready_state_watcher_task.done():
                if (exception := ready_state_watcher_task.exception()) is not None:
                    if isinstance(exception, RateLimitedError):
                        self._tasks.remove(ready_state_watcher_task) # prevent double processing
                        raise exception
        finally:
            await self._cleanup_tasks()

    async def __aexit__(
        self,
        exception_type: Type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        errors = []

        aexit_tasks = [
            asyncio.create_task(context.__aexit__(exception_type, exception, traceback))
            for context in self._rate_limiter_to_context.values()
        ]
        if len(aexit_tasks) > 0:
            await asyncio.wait(aexit_tasks, return_when=asyncio.ALL_COMPLETED)
            errors = []

            for task in aexit_tasks:
                if (task_exception := task.exception()) is not None:
                    if task_exception != exception:
                        errors.append(task_exception)

            if len(errors) > 0:
                errors.append(exception)
                raise ExceptionGroup("rate limiters failed to handle __aexit__", errors)

        if exception is not None:
            raise exception

    # State functions
    async def _rate_limiters_ready_state_watcher(self) -> None:
        """
        Task that watches all rate limiters

        This makes sure we are only trying to acquire when it *should* pass instantly
        """
        rate_limiters_unlocked: dict[BaseRateLimiter, bool] = {
            rate_limiter: False for rate_limiter in self._rate_limiters
        }
        if len(self._rate_limiters) == 0:
            # We would never receive a unlocked event so we are skipping the checks
            _logger.debug("bypassing due to 0 sub-ratelimiters")
            self._switch_state(_AllContextManagerState.ACQUIRING)
            return

        async for rate_limiter, state in self._all_ratelimiter.global_status_feed():
            rate_limiters_unlocked[rate_limiter] = state == RateLimitStatus.UNLOCKED

            if not self._wait:
                # TODO: This hack sucks!
                # This fixes a race condition by forcing asyncio to context switch so _rate_limiters_acquired gets updated.
                await asyncio.sleep(0) 

                if not self._rate_limiters_acquired[rate_limiter] and state == RateLimitStatus.LOCKED:
                    _logger.debug("blame rate limiter %s", rate_limiter)
                    raise RateLimitedError()

            rate_limiters_ok = {
                rate_limiter: rate_limiters_unlocked[rate_limiter] or self._rate_limiters_acquired[rate_limiter]
                for rate_limiter in self._rate_limiters
            }
            if all(rate_limiters_ok.values()):
                self._switch_state(_AllContextManagerState.ACQUIRING)
            else:
                self._switch_state(_AllContextManagerState.WAITING)

    async def _try_to_acquire_all_rate_limiters(self) -> None:
        """
        Try to acquire all the rate limiters
        """
        _logger.debug("trying to acquire rate limiters")
        while self._state != _AllContextManagerState.ACQUIRED:
            await self._wait_for_state_change(_AllContextManagerState.ACQUIRING)
            wait_for_state_change_back_task = asyncio.create_task(
                self._wait_for_state_change(_AllContextManagerState.WAITING)
            )

            try:
                self._rate_limiter_to_context = {
                    rate_limiter: rate_limiter.acquire() for rate_limiter in self._rate_limiters
                }
                rate_limiter_to_aenter_task = {
                    rate_limiter: asyncio.create_task(context.__aenter__())
                    for rate_limiter, context in self._rate_limiter_to_context.items()
                }
                context_to_aenter_task: dict[AsyncContextManager[None], asyncio.Task[None]] = {
                    self._rate_limiter_to_context[rate_limiter]: rate_limiter_to_aenter_task[rate_limiter]
                    for rate_limiter in self._rate_limiters
                }

                try:
                    while not (
                        all(task.done() for task in rate_limiter_to_aenter_task.values()) or len(self._rate_limiters) == 0
                    ):
                        aenter_tasks_left: list[asyncio.Task[Any]] = [task for task in rate_limiter_to_aenter_task.values() if not task.done()]
                        await asyncio.wait(
                            [*aenter_tasks_left, wait_for_state_change_back_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )

                        for rate_limiter, task in rate_limiter_to_aenter_task.items():
                            self._rate_limiters_acquired[rate_limiter] = task.done()

                        if wait_for_state_change_back_task.done():
                            await self._cleanup_aenter_tasks(context_to_aenter_task)
                            break
                    else:
                        self._switch_state(_AllContextManagerState.ACQUIRED)
                except BaseException:
                    await self._cleanup_aenter_tasks(context_to_aenter_task)
                    raise
            finally:
                await cancel_task(wait_for_state_change_back_task)

    # Utils
    async def _wait_for_state_change(self, desired_state: _AllContextManagerState | None = None) -> None:
        """
        Wait for the state to switch to the desired state

        Parameters:
            desired_state:
                The state to wait for. If this is :data:`None` any state change will trigger this
        """
        if self._state == desired_state:
            return
        queue: asyncio.Queue[_AllContextManagerState] = asyncio.Queue()
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

    def _switch_state(self, new_state: _AllContextManagerState) -> None:
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

    async def _cleanup_tasks(self) -> None:
        """
        Cleans up all internal tasks

        Raises:
            ExceptionGroup:
                A task had an error/failed to stop.
        """
        errors = []
        for task in self._tasks:
            if task.done():
                if (exception := task.exception()) is not None:
                    errors.append(exception)
            else:
                try:
                    await cancel_task(task)
                except Exception as error:
                    errors.append(error)

        if len(errors) > 0:
            raise ExceptionGroup("errors in internal tasks", errors)

    async def _cleanup_aenter_tasks(
        self, context_to_aenter_task: dict[AsyncContextManager[None], asyncio.Task[None]]
    ) -> None:
        """
        Cleans up aenter tasks

        This will cancel any non-finished aenters, and call aclose on any that finished aenter.

        Raises:
            ExceptionGroup:
                A task had an error/failed to stop.
        """
        errors = []
        aexit_tasks = []
        for context, aenter_task in context_to_aenter_task.items():
            if aenter_task.done():
                if (exception := aenter_task.exception()) is not None:
                    errors.append(exception)
                else:
                    aexit_tasks.append(asyncio.create_task(context.__aexit__(None, None, None)))
            else:
                aenter_task.cancel()

        await asyncio.gather(*aexit_tasks, return_exceptions=True)

        for task in aexit_tasks:
            if (exception := task.exception()) is not None:
                errors.append(exception)

        if len(errors) > 0:
            raise ExceptionGroup("errors in rate limiters __aenter__/__aexit__", errors)
