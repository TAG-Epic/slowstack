import asyncio
from types import TracebackType
import typing
import pytest
from slowstack.asynchronous.base import BaseRateLimiter
from slowstack.asynchronous.times_per import TimesPerRateLimiter
from slowstack.asynchronous.any import AnyRateLimiter
from slowstack.common.errors import RateLimitCancelError, RateLimitedError
from slowstack.common.status import RateLimitStatus
from tests.utils import match_time


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_empty():
    any_ratelimiter = AnyRateLimiter([])

    async with any_ratelimiter.acquire():
        ...


@pytest.mark.asyncio
async def test_propagates_error():
    any_ratelimiter = AnyRateLimiter([])

    with pytest.raises(RateLimitCancelError):
        async with any_ratelimiter.acquire():
            raise RateLimitCancelError()


@pytest.mark.asyncio
@match_time(2, 0.1)
async def test_two_rate_limiters():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 1)

    any_ratelimiter = AnyRateLimiter([times_per_1, times_per_2])

    for _ in range(6):
        async with any_ratelimiter.acquire():
            ...


@pytest.mark.asyncio
@match_time(0.5, 0.1)
async def test_use_concurrent():
    times_per_1 = TimesPerRateLimiter(1, 0.5)
    times_per_2 = TimesPerRateLimiter(1, 0.5)
    any_ratelimiter = AnyRateLimiter([times_per_1, times_per_2])

    async def use_rate_limiter():
        print("acquiring")
        async with any_ratelimiter.acquire():
            pass
        print("acquired")

    await asyncio.gather(*[asyncio.create_task(use_rate_limiter()) for _ in range(4)])

@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_use_concurrent_cancel():
    times_per_1 = TimesPerRateLimiter(1, 0.5)
    times_per_2 = TimesPerRateLimiter(1, 0.5)
    any_ratelimiter = AnyRateLimiter([times_per_1, times_per_2])

    async def use_rate_limiter():
        print("acquiring")
        try:
            async with any_ratelimiter.acquire():
                raise RateLimitCancelError()
        except RateLimitCancelError:
            pass
        print("acquired")

    await asyncio.gather(*[asyncio.create_task(use_rate_limiter()) for _ in range(4)])


class TrickRateLimiter(BaseRateLimiter):
    def __init__(self) -> None:
        self._acquired: asyncio.Event = asyncio.Event()

    async def status(self) -> RateLimitStatus:
        if self._acquired.is_set():
            return RateLimitStatus.LOCKED
        return RateLimitStatus.UNLOCKED

    async def status_feed(self) -> typing.AsyncGenerator[RateLimitStatus, None]:
        yield await self.status()

        await self._acquired.wait()
        yield await self.status()

    def acquire(self, *, wait: bool = True) -> typing.AsyncContextManager[None]:
        return _TrickContextManager(self, wait)


class _TrickContextManager:
    def __init__(self, instance: TrickRateLimiter, wait: bool) -> None:
        self._instance: TrickRateLimiter = instance
        self._wait: bool = wait

    async def __aenter__(self) -> None:
        if not self._wait:
            raise RateLimitedError()
        # wait forever
        await asyncio.Future()

    async def __aexit__(
        self, exc_type: typing.Type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        del exc_type, exc, traceback
        raise NotImplementedError()


@pytest.mark.asyncio
@match_time(1, 10)
async def test_rate_limiter_stolen():
    times_per = TimesPerRateLimiter(1, 1)
    trick_rate_limiter = TrickRateLimiter()

    any_ratelimiter = AnyRateLimiter([times_per, trick_rate_limiter])

    for _ in range(3):
        async with any_ratelimiter.acquire():
            ...

@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_uses_available_rate_limiter():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 1)
    
    async with times_per_1.acquire():
        ...
    
    any_rate_limiter = AnyRateLimiter([times_per_1, times_per_2])
    async with any_rate_limiter.acquire():
        ...


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_uses_available_rate_limiter_reverse():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 1)
    
    async with times_per_2.acquire():
        ...
    
    any_rate_limiter = AnyRateLimiter([times_per_1, times_per_2])
    async with any_rate_limiter.acquire():
        ...


