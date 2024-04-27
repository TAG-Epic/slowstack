import asyncio
import pytest
from slowstack.asynchronous.times_per import TimesPerRateLimiter
from slowstack.asynchronous.all import AllRateLimiter
from slowstack.common.errors import RateLimitCancelError
from tests.utils import match_time


@pytest.mark.asyncio
async def test_use_both():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 1)

    all_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

    async with all_limiter.acquire():
        pass

    assert times_per_1.remaining == 0
    assert times_per_2.remaining == 0


@pytest.mark.asyncio
async def test_use_both_no_wait():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 1)

    all_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

    async with all_limiter.acquire(wait=False):
        pass

    assert times_per_1.remaining == 0
    assert times_per_2.remaining == 0


@pytest.mark.asyncio
async def test_errors_propagate():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 1)

    all_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

    with pytest.raises(RateLimitCancelError):
        async with all_limiter.acquire():
            raise RateLimitCancelError()
    print(times_per_1.remaining, times_per_1._in_progress, times_per_1._remaining)

    assert times_per_1.remaining == 1
    assert times_per_2.remaining == 1


@pytest.mark.asyncio
async def test_no_rate_limiters():
    all_limiter = AllRateLimiter(set([]))

    async with all_limiter.acquire():
        ...


@pytest.mark.asyncio
@match_time(2, 0.1)
async def test_different_per():
    times_per_1 = TimesPerRateLimiter(1, 1)
    times_per_2 = TimesPerRateLimiter(1, 2)

    all_limiter = AllRateLimiter(set([times_per_1, times_per_2]))

    async with all_limiter.acquire():
        pass

    assert times_per_1.remaining == 0
    assert times_per_2.remaining == 0

    async with all_limiter.acquire():
        pass


@pytest.mark.asyncio
@match_time(1, 0.1)
async def test_use_concurrent():
    times_per_1 = TimesPerRateLimiter(1, 0.5)
    times_per_2 = TimesPerRateLimiter(1, 0.5)
    all_ratelimiter = AllRateLimiter(set([times_per_1, times_per_2]))

    async def use_rate_limiter():
        print("acquiring")
        async with all_ratelimiter.acquire():
            pass
        print("acquired")

    await asyncio.gather(*[asyncio.create_task(use_rate_limiter()) for _ in range(3)])

@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_use_concurrent_cancel():
    times_per_1 = TimesPerRateLimiter(1, 0.5)
    times_per_2 = TimesPerRateLimiter(1, 0.5)
    all_ratelimiter = AllRateLimiter(set([times_per_1, times_per_2]))

    async def use_rate_limiter():
        print("acquiring")
        try:
            async with all_ratelimiter.acquire():
                raise RateLimitCancelError()
        except RateLimitCancelError:
            pass
        print("acquired")

    await asyncio.gather(*[asyncio.create_task(use_rate_limiter()) for _ in range(3)])
