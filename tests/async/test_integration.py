import asyncio
import pytest
from slowstack.asynchronous.any import AnyRateLimiter
from slowstack.asynchronous.times_per import TimesPerRateLimiter
from slowstack.asynchronous.all import AllRateLimiter
from slowstack.common.errors import RateLimitCancelError
from tests.utils import debug_time, match_time


@pytest.mark.asyncio
async def test_collisions():
    times_per = TimesPerRateLimiter(1, 0.1)
    any_rate_limiter_1 = AnyRateLimiter([times_per])
    any_rate_limiter_2 = AnyRateLimiter([times_per])

    all_ratelimiter = AllRateLimiter(set([any_rate_limiter_1, any_rate_limiter_2]))

    with pytest.raises(asyncio.TimeoutError):
        async with asyncio.timeout(1):
            async with all_ratelimiter.acquire():
                pass

@pytest.mark.asyncio
@match_time(0.2, 0.05)
async def test_concurrent():
    times_per_1 = TimesPerRateLimiter(1, 0.1)
    times_per_2 = TimesPerRateLimiter(1, 0.1)
    any_1 = AnyRateLimiter([times_per_1, times_per_2])
    times_per_3 = TimesPerRateLimiter(2, 0.1)
    all_1 = AllRateLimiter({any_1, times_per_3})

    async def use_rate_limiter():
        async with all_1.acquire():
            ...
    await asyncio.gather(*[use_rate_limiter() for i in range(3)])

@pytest.mark.asyncio
@match_time(0, 0.05)
async def test_cancel():
    times_per_1 = TimesPerRateLimiter(1, 0.1)
    times_per_2 = TimesPerRateLimiter(1, 0.1)
    any_1 = AnyRateLimiter([times_per_1, times_per_2])
    times_per_3 = TimesPerRateLimiter(2, 0.1)
    all_1 = AllRateLimiter({any_1, times_per_3})

    for i in range(100):
        try:
            async with all_1.acquire():         
                raise RateLimitCancelError()
        except RateLimitCancelError:
            pass
    
@pytest.mark.asyncio
#@match_time(0, 0.05)
@debug_time()
async def test_concurrent_cancel():
    #times_per_1 = TimesPerRateLimiter(1, 1)
    #times_per_2 = TimesPerRateLimiter(1, 1)
    #any_1 = AnyRateLimiter([times_per_1, times_per_2])
    #times_per_3 = TimesPerRateLimiter(2, 10)
    #all_1 = AllRateLimiter({any_1, times_per_3})

    all_1 = AllRateLimiter({TimesPerRateLimiter(1, 10), AnyRateLimiter([TimesPerRateLimiter(1, 10)])})
    async def use_rate_limiter():
        try:
            async with all_1.acquire():         
                raise RateLimitCancelError()
        except RateLimitCancelError:
            pass
    await asyncio.gather(*[use_rate_limiter() for i in range(10)])
