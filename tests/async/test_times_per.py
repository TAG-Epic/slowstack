import pytest
import asyncio
from tests.utils import match_time
from slowstack.asynchronous.times_per import TimesPerRateLimiter
from slowstack.common.errors import RateLimitCancelError, RateLimitedError


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_should_not_sleep():
    rate_limiter = TimesPerRateLimiter(5, 5)

    for _ in range(5):
        async with rate_limiter.acquire():
            ...

    await rate_limiter.close()


@pytest.mark.asyncio
@match_time(0.1, 0.01)
async def test_should_sleep():
    rate_limiter = TimesPerRateLimiter(5, 0.1)

    for _ in range(10):
        async with rate_limiter.acquire():
            ...

    await rate_limiter.close()


def test_repr():
    repr(TimesPerRateLimiter(1, 1))


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_exception_undos():
    rate_limiter = TimesPerRateLimiter(1, 1)

    for _ in range(100):
        try:
            async with rate_limiter.acquire():
                raise RateLimitCancelError()
        except:
            pass

    await rate_limiter.close()


@pytest.mark.asyncio
async def test_no_wait():
    rate_limiter = TimesPerRateLimiter(1, 1)

    async with rate_limiter.acquire(wait=False):
        ...  # Good!

    with pytest.raises(RateLimitedError):
        async with rate_limiter.acquire(wait=False):
            ...

    await rate_limiter.close()


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_reset_offset_negative():
    rate_limiter = TimesPerRateLimiter(1, 1)
    rate_limiter.reset_offset_seconds = -1

    for _ in range(100):
        async with rate_limiter.acquire():
            ...


@pytest.mark.asyncio
@match_time(2, 0.1)
async def test_reset_offset_positive():
    rate_limiter = TimesPerRateLimiter(1, 1)
    rate_limiter.reset_offset_seconds = 1

    for _ in range(2):
        async with rate_limiter.acquire():
            ...


@pytest.mark.asyncio
@match_time(1, 0.1)
async def test_use_concurrent():
    rate_limiter = TimesPerRateLimiter(1, 0.5)

    async def use_rate_limiter():
        async with rate_limiter.acquire():
            pass

    await asyncio.gather(use_rate_limiter(), use_rate_limiter(), use_rate_limiter())

@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_use_concurrent_cancel():
    rate_limiter = TimesPerRateLimiter(1, 0.5)

    async def use_rate_limiter():
        try:
            async with rate_limiter.acquire():
                raise RateLimitCancelError()
        except RateLimitCancelError:
            pass

    await asyncio.gather(use_rate_limiter(), use_rate_limiter(), use_rate_limiter())
