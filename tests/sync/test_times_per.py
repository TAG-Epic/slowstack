import threading
import pytest
from tests.utils import match_time
from slowstack.synchronous.times_per import TimesPerRateLimiter
from slowstack.common.errors import RateLimitCancelError, RateLimitedError


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_should_not_sleep():
    rate_limiter = TimesPerRateLimiter(5, 5)

    for _ in range(5):
        with rate_limiter.acquire():
            ...


@pytest.mark.asyncio
@match_time(0.1, 0.01)
async def test_should_sleep():
    rate_limiter = TimesPerRateLimiter(5, 0.1)

    for _ in range(10):
        with rate_limiter.acquire():
            ...


def test_repr():
    repr(TimesPerRateLimiter(1, 1))


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_exception_undos():
    rate_limiter = TimesPerRateLimiter(1, 1)

    for _ in range(100):
        try:
            with rate_limiter.acquire():
                raise RateLimitCancelError()
        except:
            pass


@pytest.mark.asyncio
async def test_no_wait():
    rate_limiter = TimesPerRateLimiter(1, 1)

    with rate_limiter.acquire(wait=False):
        ...  # Good!

    with pytest.raises(RateLimitedError):
        with rate_limiter.acquire(wait=False):
            ...


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_reset_offset_negative():
    rate_limiter = TimesPerRateLimiter(1, 1)
    rate_limiter.reset_offset_seconds = -1

    for _ in range(100):
        with rate_limiter.acquire():
            ...


@pytest.mark.asyncio
@match_time(2, 0.1)
async def test_reset_offset_positive():
    rate_limiter = TimesPerRateLimiter(1, 1)
    rate_limiter.reset_offset_seconds = 1

    for _ in range(2):
        with rate_limiter.acquire():
            ...


@pytest.mark.asyncio
@match_time(1, 0.1)
async def test_use_concurrent():
    rate_limiter = TimesPerRateLimiter(1, 0.5)

    def use_rate_limiter():
        with rate_limiter.acquire():
            pass

    threads: list[threading.Thread] = []
    for i in range(3):
        thread = threading.Thread(target=use_rate_limiter)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


@pytest.mark.asyncio
@match_time(0, 0.1)
async def test_use_concurrent_cancel():
    rate_limiter = TimesPerRateLimiter(1, 0.5)

    def use_rate_limiter():
        try:
            with rate_limiter.acquire():
                raise RateLimitCancelError()
        except RateLimitCancelError:
            pass

    threads: list[threading.Thread] = []
    for i in range(3):
        thread = threading.Thread(target=use_rate_limiter)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
