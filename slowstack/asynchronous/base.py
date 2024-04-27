from abc import ABC, abstractmethod
from typing import AsyncGenerator, AsyncContextManager

from ..common.status import RateLimitStatus


class BaseRateLimiter(ABC):
    @abstractmethod
    async def status_feed(self) -> AsyncGenerator[RateLimitStatus, None]:
        """
        A live feed of rate limit events

        This is generally used to wait until :meth:`acquire` is ready but without acquiring it.

        .. note::
            To simplify the use of this, this needs to emit the current status when starting to iterate
        """
        ...

    @abstractmethod
    async def status(self) -> RateLimitStatus:
        """
        The status of the rate limiter.

        If this is :attr:`RateLimitStatus.UNLOCKED` this means that the next call to :meth:`acquire` will instantly pass
        """
        ...

    @abstractmethod
    def acquire(self, *, wait: bool = True) -> AsyncContextManager[None]:
        """
        Acquire the rate limiter until the context manager is exited

        .. warning::
            It is important that the ``__enter__`` finishes before the :meth:`status_feed` gets the :attr:`RateLimitStatus.LOCKED` message.

        Parameters:
            wait:
                If the rate limiter should wait for the rate limiter to be ready.
                if this is :data:`False`, this will raise :exc:`RateLimitedError`

        Raises:
            RateLimitedError:
                If ``wait`` was set to :data:`False` and the rate limiter was locked.

        .. warn::
            There can be max 1 asyncio context switch between :meth:`status_feed` switching to :attr:`RateLimitStatus.LOCKED` and ``__aenter__`` finishing.

            If not, setting ``wait`` to :data:`False` will not work when this rate limiter is used in parent rate limiters.

            This will be fixed in the future.
        """
        ...
