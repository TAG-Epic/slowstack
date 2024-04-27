from abc import ABC, abstractmethod
from typing import Generator, ContextManager

from slowstack.common.status import RateLimitStatus


class BaseRateLimiter(ABC):
    @abstractmethod
    def status_feed(self) -> Generator[RateLimitStatus, None, None]:
        """
        A live feed of rate limit events

        This is generally used to wait until :meth:`acquire` is ready but without acquiring it.

        .. note::
            To simplify the use of this, this needs to emit the current status when starting to iterate
        """
        ...

    @abstractmethod
    def status(self) -> RateLimitStatus:
        """
        The status of the rate limiter.

        If this is :attr:`RateLimitStatus.UNLOCKED` this means that the next call to :meth:`acquire` will instantly pass
        """
        ...

    @abstractmethod
    def acquire(self, *, wait: bool = True) -> ContextManager[None]:
        """
        Acquire the rate limiter until the context manager is exited

        .. warn::
            It is important that the ``__enter__`` finishes before the :meth:`status_feed` gets the :attr:`RateLimitStatus.LOCKED` message.

        Parameters:
            wait:
                If the rate limiter should wait for the rate limiter to be ready.
                if this is :data:`False`, this will raise :exc:`RateLimitedError`

        Raises:
            RateLimitedError:
                If ``wait`` was set to :data:`False` and the rate limiter was locked.
        """
        ...
