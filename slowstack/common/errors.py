class RateLimitedError(Exception):
    """
    An error raised when the rate limiter was :attr:`RateLimitStatus.LOCKED` and ``wait`` was :data:`False`
    """

    def __init__(self) -> None:
        super().__init__("rate limited!")


class RateLimitCancelError(Exception):
    """
    An error you want to raise when inside the async context manager of a rate limiter to mark to the rate limiter that you want to undo the use of the rate limit
    """

    def __init__(self) -> None:
        super().__init__(
            "you are supposed to catch or subclass this with your own error"
        )
