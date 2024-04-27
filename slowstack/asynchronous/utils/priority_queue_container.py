from __future__ import annotations

import asyncio


class PriorityQueueContainer:
    """A container for times per uses for :class:`asyncio.PriorityQueue` to ignore the future when comparing greater than and less than

    Parameters
    ----------
    priority:
        The request priority. This will be compared!
    future:
        The future for when the request is done

    Attributes
    ----------
    priority:
        The request priority. This will be compared!
    future:
        The future for when the request is done
    """

    __slots__: tuple[str, ...] = ("priority", "future")

    def __init__(self, priority: int, future: asyncio.Future[None]) -> None:
        self.priority: int = priority
        self.future: asyncio.Future[None] = future

    def __gt__(self, other: PriorityQueueContainer):
        return self.priority > other.priority
