from __future__ import annotations

import threading


class PriorityQueueContainer:
    """A container for times per uses for :class:`queue.PriorityQueue` to ignore the future when comparing greater than and less than

    Parameters
    ----------
    priority:
        The request priority. This will be compared!
    event:
        The event for when a spot is available in the rate limit

    Attributes
    ----------
    priority:
        The request priority. This will be compared!
    event:
        The event for when a spot is available in the rate limit
    """

    __slots__: tuple[str, ...] = ("priority", "event")

    def __init__(self, priority: int, event: threading.Event) -> None:
        self.priority: int = priority
        self.event: threading.Event = event

    def __gt__(self, other: PriorityQueueContainer):
        return self.priority > other.priority
