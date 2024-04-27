import asyncio
from typing import Any


async def cancel_task(task: asyncio.Task[Any]) -> None:
    """
    Safely cancels a task by sending a cancel request and waiting for it to stop

    .. warn::
        A task may refuse being cancelled. If so, please add a timeout as this function will wait forever

    Raises:
        BaseException:
            Any exception the task raised except :exc:`asyncio.CancelledError`
    """
    if task.done():
        if (exception := task.exception()) is not None:
            raise exception
    else:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
