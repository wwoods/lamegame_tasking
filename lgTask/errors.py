"""Exception classes for lamegame_tasking"""

class ProcessorAlreadyRunningError(Exception):
    """This processor is already running."""

class RetryTaskError(Exception):
    """Raised by retryTask(), signals that the task is to be marked as failed
    but retried, and retried after the given delay.
    """

    def __init__(self, delay):
        self.delay = delay

class TaskKwargError(Exception):
    """The task was passed an invalid parameter, or different kwargs from
    the running singleton."""

