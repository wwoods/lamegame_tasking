"""Exception classes for lamegame_tasking"""

class SingletonAlreadyRunningError(Exception):
    """This singleton task was already running on some processor when
    start() was called.
    """
    pass

class TaskKwargError(Exception):
    """The task was passed an invalid parameter, or different kwargs from
    the running singleton."""

