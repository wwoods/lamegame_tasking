"""Exception classes for lamegame_tasking"""

class SingletonAlreadyRunningError(Exception):
    """This singleton task was already running on some processor when
    start() was called.
    """
    pass

class TaskKwargError(Exception):
    """The task was passed an invalid parameter."""
    def __init__(self, param, value):
        Exception.__init__(
            self, "TaskKwargError: {0} ({1})".format(param, value))

