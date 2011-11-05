"""Exception classes for lamegame_tasking"""

class SingletonAlreadyRunning(Exception):
    """This singleton task was already running on some processor when
    start() was called.
    """
    pass
