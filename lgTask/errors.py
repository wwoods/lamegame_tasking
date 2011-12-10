"""Exception classes for lamegame_tasking"""

class ProcessorAlreadyRunningError(Exception):
    """This processor is already running."""

class TaskKwargError(Exception):
    """The task was passed an invalid parameter, or different kwargs from
    the running singleton."""

