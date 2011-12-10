"""lgTask provides lamegame_tasking's functionality.
"""

from .errors import *
from .connection import Connection
from .task import Task
from .runTask import _runTask
from .processor import Processor
from .lib.timeInterval import TimeInterval

