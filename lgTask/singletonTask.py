
import datetime
from lgTask.lib.timeInterval import TimeInterval
from lgTask.connection import Connection
from lgTask.task import Task
from lgTask.errors import *
import pymongo

class SingletonTask(Task):
    
    HEARTBEAT_INTERVAL = TimeInterval('5 seconds')
    HEARTBEAT_INTERVAL__doc = """Time between heartbeats for this singleton; if 
        more than two of these intervals elapse without any heartbeat, the
        task is declared non-responsive and a new instance may be launched.
        """
    
    def __init__(self, taskConnection, taskId=None, taskName=None):
        """Runs this singleton task on the given task connection.
        
        taskConnection -- lgTask.Connection - The connection for tasking.
        
        taskName -- String or None - The name of the singleton; this is what is
            used to enforce uniqueness.
        """
        self.taskName = taskName or self.__class__.__name__
        Task.__init__(self, taskConnection=taskConnection, taskId=taskId)
        
    def start(self):
        """Override Task.start() to assert singleton status first.
        """
        self.taskConnection.acquireSingleton(
            self.taskName, self.HEARTBEAT_INTERVAL
        )
        Task.start(self)
        
    def stop(self, **kwargs):
        """Override task.stop() to release singleton status.
        """
        Task.stop(self, **kwargs)
        self.taskConnection.releaseSingleton(self.taskName)

