
import datetime
from lgTask.lib.interruptableThread import InterruptableThread
from lgTask.lib.timeInterval import TimeInterval
from lgTask.connection import Connection
from lgTask.task import Task
from lgTask.errors import *
import pymongo
import threading
import time

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
        
    def start(self, **kwargs):
        """Override Task.start() to assert singleton status first, and start
        heartbeat.
        """
        self.taskConnection.singletonAcquire(
            self.taskName, self.HEARTBEAT_INTERVAL
        )
        self._startHeartbeat()
        Task.start(self, **kwargs)
        
    def stop(self, **kwargs):
        """Override task.stop() to stop heartbeat and release singleton status.
        """
        Task.stop(self, **kwargs)
        self._stopHeartbeat()
        self.taskConnection.singletonRelease(self.taskName)

    def _startHeartbeat(self):
        self._heartbeat = _SingletonHeartbeat(self)
        self._heartbeat.start()
    
    def _stopHeartbeat(self):
        self._heartbeat.stop()
    
    

class _SingletonHeartbeatInterrupt(Exception):
    """Used when a Heartbeat should stop."""
        
        
        
class _SingletonHeartbeat(InterruptableThread):
    """Keeps a SingletonTask alive and well
    """
    
    def __init__(self, singleton):
        self.task = singleton
        self.stopRequested = False
        InterruptableThread.__init__(self)
        
    def stop(self):
        self.stopRequested = True
        self.raiseException(_SingletonHeartbeatInterrupt)
        
    def run(self):
        t = self.task.HEARTBEAT_INTERVAL
        t = t.days * 24 * 60 * 60 + t.seconds + t.microseconds * 1e-6
        while not self.stopRequested:
            try:
                time.sleep(t)
                self.task.taskConnection.singletonHeartbeat(self.task.taskName)
            except _SingletonHeartbeatInterrupt:
                return
