
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
        self.lastHeartbeat = self.taskConnection.singletonAcquire(
            self.taskName, self.HEARTBEAT_INTERVAL
        )
        self._startHeartbeat()
        Task.start(self, **kwargs)
        
    def stop(self, **kwargs):
        """Override task.stop() to stop heartbeat and release singleton status.
        """
        Task.stop(self, **kwargs)
        self._stopHeartbeat()
        self.taskConnection.singletonRelease(self.taskName, self.lastHeartbeat)

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
        self.join()
        
    def run(self):
        t = self.task.HEARTBEAT_INTERVAL
        t = t.days * 24 * 60 * 60 + t.seconds + t.microseconds * 1e-6
        while not self.stopRequested:
            try:
                time.sleep(t)
                self.task.lastHeartbeat = self.task.taskConnection.singletonHeartbeat(
                    self.task.taskName
                    , self.task.lastHeartbeat
                )
            except SingletonAlreadyRunning:
                # Another instance of ourselves is already running; this is
                # a critical condition and we should definitely signal error
                # status and try to abort immediately.
                # This shouldn't ever really happen, but it's good to be
                # thorough.
                self.task.error("Heartbeat update failed; found last timestamp "
                    + "not equal to expected")
                self.task.stop(timeout=0)
            except _SingletonHeartbeatInterrupt:
                return
            except:
                # The likely exception here is that we could not write a 
                # heartbeat value.  This means that we can no longer enforce
                # our singleton status, and it is probably better to abort
                # if possible.
                self.task.error("Heartbeat update failed")
                self.task.stop(timeout=0)

