
import ctypes
import inspect
from lgTask.errors import *
from lgTask.lib.compat import cherrypy_subscribe
from lgTask.lib.interruptableThread import InterruptableThread
import traceback
import pymongo

class Task(object):
    """Runs the run() function with our given kwargs in a new thread. 
    Manages communication with that thread.
    """
    
    class StopTaskError(Exception):
        """Raised in thread running task when stop() is called and times out.
        """

    def __init__(self, taskConnection, taskId=None, **kwargs):
        """taskId is not required, but that should only be used by Processor.
        """
        self.stopRequested = False
        self.taskConnection = taskConnection
        self.taskId = taskId
        self._thread = None
        self._logs = []
        
    def __str__(self):
        type = self.__class__.__name__
        if hasattr(self, taskName):
            type += "({0})".format(taskName)
        return type
        
    def error(self, message=''):
        if message:
            message += ': '
        msg = message + traceback.format_exc()
        self.log(msg)
        
    def isRunning(self):
        if self._thread and self._thread.is_alive():
            return True
        return False
        
    def log(self, message):
        """Logs the given message with this task.
        """
        self._logs.append(message)
        
    def start(self, **kwargs):
        """Starts the task with the given kwargs (calls the Task class' run()
        method.
        """
        if self._thread is not None:
            raise Exception("start() already called on this Task")
        
        self._thread = _TaskThread(self, kwargs)
        self._thread.start()
        cherrypy_subscribe(self)
        
    def stop(self, timeout=None):
        """Stops the task from running.  Blocks indefinitely with no timeout
        specified, or forcefully kills the processing thread and logs an 
        error if timeout seconds elapse.
        
        Important: after timeout, blocks for 2 additional seconds before 
        returning since we raise an exception in the running thread's stack.
        """
        if not self._thread:
            raise Exception("Task never start()ed")
        
        self.stopRequested = True
        self._thread.join(timeout)
        if self._thread.is_alive():
            self._thread.raiseException(self.StopTaskError)
            
    def _finished(self, success):
        """Called when the task finishes; must be callable multiple times,
        since StopTaskError can be raised in the middle of it.
        """
        self.taskConnection.taskStopped(self.taskId, success, self._logs)
        
        
        
class _TaskThread(InterruptableThread):
    """Runs the Task's code"""
    
    def __init__(self, task, kwargs):
        InterruptableThread.__init__(self)
        self.task = task
        self.kwargs = kwargs
        
    def run(self):
        success = True
        try:
            try:
                self.task.run(**self.kwargs)
            except Exception:
                success = False
                self.task.error()
            finally:
                self.task._finished(success)
        except self.task.StopTaskError:
            # We've been interrupted; if we were interrupted in run(), the
            # Exception block above would have caught it.  So we must
            # have been interrupted in _finished.
            self.task._finished(success)
            
