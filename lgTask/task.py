
import datetime
from lgTask.errors import *
from lgTask.lib.compat import cherrypy_subscribe
from lgTask.lib.interruptableThread import InterruptableThread
import traceback

class Task(object):
    """Runs the run() function with our given kwargs in a new thread. 
    Manages communication with that thread.
    """

    DEBUG_TIMING = False
    DEBUG_TIMING__doc = "Add initialization timestamps to logs"
    
    class StopTaskError(Exception):
        """Raised in thread running task when stop() is called and times out.
        """

    def __init__(self, taskConnection, taskId=None, taskData={}, **kwargs):
        """taskId is not required, but that should only be used by Processor.
        """
        self.stopRequested = False
        self.taskConnection = taskConnection
        self.taskData = taskData
        self.taskId = taskId
        self._thread = None
        self._logs = []

    def __str__(self):
        type = self.__class__.__name__
        return "Task<{0} - {1}>".format(type, self.taskId)
        
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

    def retryTask(self, delay, maxRetries):
        """Should be called right before a task is about to fail.  Runs a 
        delayed task with the exact same arguments as the current one, with
        the caveat that it will not retry more than maxRetries times (since
        that could end up with an infinite loop).

        This function works by raising a RetryTaskError - if caught, the retry
        will not work.

        Raises an Exception if maxRetries is met.
        """
        retryCount = self.taskData.get('retry', 0)
        if retryCount >= maxRetries:
            raise Exception("Not retrying - at {0} retries".format(retryCount))
        else:
            raise RetryTaskError(delay)
        
    def start(self, **kwargs):
        """Starts the task with the given kwargs (calls the Task class' run()
        method.
        """
        # Note - this is kept here in case I'm wrong and it is used...
        raise NotImplementedError("THIS IS OBSOLETE AND SHOULD NOT BE USED")
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
        raise NotImplementedError("Obsolete; same as start()")
        self.taskConnection.taskStopped(self, success, self._logs[-1])
        
        
        
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
            
