
import ctypes
import inspect
import traceback
import pymongo
from threading import Thread

class Task(object):
    """Runs the run() function with our given kwargs in a new thread. 
    Manages communication with that thread.
    """
    
    class StopTaskError(Exception):
        """Raised in thread running task when stop() is called and times out.
        """

    def __init__(self, taskConnection, taskId=None, **kwargs):
        """taskId is not required, but will raise an error on any logging
        function.  Used by Processor.
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
            self._thread.join(2.0)
            
    def _finished(self, success):
        self.taskConnection.taskStopped(self.taskId, success, self._logs)
        
class _TaskThread(Thread):
    """Runs the Task's code"""
    
    def __init__(self, task, kwargs):
        Thread.__init__(self)
        self.task = task
        self.kwargs = kwargs
        
    def raiseException(self, exceptionClass):
        """Thanks to http://stackoverflow.com/questions/323972/is-there-any-way-to-kill-a-thread-in-python
        for providing a method of raising an exception in a thread to neatly
        kill it while respecting python cleanup.
        
        Call this from the main thread to raise an exception of type 
        exceptionClass in the running thread's code.
        """
        if not inspect.isclass(exceptionClass):
            raise ValueError("Requires exception class, not instance")
        
        tid = self.ident()
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            tid
            , ctypes.py_object(exceptionClass)
        )
        if res == 0:
            raise ValueError("invalid thread id")
        elif res != 1:
            # "if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        
    def run(self):
        success = True
        try:
            self.task.run(**self.kwargs)
        except Exception:
            success = False
            self.task.error()
        finally:
            self.task._finished(success)
            