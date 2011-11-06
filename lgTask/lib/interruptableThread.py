
import ctypes
import inspect
import threading

class InterruptableThread(threading.Thread):
    """A thread class which has a raiseException() function that may be 
    used to raise an arbitrary exception at any point in the running 
    thread code.
    """
    
        
    def raiseException(self, exceptionClass):
        """Thanks to http://stackoverflow.com/questions/323972/is-there-any-way-to-kill-a-thread-in-python
        for providing a method of raising an exception in a thread to neatly
        kill it while respecting python cleanup.
        
        Call this from the main thread to raise an exception of type 
        exceptionClass in the running thread's code.
        """
        if not inspect.isclass(exceptionClass):
            raise ValueError("Requires exception class, not instance")
        
        tid = self.ident
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            tid
            , ctypes.py_object(exceptionClass)
        )
        if res == 0:
            # raise ValueError("invalid thread id")
            # This is ok.  The thread might have stopped naturally; ident
            # should be reliable enough that we don't need to worry about
            # this case.
            pass
        elif res != 1:
            # "if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
            raise Exception("Failed to raiseException in thread")
            
