
import ctypes
import inspect
import threading
import time

class InterruptableThread(threading.Thread):
    """A thread class which has a raiseException() function that may be 
    used to raise an arbitrary exception at any point in the running 
    thread code.
    """


    def raiseException(self, exceptionClass, ensureDeath=True):
        """Thanks to http://stackoverflow.com/questions/323972/is-there-any-way-to-kill-a-thread-in-python
        for providing a method of raising an exception in a thread to neatly
        kill it while respecting python cleanup.

        ensureDeath -- If specified as False, don't wait to join() the thread.
            Just assume that the specified exceptionClass did, or will 
            eventually, terminate the thread.
        
        Call this from the main thread to raise an exception of type 
        exceptionClass in the running thread's code.
        """
        if not inspect.isclass(exceptionClass):
            raise ValueError("Requires exception class, not instance")
        
        if ensureDeath:
            while self.is_alive():
                self._raiseException(exceptionClass)
                self.join(1)
        else:
            self._raiseException(exceptionClass)


    def _raiseException(self, exceptionClass):
        """Actually raise the exception; this fails if the thread is in
        system calls, so we need to keep calling this until it our
        thread is no longer alive.
        """
        tid = ctypes.c_long(self.ident)
        res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            tid
            , ctypes.py_object(exceptionClass)
        )
        if res == 0:
            if self.is_alive():
                raise ValueError("invalid thread id")
            else:
                # This is ok.  The thread might have stopped naturally; ident
                # should be reliable enough that we don't need to worry about
                # this case.
                pass
        elif res != 1:
            # "if it returns a number greater than one, you're in trouble,
            # and you should call it again with exc=NULL to revert the effect"
            ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
            raise SystemError("Failed to raiseException in thread")


