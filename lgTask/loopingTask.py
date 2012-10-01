
import time
from lgTask.task import Task

class LoopingTask(Task):
    """A Task that runs for at least a given period of time, repeatedly calling
    the loop() method for the duration (instead of / in addition to calling 
    run() just once like normal).  Note that if you re-define run(), you will
    need to call LoopingTask.run(kwargs) at the end, where kwargs is anything
    you want passed to each invocation of loop().
    
    Be sure to check out the constants at the top of the class - specifically,
    LOOP_TIME and LOOP_SLEEP.
    """
    
    LOOP_TIME = 30 * 60
    LOOP_TIME_doc = """Seconds to run for in total"""
    LOOP_SLEEP = 0
    LOOP_SLEEP_doc = """Seconds between run() calls.  Set to 0 to disable."""
    
    def loop(self, **kwargs):
        raise NotImplementedError()


    def run(self, **kwargs):
        """The run() method for LoopingTasks.  The constructor for LoopingTask
        silently replaces the run() method with this method, which calls the
        run() defined in a derivative.
        """
        e = time.time() + self.LOOP_TIME
        while True:
            self.loop(**kwargs)
            if time.time() >= e or self.shouldExit():
                break
            if self.LOOP_SLEEP > 0:
                time.sleep(self.LOOP_SLEEP)
        
        
    def shouldExit(self):
        """Implementation of logic to abort before LOOP_TIME is finished.  For 
        instance, when code changes are picked up on the Processor process,
        this method should return True to indicate the LoopingTask should abort
        so that it will get re-launched with the new code.
        """
        return False


