
import time
from lgTask.task import Task

class LoopingTask(Task):
    """A Task that runs for at least a given period of time, repeatedly calling
    the run() method for the duration (instead of calling it just once like 
    normal).
    
    Be sure to check out the constants at the top of the class - specifically,
    LOOP_TIME and LOOP_SLEEP.
    """
    
    LOOP_TIME = 30 * 60
    LOOP_TIME_doc = """Seconds to run for in total"""
    LOOP_SLEEP = 0
    LOOP_SLEEP_doc = """Seconds between run() calls.  Set to 0 to disable."""
    
    def __init__(self, *args, **kwargs):
        Task.__init__(self, *args, **kwargs)
        self._loopingTask_oldRun = self.run
        self.run = self._loopingTask_run


    def _loopingTask_run(self):
        """The run() method for LoopingTasks.  The constructor for LoopingTask
        silently replaces the run() method with this method, which calls the
        run() defined in a derivative.
        """
        e = time.time() + self.LOOP_TIME
        while True:
            self._loopingTask_oldRun()
            if time.time() >= e:
                break
            if self.LOOP_SLEEP > 0:
                time.sleep(self.LOOP_SLEEP)
