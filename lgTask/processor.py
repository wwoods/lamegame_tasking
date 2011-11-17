
import imp
import inspect
import socket
from lgTask import Connection, Task, SingletonTask
from lgTask.errors import SingletonAlreadyRunningError
from lgTask.lib.reprconf import Config
from lgTask.lib.timeInterval import TimeInterval
import os

class Processor(SingletonTask):
    """Processes tasks for the given db.
    """

    HEARTBEAT_INTERVAL = TimeInterval('0.5 seconds')
    MAX_TASKS = 1
    MAX_TASKS__doc = """Python processes are inherently single-threaded because of the GIL.  MAX_TASKS only ever makes sense being 1, but is placed into a variable for this documentation."""
    
    def __init__(self, config, taskName=None):
        """Creates a new task processor.
        config -- File path (String) or dict object - We look for the following
            parameters:
            taskDatabase -- Connection string (see lgTask.Connection) - Where
                the task database to pull tasks from resides.
            taskDir -- Local path - Scan this folder for *.py files and
                import any classes who derive from "Task".  These are the
                tasks that this processor can process.
            taskName -- Name of this task processor.  Will not be used if
                taskName is passed to the __init__ function.
        taskName -- String - The name to run this processor under.  If None,
            use config['taskName'].  If that's None, use hostname.
        """
        if isinstance(config, dict):
            self.config = config
        elif isinstance(config, basestring):
            self.config = Config(config)
        else:
            raise ValueError("Unrecognized config type")
        
        taskName = taskName or self.config['lgTaskProcessor'].get('taskName') 
        taskName = taskName or socket.gethostname()
        
        connection = Connection(self.config['lgTaskProcessor']['taskDatabase'])
        connection.ensureIndexes()
        
        SingletonTask.__init__(self, taskConnection=connection
                , taskName=taskName)
        
        self._initTasksAvailable(self.config['lgTaskProcessor']['taskDir'])
        self._tasks = []
        
    def log(self, message):
        print(message)
        
    def run(self):
        """Run until our stop() is called"""
        while not self.stopRequested:
            self._checkTasks()
            if not self._consume():
                import time
                time.sleep(0.01)
        
        if self.stopRequested == 'onConsume':
            # This is only ever used for debugging, but finish out all
            # queued tasks.
            while self._consume() or len(self._tasks) != 0:
                self._checkTasks()
                import time
                time.sleep(0.01)
                
        
    def stop(self, timeout=None, onNoTasksToConsume=False):
        """Overrides Task.stop() since we need to be sure that all of our 
        running tasks are also stopped.
        
        If onNoTasksToConsume is specified, this call will block until both
        of the following are true:
            There are no more tasks to consume
            All running tasks are finished
        """
        if onNoTasksToConsume:
            self.stopRequested = 'onConsume'
            self._thread.join()
            SingletonTask.stop(self)
        else:
            SingletonTask.stop(self, timeout=timeout)
            
        # Our thread is dead; now merge other threads
        while len(self._tasks) > 0:
            t = self._tasks[0]
            if t.isRunning():
                t.stop(timeout=timeout)
            self._checkTasks()
            
    def _checkTasks(self):
        for t in self._tasks[:]:
            if t.isRunning():
                continue
            
            # The task is done
            self._tasks.remove(t)
            self.log("Finished task {0}: {1}".format(
                getattr(t, 'taskName', t.__class__.__name__)
                , t._logs[-1] if len(t._logs) > 0 else '(no log)'
            ))
                
        
    def _consume(self):
        """Grab and consume a task if one is available.  All exceptions should
        be handled inline (not raised).
        
        Returns True if a task is started.  False otherwise.
        """

        if len(self._tasks) >= self.MAX_TASKS:
            # We are already running our max count.  Do not accept new work.
            return False

        c = self.taskConnection
        try:
            task = c.startTask(self._tasksAvailable)
        except SingletonAlreadyRunningError:
            # We don't really care about this from a processor logging
            # point of view.
            pass
        except Exception as e:
            self.error("While starting task")
        else:
            if task is not None:
                self._tasks.append(task)
                self.log("Started task {0}".format(
                    getattr(task, 'taskName', task.__class__.__name__)
                ))
                return True
        
    def _initTasksAvailable(self, taskDir):
        self._tasksAvailable = {}
        for file in os.listdir(taskDir):
            path = os.path.join(taskDir, file)
            if os.path.isfile(path) and path[-3:] == '.py':
                self._loadTasksFrom(path)
                
    def _loadTasksFrom(self, path):
        """Thanks http://stackoverflow.com/questions/6811902/import-arbitrary-named-file-as-a-python-module !
        """
        module_name = os.path.splitext(os.path.basename(path))[0]
        with open(path, 'U') as module_file:
            module = imp.load_module(
                module_name
                , module_file
                , path
                , ( ".py", "U", imp.PY_SOURCE )
            )
            
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and issubclass(obj, Task):
                self._tasksAvailable[name] = obj
