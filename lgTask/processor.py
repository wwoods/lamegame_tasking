
import atexit
import datetime
import errno
import imp
import inspect
import json
import multiprocessing
from Queue import Empty, Queue
import os
import signal
import site
import socket
import subprocess
import sys
import threading
import time
import traceback
from lgTask import Connection, Task, _runTask
from lgTask.errors import ProcessorAlreadyRunningError
from lgTask.lib.interruptableThread import InterruptableThread
from lgTask.lib.reprconf import Config
from lgTask.lib.timeInterval import TimeInterval
from lgTask.scheduleAuditTask import ScheduleAuditTask


class _JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat() + 'Z'
        else:
            return json.JSONEncoder.default(self, obj)

class _JsonDecoder(json.JSONDecoder):
    def default(self, s):
        if len(s) == 27 and s.endswith('Z'):
            return datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Fallback
        return json.JSONDecoder.default(self, s)


class ProcessorLock(object):
    """Tests against a lock folder with the given name."""

    def __init__(self, path):
        self._path = os.path.abspath(path)


    def acquire(self, killExisting=False):
        """Raises a ProcessorAlreadyRunningError if cannot be acquired

        If killExisting is specified, kill any process that has the lock.
        """
        try:
            os.makedirs(self._path)
        except OSError, e:
            if e.errno != errno.EEXIST:
                raise
            else:
                pid = open(self._getPidFile(), 'r').read()
                if os.path.exists('/proc/' + pid):
                    if not killExisting:
                        raise ProcessorAlreadyRunningError()
                    else:
                        try:
                            os.kill(int(pid), signal.SIGKILL)
                        except OSError, e:
                            if e.errno != errno.ESRCH:
                                raise
                        time.sleep(0.01)

                # If we get here, we want to retry
                self.release(force = True)
                return self.acquire()
        else:
            with open(self._getPidFile(), 'w') as f:
                f.write(str(os.getpid()))


    def release(self, force = True):
        """Releases the lock if the lock belongs to this process or force is
        True.  Silently does not release the lock if it belongs to another
        process.
        """
        try:
            pid = open(self._getPidFile(), 'r').read()
        except OSError, e:
            # If no pid file, ok
            if e.errno != errno.ENOENT:
                raise
        else:
            if pid == os.getpid() or force:
                # Clear it out!
                os.remove(self._getPidFile())
                os.rmdir(self._path)


    def _getPidFile(self):
        """Returns the absolute path to this lock's pid file."""
        return os.path.join(self._path, 'lock.pid')


class Processor(object):
    """Processes tasks for the given db.  Also performs certain administrative
    actions (or ensures that they are performed) on the tasks database.
    """

    LOGFILE = 'processor.log'
    NO_TASK_CHECK_INTERVAL = 0.5
    NO_TASK_CHECK_INTERVAL__doc = 'Seconds to wait when we are not running ' \
        + 'any tasks and cannot find a new task to run.'
    
    def __init__(self, home='.'):
        """Creates a new task processor operating out of the current working
        directory.  Uses processor.cfg as the config file.

        home -- The directory to run the processor out of.
        """
        self._home = os.path.abspath(home)
        self.config = Config(self.getPath("processor.cfg"))['processor']

        connection = Connection(self.config['taskDatabase'])
        connection._ensureIndexes()
        self.taskConnection = connection
        
        # Add other path elements before trying imports
        for i,path in enumerate(self.config.get('pythonPath', [])):
            sys.path.insert(
                i
                , os.path.abspath(os.path.join(home, path))
            )
        
        self._tasksAvailable = self._getTasksAvailable(self._home)
        self._monitors = {}

        self._startTaskQueue = Queue()
        self._stopOnNoTasks = False

    def error(self, message):
        error = traceback.format_exc()
        self.log("{0} - {1}".format(message, error))

    @classmethod
    def fork(cls, home='.', killExisting=True):
        """Forks a new subprocess to run a Processor instance out of the
        given home directory.  Useful for e.g. debug environments, where the
        main script should also spawn a processor but perhaps does something
        else, like serving webpages.

        The fork is automatically registered with an atexit to terminate the
        forked Processor.  Look at lamegame_tasking/bin/lgTaskProcessor for
        a standalone script.

        Automatically forwards site.ENABLE_USER_SITE to forked interpreter.

        Returns the function that is already registered with atexit, but may
        be called manually if you need to kill the fork.

        killExisting, if True, will kill any processor holding
          the lock that this processor will need (thus freeing the lock).  Since
          fork() is primarily meant for debugging code that expects the fork
          to always be running with the latest version, this defaults to True.
        """
        hasS = ('-s' in sys.argv)
        runProcess = os.path.abspath(os.path.join(
            __file__
            , '../../bin/lgTaskProcessor'
        ))
        args = [ sys.executable ]
        # site.ENABLE_USER_SITE tells us if, for instance, -s was passed
        if not site.ENABLE_USER_SITE:
            args.append('-s')
        args.extend([ runProcess, home ])
        if killExisting:
            args.append('-killExisting')

        args = tuple(args)
        proc = subprocess.Popen(args)
        def terminateProc():
            # We have to both terminate AND wait, or we'll get defunct
            # processes lying around
            if proc.poll() is None:
                # Process is still running
                proc.terminate()
            proc.wait()
        atexit.register(terminateProc)
        return terminateProc
        
    def getPath(self, path):
        """Returns the absolute path for path, taking into account our
        home directory.
        """
        return self._home + '/' + path

    def log(self, message):
        now = datetime.datetime.utcnow().isoformat()
        print(message)
        open(self.getPath('logs/' + self.LOGFILE), 'a').write(
            "[{0}] {1}\n".format(now, message)
        )
        
    def run(self, killExisting=False):
        """Run indefinitely or (for debugging) until no tasks are available.

        If killExisting is specified, then forcibly break the lock by killing
        the process that currently has the lock.
        """

        # .lock is automatically appended to FileLock (processor.lock)
        self._lock = ProcessorLock(self._home + '/.processor.lock')
        # raises ProcessorAlreadyRunningError on fail
        self._lock.acquire(killExisting=killExisting)
        try:
            try:
                os.makedirs(self._home + '/logs')
            except OSError:
                pass
            try:
                os.makedirs(self._home + '/pids')
            except OSError:
                pass

            self.log("Tasks loaded: {0}".format(self._tasksAvailable.keys()))

            # The advantage to multiprocessing is that since we are forking
            # the process, our libraries don't need to load again.  This
            # means that the startup time for new tasks is substantially
            # (~ 0.5 sec in my tests) faster.
            # The disadvantage is that there's a bug in python 2.6 that
            # prohibits it from working from non-main threads.
            self._useMultiprocessing = (
                sys.version_info[0] >= 3
                or sys.version_info[1] >= 7
                or threading.current_thread().name == 'MainThread'
            )
            if self._useMultiprocessing:
                self.log("Using multiprocessing")
            else:
                self.log("Not using multiprocessing - detected non-main thread")

            self.log("Processor started - pid " + str(os.getpid()))


            # Run the scheduler loop; start with 1 task
            self._startTaskQueue.put('any')
            lastTime = datetime.datetime.utcnow()
            while True:
                self._monitorCurrentPids()
                lastTime = self._schedulerAudit(lastTime)
                try:
                    next = self._startTaskQueue.get(timeout=5)
                except Empty:
                    # Timeout, no task start tokens are available.
                    # Time to run scheduler again.
                    # For now, we can safely assume that this always means that
                    # any tasks running are long-running, and can grab a new 
                    # token.
                    self._startTaskQueue.put('any')
                else:
                    # We got a token, OK to start a new task
                    try:
                        result = self._consume()
                    except:
                        # _consume has its own error logging; just wait and
                        # retry.
                        time.sleep(self.NO_TASK_CHECK_INTERVAL)
                        result = False
                    if not result:
                        if self._stopOnNoTasks:
                            break
                        elif len(self._monitors) == 0:
                            time.sleep(self.NO_TASK_CHECK_INTERVAL)
                            self._startTaskQueue.put('any')
        finally:
            self._lock.release()

    def start(self):
        """Run the Processor asynchronously for test cases.
        """
        self.NO_TASK_CHECK_INTERVAL = 0.01
        self._thread = InterruptableThread(target=self.run)
        self._thread.start()

    def stop(self):
        """Halt an asynchronously started processor.
        """
        self._stopOnNoTasks = True
        self._thread.join(10)
        if self._thread.is_alive():
            class ProcessorStop(Exception):
                pass
            self._thread.raiseException(ProcessorStop)

    def _consume(self):
        """Grab and consume a task if one is available.  All exceptions should
        be handled inline (not raised).
        
        Returns True if a task is started.  False otherwise.
        """

        c = self.taskConnection
        try:
            taskData = c._startTask(self._tasksAvailable)
            if taskData is None:
                return False
        except Exception as e:
            self.error("While starting task")
        else:
            taskId = taskData['_id']
            latestStartTime = datetime.datetime.utcnow()
            try:
                # We write the full data for the task to the logfile as a 
                # security 
                # measure - if we pass it in the program arguments, anyone could
                # read them.  It's also nice to have the exact parameters at 
                # start
                # available as part of the log.
                with open(self._getLogFile(taskId), 'w') as f:
                    f.write(json.dumps(taskData, cls=_JsonEncoder) + '\n')

                if Task.DEBUG_TIMING:
                    open(self._getLogFile(taskId), 'a').write(
                        "Starting process at {0}\n".format(
                            datetime.datetime.utcnow().isoformat()
                        )
                    )

                if self._useMultiprocessing:
                    args = ( 
                        self._tasksAvailable[taskData['taskClass']]
                        , taskData
                        , self.taskConnection
                        , self._home
                    )
                    process = multiprocessing.Process(
                        target=_runTask
                        , args=args
                    )
                    process.start()
                else:
                    # Get our task runner script
                    taskRunner = os.path.abspath(os.path.join(
                        __file__
                        , '../../bin/lgTaskRun'
                    ))
                    
                    args = (taskRunner,taskId,self._home)
                    process = subprocess.Popen(args)

                # Start was successful, start a PID file and monitor it
                pid = process.pid
                with open(self._getPidFile(taskId), 'w') as pidFile:
                    pidFile.write(str(pid))
                self._monitorPid(taskId, pid, latestStartTime)
                return True
            except Exception as e:
                open(self._getLogFile(taskId), 'a').write(
                    'Error on launch: ' + traceback.format_exc()
                )
                self.taskConnection.taskDied(taskId, latestStartTime)
                raise

    def _getLogFile(self, tid):
        return self.getPath('logs/' + str(tid) + '.log')

    def _getPidFile(self, tid):
        return self.getPath('pids/' + str(tid) + '.pid')
        
    @classmethod
    def _getTasksAvailable(cls, home):
        """Import the "tasks" module, and parse its members for derivation
        from Task.

        home -- Directory to try importing tasks from
        """
        import sys
        oldPath = sys.path[:]
        sys.path.insert(0, home)
        # This is primarily for testing, but we have to unload tasks if it
        # is loaded.  Otherwise, the updated sys path will be overlooked as
        # python will use its cached tasks module.
        try:
            del sys.modules['tasks']
            del tasks
        except (KeyError, UnboundLocalError):
            import tasks
        sys.path.pop(0)

        tasksAvailable = { 'ScheduleAuditTask': ScheduleAuditTask }
        for name, obj in inspect.getmembers(tasks):
            if inspect.isclass(obj) and issubclass(obj, Task):
                tasksAvailable[name] = obj
        return tasksAvailable

    def _monitorCurrentPids(self):
        """Spawn a monitor for each task in the pids folder; used on init and
        could be used by sanity checks.  Will not double monitor any tasks.
        """
        for file in os.listdir(self.getPath('pids')):
            path = os.path.join(self.getPath('pids'), file)
            if os.path.isfile(path) and path[-4:] == '.pid':
                tid = file[:-4]
                try:
                    with open(path, 'r') as f:
                        pid = int(f.read())
                    oldMonitor = self._monitors.get(tid)
                    if oldMonitor is None or not oldMonitor.isAlive():
                        latestStart = datetime.datetime.utcfromtimestamp(
                            os.path.getmtime(path)
                        )
                        self._monitorPid(tid, pid, latestStart)
                except Exception, e:
                    self.log("Error on loading task pid {0}: {1}".format(
                        tid, e
                    ))

    def _monitorPid(self, tid, pid, latestStart):
        """Monitors the specified task, which is running under the given pid.

        latestStart - See _monitorPid_thread
        """
        t = threading.Thread(
            target=self._monitorPid_thread
            , args=(tid, pid, latestStart)
        )
        t.daemon = True
        self._monitors[tid] = t
        t.start()
        self.log("Monitoring task {0}:{1}".format(tid, pid))
    
    def _monitorPid_thread(self, tid, pid, latestStart):
        """Monitors the given process until it terminates.  This thread is
        daemonic; that is, it is assumed insignificant if it dies (it must
        be able to recover).

        This thread is responsible for cleaning up the pid file on task
        completion.

        latestStart - An upper bound on the start time for the process.  Used
            for preventing database corruption on task death.
        """
        while True:
            try:
                try:
                    # For efficiency, we want to use event-driven wait for
                    # a process if possible.
                    os.waitpid(pid, 0)
                except OSError:
                    # If we're waiting on a non-child process, we have to do 
                    # our own polling loop
                    while True:
                        try:
                            os.kill(pid, 0)
                            time.sleep(0.1)
                        except OSError:
                            break
            except Exception, e:
                self.log("Monitor exception for {0}:{1} - {2}".format(
                    tid, pid, e
                ))
            else:
                # The only way to reach here is for the process to have died.
                # We want to clean up the pid file and ensure that the database
                # entry is OK.
                try:
                    self.taskConnection.taskDied(tid, latestStart)
                    os.remove(self._getPidFile(tid))
                except Exception, e:
                    self.log(
                        "Monitor exception in end for {0}:{1} - {2}".format(
                            tid, pid, e.__class__.__name__ + ': ' + str(e)
                        )
                    )
                break

        # Try to add a new process
        self._monitors.pop(tid)
        self._startTaskQueue.put('any')
        self.log("Monitor finished {0}:{1}".format(tid, pid))

    def _schedulerAudit(self, lastTime):
        """Checks if we need to batch up a new scheduler audit task and returns
        the new scheduled time
        """

        now = datetime.datetime.utcnow()
        if lastTime is None or now.minute != lastTime.minute:
            self.taskConnection.batchTask('30 minutes', 'ScheduleAuditTask')
            lastTime = now
        return lastTime

