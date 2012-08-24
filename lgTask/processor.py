
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
                        # Since it might be dependent on our process and
                        # will be defunct, we won't be able to tell if it's 
                        # dead.  We can be reasonably sure that breaking the
                        # lock is OK.
                        self.release(force = True)
                else:
                    # The process is verified as dead
                    self.release(force = True)

                # If we get here, we want to retry
                return self.acquire(killExisting=killExisting)
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
            # Threaded tasks execute in a slave process to the processor itself.
            # There are a few reasons for this:
            # 1. If code changes, and we need to reboot the processor, the
            #    slave process can finish running its tasks before exiting
            #    but the processor can restart and keep going with new tasks.
            # 2. If a task misbehaves and wreaks havoc, the processor itself
            #    will not be affected.
            self._useThreading = (
                self._useMultiprocessing 
                and self.config.get('threaded', False)
            )
            if self._useThreading:
                self._useMultiprocessing = False
                self.log("Using threading")
                self._slave = None
            elif self._useMultiprocessing:
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
                    except (Exception, OSError):
                        # _consume has its own error logging; just wait and
                        # retry.
                        time.sleep(self.NO_TASK_CHECK_INTERVAL)
                        result = False
                    if not result:
                        if len(self._monitors) == 0:
                            if self._stopOnNoTasks:
                                # This is a test, we're not running anything,
                                # so done
                                break
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
                        f.write(
                            "Starting process at {0}\n".format(
                                datetime.datetime.utcnow().isoformat()
                            )
                        )

                if self._useThreading:
                    # Run tasks as threads in the Processor's forked processing
                    # task.
                    slave = self._getSlave()
                    slave.execute(taskData)
                    # We use the process variable to get our pid for tracking
                    # the new task; the slave process suffices
                    process = slave
                elif self._useMultiprocessing:
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

    def _getSlave(self):
        """Returns a fork'd slave process."""
        if (
                self._slave is not None 
                and self._slave.is_alive()
                and self._slave.isAccepting()
            ):
            return self._slave

        self._slave = _ProcessorSlave(self)
        self._slave.start()
        return self._slave
        
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
                        pid = int(f.read().split(',')[0])
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
                    if not self._useThreading:
                        os.waitpid(pid, 0)
                    else:
                        # The pid being alive can still mean the task is dead.
                        # We also have to periodically check the pid file
                        raise OSError
                except OSError:
                    # If we're waiting on a non-child process, we have to do 
                    # our own polling loop
                    while True:
                        try:
                            os.kill(pid, 0)
                            time.sleep(0.1)
                            if self._useThreading:
                                with open(self._getPidFile(tid), 'r') as f:
                                    data = f.read()
                                if ',done' in data:
                                    # Task marked as done, don't need to wait
                                    # on pid to exit
                                    raise OSError
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


class _ProcessorSlave(multiprocessing.Process):

    MAX_TIME = 3600
    MAX_TIME_doc = """Max time, in seconds, to accept tasks."""

    def __init__(self, processor):
        multiprocessing.Process.__init__(self)

        self._processor = processor
        self._connection = self._processor.taskConnection
        self._processorHome = self._processor._home
        self._taskClasses = self._processor._tasksAvailable
        self._processorPid = os.getpid()

        self._queue = multiprocessing.Queue()
        self._running = []
        self._running_doc = """List of running task threads"""
        self._isAccepting = multiprocessing.Value('b', True)
        self._startTime = time.time()


    def execute(self, taskData):
        """Queue the task to be ran."""
        self._queue.put(taskData)


    def isAccepting(self):
        """Called by Processor to see if we're still accepting"""
        return self._isAccepting.value


    def run(self):
        try:
            while True:
                try:
                    # See if we should still be running...
                    if self._isAccepting.value:
                        self._checkAccepting()
                    if not self._shouldContinue():
                        break

                    self._checkRunning()

                    taskData = self._queue.get(timeout = 4)
                    taskThread = threading.Thread(
                        target = self._runTaskThreadMain
                        , args = (taskData,)
                    )
                    taskThread.start()
                    self._running.append(taskThread)

                except Empty:
                    pass
                except Exception:
                    self._processor.log("Slave error {0}: {1}".format(
                        self.pid, traceback.format_exc()
                    ))
        finally:
            pass


    def start(self):
        """We override multiprocessing.Process.start() so that the Processor
        can gracefully exit without waiting for its child process to exit.
        The default python multiprocessing behavior is to wait until all
        child processes have exited before exiting the main process; we don't
        want this.
        """
        result = multiprocessing.Process.start(self)
        multiprocessing.current_process()._children.remove(self)
        return result


    def _checkAccepting(self):
        """Stop accepting new tasks to execute if:

        1. We've run for too long
        2. Our parent process is no longer our processor (this means that
            the processor has executed, so we won't get more)
        """
        if (
                time.time() - self._startTime >= self.MAX_TIME
                or os.getppid() != self._processorPid
            ):
            self._isAccepting.value = False


    def _checkRunning(self):
        """Check on running threads"""
        for i in reversed(range(len(self._running))):
            t = self._running[i]
            if not t.is_alive():
                self._running.pop(i)


    def _runTaskThreadMain(self, taskData):
        """Ran as the main method of a spawned thread; responsible for
        running the task passed.
        """
        taskCls = self._taskClasses[taskData['taskClass']]
        try:
            _runTask(
                taskCls
                , taskData
                , self._connection
                , self._processorHome
                , setProcTitle = False
            )
        finally:
            # This task's "pid" is no longer running, so mark the pid file as
            # done so that the executor might exit
            with open(self._processor._getPidFile(taskData['_id']), 'a') \
                as f:
                f.write(",done")


    def _shouldContinue(self):
        """A slave should stop running if it is not currently running any
        tasks and:

        It is no longer accepting new tasks.
        """
        if len(self._running) == 0 and not self._isAccepting.value:
            return False
        return True

