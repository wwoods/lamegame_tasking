
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
import thread
import threading
import time
import traceback
from lgTask import Connection, Task, _runTask
from lgTask.errors import KillTaskError, ProcessorAlreadyRunningError
from lgTask.lib import portalocker
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
    """Tests against a lock pidfile with the given name."""

    def __init__(self, path):
        self._path = os.path.abspath(path)


    def acquire(self, killExisting=False):
        """Raises a ProcessorAlreadyRunningError if cannot be acquired

        If killExisting is specified, kill any process that has the lock.
        """
        # So, we can just use the handy portalocker... which handles all of
        # the pid management and whatnot for us.  Note that we must keep our
        # file handle open for our duration... which doesn't necessarily work
        # for us since we do some forking and whatnot that relies on keeping
        # some file descriptors open.  SO we'll use the lock mechanism just to
        # "grab" priority, then release it
        # Note that we want to open the file for read / write if it exists 
        # without truncating it, but create it if it doesn't exist.
        # See http://stackoverflow.com/questions/10349781/how-to-open-read-write-or-create-a-file-with-truncation-possible
        fd = os.open(self._path, os.O_RDWR | os.O_CREAT)
        with os.fdopen(fd, "r+") as f:
            try:
                portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.LockException:
                # Someone else is writing to the file; they take precedence
                raise ProcessorAlreadyRunningError()
            
            # Go to end of file and see if anything's been written
            f.seek(0, 2)
            if f.tell() != 0:
                # Someone else has the pid file
                f.seek(0)
                try:
                    oldPid = int(f.read())
                    # See if they're still running; if they are, we'll yield
                    os.kill(oldPid, 0)
                    # If we get here, then they're alive and we need to let
                    # them run... unless killExisting was specified
                    if not killExisting:
                        raise ProcessorAlreadyRunningError()
                    else:
                        try:
                            os.kill(oldPid, signal.SIGKILL)
                        except OSError, e:
                            if e.errno != errno.ESRCH:
                                raise
                except OSError, e:
                    if e.errno != errno.ESRCH:
                        raise
                    # They are no longer alive, so we can reasonably proceed
                    # with taking over the lock

            # We now need to replace whatever contents there were with our
            # pid, and then we've acquired it            
            f.seek(0)
            f.write(str(os.getpid()))
            f.truncate()


    def release(self):
        """Releases the lock under the assumption that it belongs to our
        process.
        """
        os.remove(self._path)


class _ProcessorStop(Exception):
    pass


class Processor(object):
    """Processes tasks for the given db.  Also performs certain administrative
    actions (or ensures that they are performed) on the tasks database.
    """

    LOGFILE = 'processor.log'

    LOAD_SLEEP_SCALE = 0.1
    LOAD_SLEEP_SCALE_doc = """Seconds to wait for a running task to finish
            before starting a parallel task.  Scaled by system load divided
            by number of cores.  Set to 0 for tests, which will disable both
            this and LOAD_SLEEP_SCALE_NO_TASK."""

    LOAD_SLEEP_SCALE_NO_TASK = 4.0
    LOAD_SLEEP_SCALE_NO_TASK_doc = """LOAD_SLEEP_SCALE for when we tried to
            get a task but none were waiting"""

    CLEANUP_INTERVAL = 3600
    CLEANUP_INTERVAL_doc = """Seconds between cleaning up logs"""

    KEEP_LOGS_FOR = 30*24*60*60
    KEEP_LOGS_FOR_doc = """Seconds to keep logs around for"""

    _LOG_DIR = "logs/"
    _LOG_DIR_doc = """Directory used for logs; only for testing.  Must end
            in slash."""

    KILL_INTERVAL = 5
    KILL_INTERVAL_doc = """Seconds between checking if we should kill a task"""

    KILL_TOLERANCE = 10
    KILL_TOLERANCE_doc = """Min seconds between sending a kill message to a 
            task.  Used to prevent killing cleanup code."""

    MONITOR_CHECK_INTERVAL = 0.01
    MONITOR_CHECK_INTERVAL_doc = """Minimum seconds between checking if tasks
            have died."""
    
    def __init__(self, home='.'):
        """Creates a new task processor operating out of the current working
        directory.  Uses processor.cfg as the config file.

        home -- The directory to run the processor out of.
        """
        self._home = os.path.abspath(home)

        # Once we have home set up, do everything in a try and log the error if
        # we get one
        try:
            self.config = Config(self.getPath("processor.cfg"))['processor']
            allowed = [ 'taskDatabase', 'pythonPath'
                    , 'threaded', 'rconsole' 
            ]
            for k in self.config.keys():
                if k not in allowed:
                    raise ValueError(
                            "Processor parameter '{0}' unrecognized".format(k))

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

            self._cleanupThread = None
            self._cleanupThread_doc = """Thread that cleans up the _LOG_DIR"""

            self._startTaskQueue = Queue()
            self._stopOnNoTasks = False

            self._useRConsole = self.config.get('rconsole', False)
        except:
            self.error('During init')
            raise

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
        proc = subprocess.Popen(args, close_fds = True)
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
        open(self.getPath(self._LOG_DIR + self.LOGFILE), 'a').write(
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

            # Can we use psutil?
            try:
                import psutil
                self._psutil = psutil
            except ImportError:
                self.log("No stats, install psutil for stats")
                self._psutil = None

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
                self._slaves = [ None ] * multiprocessing.cpu_count()
            elif self._useMultiprocessing:
                self.log("Using multiprocessing")
            else:
                self.log("Not using multiprocessing - detected non-main thread")

            self.log("Processor started - pid " + str(os.getpid()))

            # Start monitoring our starting pids
            self._monitorCurrentPids()

            # Run the scheduler loop; start with 1 task running at a time
            self._startTaskQueue.put('any')
            lastScheduler = time.time()
            lastMonitor = 0.0
            lastMonitorCheck = 0.0
            self._lastMonitorCheckKill = 0.0
            lastStats = 0.0
            loadMult = self.LOAD_SLEEP_SCALE
            lastCleanup = 0.0
            while True:
                lastScheduler = self._schedulerAudit(lastScheduler)
                lastMonitor = self._monitorAudit(lastMonitor)
                lastMonitorCheck = self._monitorCheckAudit(lastMonitorCheck)
                lastStats = self._statsAudit(lastStats)
                lastCleanup = self._cleanupAudit(lastCleanup)
                try:
                    load = os.getloadavg()[0] / multiprocessing.cpu_count()
                    loadSleep = min(10.0, loadMult * load)
                    if loadSleep > 0:
                        # We need to wait for either a running task to stop
                        # or our timeout to be met
                        self._startTaskQueue.get(timeout=loadSleep)
                except Empty:
                    # Timeout, no task start tokens are available.
                    # Time to run scheduler again.
                    # For now, we can safely assume that this always means that
                    # any tasks running are long-running, and can grab a new 
                    # token.
                    pass

                # We got a token or timed out, OK to start a new task
                loadMult = self.LOAD_SLEEP_SCALE
                try:
                    result = self._consume()
                except _ProcessorStop:
                    raise
                except (Exception, OSError):
                    # _consume has its own error logging; just remove our 
                    # extra sleep bonus and try again
                    continue

                if not result:
                    # Failed to get a new task, avoid using cpu
                    if self.LOAD_SLEEP_SCALE > 0:
                        # Otherwise it's a test, so don't sleep
                        loadMult = self.LOAD_SLEEP_SCALE_NO_TASK
                    if len(self._monitors) == 0:
                        if self._stopOnNoTasks:
                            # This is a test, we're not running anything,
                            # so done
                            break
        except _ProcessorStop:
            self.error("Received _ProcessorStop")
        except Exception:
            self.error("Unhandled error, exiting")
        finally:
            self._lock.release()

    def start(self):
        """Run the Processor asynchronously for test cases.
        """
        self.LOAD_SLEEP_SCALE = 0.0
        self._thread = InterruptableThread(target=self.run)
        self._thread.start()
        '''
        def withProfile(self):
            import cProfile
            p = cProfile.Profile(builtins = False, subcalls = False)
            p.runctx('self.run()', globals(), locals())
            from io import BytesIO
            buffer = BytesIO()
            import pstats
            pr = pstats.Stats(p, stream = buffer)
            pr.sort_stats("cumulative")
            pr.print_stats()
            open('processor.profile', 'w').write(buffer.getvalue())
        self._thread = InterruptableThread(target=withProfile, args=(self,))
        self._thread.start()
        '''

    def stop(self, timeout = 5.0):
        """Halt an asynchronously started processor (only use this for tests!).

        Also kills all tasks.

        timeout -- Max # of seconds to run
        """
        self._stopOnNoTasks = True
        self._thread.join(timeout)
        if self._thread.is_alive():
            self._thread.raiseException(_ProcessorStop)
        while self._thread.is_alive():
            time.sleep(0.1)
        # Now kill our tasks
        pids = [ d[1] for d in self._monitors.itervalues() ]
        for p in pids:
            try:
                os.kill(p, signal.SIGKILL)
            except OSError, e:
                if e.errno == 3:
                    # No such process
                    continue
                raise


    def _cleanupAudit(self, lastCleanup):
        """Schedules a cleanup of the logs directory, returns new scheduled
        time.
        """
        if self._cleanupThread is not None and self._cleanupThread.isAlive():
            return lastCleanup

        now = time.time()
        if now - lastCleanup > self.CLEANUP_INTERVAL:
            self._cleanupThread = threading.Thread(
                target = self._cleanupThreadTarget
            )
            self._cleanupThread.daemon = True
            self._cleanupThread.start()
            return now
        return lastCleanup


    def _cleanupThreadTarget(self):
        """Clean up our logs folder, deleting things older than X.
        """
        now = time.time()
        cutoff = now - self.KEEP_LOGS_FOR
        c = self.taskConnection
        log = None
        try:
            for log in os.listdir(self.getPath(self._LOG_DIR)):
                if log == 'processor.log':
                    # Never remove ourselves
                    continue
                logFile = self.getPath(self._LOG_DIR + log)
                mtime = os.path.getmtime(logFile)
                if mtime <= cutoff:
                    # taskId is just log name without ".log"
                    taskId = log[:-4]
                    task = c.getTask(taskId)
                    if (task is not None 
                            and task['state'] not in c.states.DONE_GROUP):
                        # Task still running, don't clean
                        continue

                    # Clean up the task entry if it exists
                    if task is not None:
                        c._cleanupTask(taskId)
                    # And delete the file
                    os.remove(logFile)
        except:
            self.error("While cleaning up {0}".format(log))


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
        except _ProcessorStop:
            # Stopped!
            raise
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

                # For the monitor, we need a method to poll child processes to
                # see if they have exited (since os.kill() will silently do
                # nothing if they are a zombie process, we need to wait on them
                # to see if they're not a zombie)
                childPoll = None
                with open(self._getPidFile(taskId), 'w') as pidFile:
                    pid = None
                    if self._useThreading:
                        # Run tasks as threads in the Processor's forked 
                        # processing task.
                        slave = self._getSlave()
                        slave.execute(taskData)
                        # We use the process variable to get our pid for 
                        # tracking the new task; the slave process suffices
                        pid = slave.pid
                        childPoll = slave.is_alive
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
                            , kwargs = dict(useRConsole = self._useRConsole)
                        )
                        process.start()
                        pid = process.pid
                        childPoll = process.is_alive
                    else:
                        # Get our task runner script
                        taskRunner = os.path.abspath(os.path.join(
                            __file__
                            , '../../bin/lgTaskRun'
                        ))
                        
                        args = (taskRunner,taskId,self._home)
                        process = subprocess.Popen(args)
                        pid = process.pid
                        childPoll = process.poll

                    # Start was successful, start a PID file and monitor it
                    pidFile.write(str(pid))

                self._monitorPid(taskId, pid, latestStartTime, childPoll)
                return True
            except Exception as e:
                try:
                    os.remove(self._getPidFile(taskId))
                except OSError:
                    # No biggie if we can't clean up, a monitor will clean
                    # it up eventually.
                    pass
                open(self._getLogFile(taskId), 'a').write(
                    'Error on launch: ' + traceback.format_exc()
                )
                self.taskConnection.taskDied(taskId, latestStartTime
                    , 'Error on launch'
                )
                raise

    def _getLogFile(self, tid):
        return self.getPath(self._LOG_DIR + str(tid) + '.log')

    def _getPidFile(self, tid):
        return self.getPath('pids/' + str(tid) + '.pid')

    def _getSlave(self):
        """Returns a fork'd slave process.  Checks on the running condition
        of the slave before delegating."""
        lowest = None
        for i in range(len(self._slaves)):
            s = self._slaves[i]
            if (
                    s is None 
                    or not s.is_alive()
                    or not s.isAccepting()
                ):
                # Allocate new slave
                self._slaves[i] = s = _ProcessorSlave(self)
                s.start()
            tc = s.getTaskCount()
            if lowest is None or tc < lowest[0]:
                lowest = (tc, s)

        return lowest[1]
        
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

        myHost = socket.gethostname()
        tasksAvailable = { 
                'ScheduleAuditTask': ScheduleAuditTask
                }
        try:
            import lgTask.talk
            tasksAvailable['ProcessorInfoTask-' + myHost] = (
                    lgTask.talk.ProcessorInfoTask)
        except ImportError:
            # OK, no talk, can't provide services
            pass
        for name, obj in inspect.getmembers(tasks):
            if inspect.isclass(obj) and issubclass(obj, Task):
                tasksAvailable[name] = obj
        return tasksAvailable

    def _monitorAudit(self, lastTime):
        """See if the database thinks we have any running tasks that don't
        have matching pid files (meaning they've died).
        """
        now = time.time()
        if now - lastTime < 60.0:
            return lastTime

        tasks = self.taskConnection.getWorking(host = True)
        for td in tasks:
            tid = td['_id']
            if tid in self._monitors:
                # Already monitoring
                continue
            if not os.path.exists(self._getPidFile(tid)):
                # No pid file, mark failed
                self.taskConnection.taskDied(tid, td['tsStart'], 'No pid file')

        return now


    def _monitorCheckAudit(self, lastTime):
        """See if the database thinks we have any running tasks that don't
        have matching pid files (meaning they've died).
        """
        now = time.time()
        if now - lastTime < self.MONITOR_CHECK_INTERVAL:
            return lastTime

        toKill = set()
        if now - self._lastMonitorCheckKill >= self.KILL_INTERVAL:
            # See if any of our tasks need to be killed
            tids = self._monitors.keys()
            toKill = set(self.taskConnection.getTasksToKill(tids))
            self._lastMonitorCheckKill = now

        # Copy monitors, since we might remove some as we go along
        lastMonitors = self._monitors.copy()
        for tid, checkArgs in lastMonitors.iteritems():
            shouldKill = (tid in toKill)
            self._monitorPid_check(*checkArgs, shouldKill = shouldKill)

        return now


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
                        self._monitorPid(tid, pid, latestStart, None)
                except Exception, e:
                    self.log("Error on loading task pid {0}: {1}".format(
                        tid, e
                    ))

    def _monitorPid(self, tid, pid, latestStart, childProcessPoll):
        """Monitors the specified task, which is running under the given pid.

        latestStart - See _monitorPid_thread
        """
        t = (tid, pid, latestStart, childProcessPoll)
        self._monitors[tid] = t
        self.log("Monitoring task {0}:{1}".format(tid, pid))
    
    def _monitorPid_check(self, tid, pid, latestStart, 
            childProcessPoll, shouldKill):
        """A function that may be used to poll-monitor the given 
        process.  Runs in the main processor thread periodically to keep the
        thread count down.

        The function takes one argument - shouldKill, which is True if this
        processor is not threaded and the task that this function is responsible
        for should be terminated.

        This thread is responsible for cleaning up the pid file on task
        completion if the pid file is not already cleaned up.

        latestStart - An upper bound on the start time for the process.  Used
            for preventing database corruption on task death.
        childProcessPoll - If set, a parameterless method to poll the child
            process.  This is used to "retire" zombie processes.
        """
        try:
            try:
                if childProcessPoll:
                    childProcessPoll()
                os.kill(pid, 0)
                if self._useThreading:
                    if not os.path.exists(self._getPidFile(tid)):
                        # Task marked as done, don't need to wait
                        # on pid to exit
                        raise OSError
                elif shouldKill:
                    # Send a terminate message, and sleep again
                    os.kill(pid, signal.SIGTERM)

                # If we get here, task is OK.  Just return to skip cleanup
                # code
                return
            except OSError:
                # Task must be dead, skip to else section
                pass
        except Exception, e:
            self.log("Monitor exception for {0}:{1} - {2}".format(
                tid, pid, e
            ))
        else:
            # The only way to reach here is for the process to have died.
            # We want to clean up the pid file and ensure that the database
            # entry is OK.
            try:
                pidFile = self._getPidFile(tid)
                if os.path.exists(pidFile):
                    # Illegal task exit, mark dead
                    self.taskConnection.taskDied(tid, latestStart
                        , 'Illegal task exit'
                    )
                    os.remove(pidFile)

                # Try to add a new process to queue, and log that we're done
                self._monitors.pop(tid, None)
                self._startTaskQueue.put('any')
                self.log("Monitor finished {0}:{1}".format(tid, pid))
            except Exception, e:
                self.log(
                    "Monitor exception in end for {0}:{1} - {2}".format(
                        tid, pid, e.__class__.__name__ + ': ' + str(e)
                    )
                )


    def _schedulerAudit(self, lastTime):
        """Checks if we need to batch up a new scheduler audit task and returns
        the new scheduled time
        """

        now = time.time()
        if lastTime is None or now - lastTime > 120.0:
            self.taskConnection.batchTask('30 minutes', 'ScheduleAuditTask')
            lastTime = now
        return lastTime

    def _statsAudit(self, lastTime):
        """See if we need to log stats, and return new scheduled time.
        """
        if self._psutil is None:
            return lastTime

        now = time.time()
        if lastTime is None or now - lastTime > 180.0:
            stats = self.taskConnection.stats
            cpu = self._psutil.cpu_percent()
            load = os.getloadavg()[0]
            pmem = self._psutil.phymem_usage()
            cmem = self._psutil.cached_phymem()

            memUtil = 100.0 - pmem[3] # pmem[3] is 0-100 % free
            memCache = 100.0 * float(cmem) / pmem[0]

            # Record maximum disk util with and without '/' partition
            diskUseAll = 0.0
            diskUseData = 0.0
            for p in self._psutil.disk_partitions():
                mount = p[1]
                dUse = self._psutil.disk_usage(mount)[3]
                diskUseAll = max(diskUseAll, dUse)
                if mount != '/':
                    diskUseData = max(diskUseData, dUse)

            basePath = [ 'processors', socket.gethostname() ]
            newStats = [
                    dict(path = basePath + [ 'cpu-util-sample' ], value = cpu)
                    , dict(path = basePath + [ 'load-sample' ], value = load)
                    , dict(path = basePath + [ 'mem-util-sample' ]
                        , value = memUtil)
                    , dict(path = basePath + [ 'mem-cached-sample' ]
                        , value = memCache)
                    , dict(path = basePath + [ 'disk-util-sample' ]
                        , value = diskUseAll)
                    , dict(path = basePath + [ 'disk-data-util-sample' ]
                        , value = diskUseData)
                    ]
            stats.addStats(newStats)
            lastTime = now
        return lastTime


class _ProcessorSlave(multiprocessing.Process):

    MAX_TIME = 3600
    MAX_TIME_doc = """Max time, in seconds, to accept tasks."""

    def __init__(self, processor):
        multiprocessing.Process.__init__(self)

        self._processor = processor
        self._connection = Connection(processor.taskConnection)
        self._processorHome = self._processor._home
        self._taskClasses = self._processor._tasksAvailable
        self._processorPid = os.getpid()

        self._queue = multiprocessing.Queue()
        self._running = []
        self._running_doc = """List of running task threads"""
        self._isAccepting = multiprocessing.Value('b', True)
        self._runningCount = multiprocessing.Value('i', 0)
        self._startTime = time.time()
        # No need to check kill right away, after all, we would have just 
        # accepted something
        self._lastKillCheck = self._startTime


    def execute(self, taskData):
        """Queue the task to be ran; this method is ONLY called by the 
        Processor, not the ProcessorSlave.

        It's a little sloppy to increment _runningCount here, but the worst
        case scenario is that we'll overwrite immediately after they update
        their running count (in which case we'll overestimate) or immediately
        before, in which case they'll pick up the queued item from Queue.qsize()
        anyway.
        """
        self._queue.put(taskData)
        self._runningCount.value += 1


    def getTaskCount(self):
        """Returns the number of running tasks from either the Processor or
        the _ProcessorSlave."""
        return self._runningCount.value


    def isAccepting(self):
        """Called by Processor to see if we're still accepting"""
        return self._isAccepting.value


    def run(self):
        # If we can, replace lgTaskProcessor with lgTaskSlave in our title
        try:
            import setproctitle
            title = setproctitle.getproctitle()
            if 'lgTaskProcessor' in title:
                title = title.replace('lgTaskProcessor', 'lgTaskSlave')
            else:
                title += ' --slave'
            setproctitle.setproctitle(title)
        except ImportError:
            pass
        # We're in our own process now, so disconnect the processor's 
        # pymongo connection to make sure we don't hold those sockets open
        self._processor.taskConnection.close()

        # Also, ensure that the global talk variables weren't copied over.
        # This only affects testing situations - that is, the normal processor
        # process won't use talk.
        import lgTask.talk
        lgTask.talk.talkConnection.resetFork()
        
        canQsize = True
        try:
            self._queue.qsize()
        except NotImplementedError:
            # Oh Mac OS X, how silly you are sometimes
            canQsize = False

        self._fixSigTerm()

        # rconsole?
        if self._processor._useRConsole:
            import lgTask.lib.rfooUtil as rfooUtil
            rfooUtil.spawnServer()

        # Any tasks that we start only really need a teeny bit of stack
        thread.stack_size(1024 * 1024)
        try:
            while True:
                try:
                    # See if we should be marked as accepting new tasks from
                    # the Processor
                    if self._isAccepting.value:
                        self._checkAccepting()

                    # Check tasks are running
                    self._checkRunning()

                    # Get new task
                    taskData = self._queue.get(
                        timeout = self._processor.KILL_INTERVAL
                    )
                    taskThread = InterruptableThread(
                        target = self._runTaskThreadMain
                        , args = (taskData,)
                    )
                    # Remember the ID so that we can check for "kill" states
                    taskThread.taskId = taskData['_id']
                    taskThread.start()
                    self._running.append(taskThread)
                    
                    # Update running count
                    newCount = len(self._running)
                    if canQsize:
                        newCount += self._queue.qsize()
                    self._runningCount.value = newCount

                except Empty:
                    pass
                except Exception:
                    self._processor.log("Slave error {0}: {1}".format(
                        self.pid, traceback.format_exc()
                    ))

                # After each iteration, see if we're alive
                if not self._shouldContinue():
                    break
        except:
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
        now = time.time()
        if self._lastKillCheck + self._processor.KILL_INTERVAL <= now:
            self._lastKillCheck = now
            allIds = [ t.taskId for t in self._running ]
            killIds = self._connection.getTasksToKill(allIds)
            for t in self._running:
                if t.taskId in killIds:
                    t.raiseException(KillTaskError)
                    # And prevent us from trying to kill again for a slightly
                    # longer interval
                    self._lastKillCheck = now + self._processor.KILL_TOLERANCE

        for i in reversed(range(len(self._running))):
            t = self._running[i]
            if not t.is_alive():
                self._running.pop(i)


    def _fixSigTerm(self):
        """Register our SIGTERM handler - that is, convert a sigterm on 
        ourselves into a KillTaskError on all of our tasks, and stop accepting
        once we get a sigterm.
        """
        def handleSigTerm(signum, frame):
            for t in self._running:
                t.raiseException(KillTaskError)
            self._isAccepting.value = False
        signal.signal(signal.SIGTERM, handleSigTerm)


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
                , isThread = True
            )
        except Exception:
            self._processor.error('In or after _runTask')


    def _shouldContinue(self):
        """A slave should stop running if it is not currently running any
        tasks and it is no longer accepting new tasks.
        """
        if len(self._running) == 0 and not self._isAccepting.value:
            return False
        return True

