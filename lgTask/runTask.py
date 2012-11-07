
import datetime
import signal
import threading
import time
import traceback
import os
from lgTask.errors import KillTaskError, RetryTaskError

try:
    # Use setproctitle if available to give us a task ID
    from setproctitle import getproctitle, setproctitle
    def setProcessTitle(task, processorHome):
        """Set the process title so that this process is indentifiable."""
        newTitle = 'lgTask ' + task.__class__.__name__ + ' '
        newTitle += processorHome + ' '
        newTitle += str(task.taskId)
        setproctitle(newTitle)
        task.log("Set process title - " + newTitle)
except ImportError:
    def setProcessTitle(task, processorHome):
        task.log("Could not set process title - easy_install setproctitle")


def _runTask(
    taskClass
    , taskData
    , taskConnection
    , processorHome
    , isThread = False
    , useRConsole = False
    ):
    def getLogForId(id):
        return processorHome + '/logs/' + str(id) + '.log'
    def getPidForId(id):
        return processorHome + '/pids/' + str(id) + '.pid'

    taskId = taskData['_id']
    logFile = getLogForId(taskId)
    pidFileName = getPidForId(taskId)

    lastLogMessage = [ "(No log)" ]
    def log(message):
        now = datetime.datetime.utcnow().isoformat() + 'Z'
        lastLogMessage[0] = message

        showAs = now + ' ' + message
        print(showAs)
        with open(logFile, 'a') as f:
            f.write(showAs + '\n')

    # If anything goes wrong, default state is error
    success = False
    try:
        # Set our python thread name
        threading.current_thread().setName('lgTask-' + str(taskId))

        # Launch rconsole?
        if useRConsole:
            import lgTask.lib.rfooUtil as rfooUtil
            rfooUtil.spawnServer()

        if not isThread:
            # If we're not a thread, then we should change the process
            # title to make ourselves identifiable, and also register
            # a signal handler to convert SIGTERM into KillTaskError.
            def onSigTerm(signum, frame):
                raise KillTaskError()
            signal.signal(signal.SIGTERM, onSigTerm)

        conn = taskConnection
        task = taskClass(conn, taskId, taskData)
        if not isThread:
            setProcessTitle(task, processorHome)

        # Convert kwargs
        kwargsOriginal = taskData['kwargs']
        task._kwargsOriginal = kwargsOriginal
        kwargs = conn._kwargsDecode(kwargsOriginal)
        task.kwargs = kwargs
    except:
        log("Exception during init: " + traceback.format_exc())
    else:
        try:
            task.log = log
            # We tell the task where its log file is only so that fetchLogTask
            # doesn't need to be smart, and can just find other tasks running
            # on the same machine by simply changing the taskId in the
            # filename.
            task._lgTask_logFile = logFile

            # Wait for the Processor to fill out our pid file; this ensures 
            # that if the processor crashes after setting our state to working
            # and after launching us, then it can justifiably claim that we
            # have died when it does not find the pid file.
            waitFor = 5 #seconds
            sleepTime = 0.1
            while True:
                with open(pidFileName, 'r') as f:
                    if f.read() == str(os.getpid()):
                        break
                waitFor -= sleepTime
                if waitFor <= 0:
                    raise Exception("Pid file never initialized")
                time.sleep(sleepTime)

            if task.DEBUG_TIMING:
                log("Starting task at {0}".format(
                    datetime.datetime.utcnow().isoformat()
                ))

            success = task.run(**kwargs)
            if success is None:
                # No return value means success
                success = True
        except RetryTaskError, e:
            log("Retrying task after {0}".format(e.delay))
            success = e
        except:
            log("Unhandled exception: " + traceback.format_exc())
            success = False
    finally:
        conn.taskStopped(taskId, taskData, success, lastLogMessage[0])
        # If taskStopped ran successfully, then we have finished execution
        # properly and should remove our pid file
        os.remove(pidFileName)


