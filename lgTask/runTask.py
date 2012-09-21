
import datetime
import time
import traceback
import os
from lgTask.errors import RetryTaskError

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
    , setProcTitle = True
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

    try:
        conn = taskConnection
        task = taskClass(conn, taskId, taskData)

        # Convert kwargs
        kwargsOriginal = taskData['kwargs']
        task._kwargsOriginal = kwargsOriginal
        kwargs = conn._kwargsDecode(kwargsOriginal)
        task.kwargs = kwargs
    except Exception, e:
        log("Exception during init: " + traceback.format_exc())
    else:
        success = True
        try:
            task.log = log
            if setProcTitle:
                setProcessTitle(task, processorHome)

            # Wait for the Processor to fill out our pid file; this ensures 
            # that if the processor crashes after setting our state to working
            # and after launching us, then it can justifiably claim that we
            # have died when it does not find the pid file.
            waitFor = 5 #seconds
            sleepTime = 0.5
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
        except Exception, e:
            log("Unhandled exception: " + traceback.format_exc())
            success = False
        finally:
            def moveLogFile(newId):
                os.rename(logFile, getLogForId(newId))
            conn.taskStopped(task, success, lastLogMessage[0], moveLogFile)


