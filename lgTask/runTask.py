
import datetime
import traceback
import os
from lgTask.errors import RetryTaskError

try:
    # Use setproctitle if available to give us a task ID
    from setproctitle import getproctitle, setproctitle
    def setProcessTitle(task):
        """Set the process title so that this process is indentifiable."""
        newTitle = getproctitle() + " task-" + str(task.taskId)
        setproctitle(newTitle)
        task.log("Set process title - " + newTitle)
except ImportError:
    def setProcessTitle(task):
        task.log("Could not set process title - easy_install setproctitle")


def _runTask(taskClass, taskData, taskConnection, processorHome):
    def getLogForId(id):
        return processorHome + '/logs/' + str(id) + '.log'

    taskId = taskData['_id']
    logFile = getLogForId(taskId)

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
            setProcessTitle(task)
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


