
import datetime
import traceback
from lgTask.errors import RetryTaskError

def _runTask(taskClass, taskData, taskConnection, processorHome):
    taskId = taskData['_id']
    logFile = processorHome + '/logs/' + str(taskId) + '.log'

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
            conn.taskStopped(task, success, lastLogMessage[0])


