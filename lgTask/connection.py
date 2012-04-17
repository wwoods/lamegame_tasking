
from datetime import datetime, timedelta
import hashlib
import json
import pymongo
import re
import socket
import subprocess
import uuid

from lgTask.errors import *
from lgTask.lib.timeInterval import TimeInterval

def _decodePyMongo(value):
    try:
        v = Connection._decodePyMongo(value)
        return v
    except ValueError:
        pass

def _encodePyMongo(value):
    if isinstance(value, pymongo.database.Collection):
        userCreds = ''
        db = value.database
        return "pymongo://{0}{1}:{2}/{3}/{4}".format(
            userCreds
            , db.connection.host
            , db.connection.port
            , db.name
            , value.name
        )
    elif isinstance(value, pymongo.database.Database):
        db = value
        userCreds = ''
        return "pymongo://{0}{1}:{2}/{3}".format(
                userCreds
                , db.connection.host
                , db.connection.port
                , db.name 
        )
    elif isinstance(value, pymongo.Connection):
        return "pymongo://{0}:{1}".format(value.host, value.port)

class Connection(object):
    """Connects to a task database and performs operations with that database.
    """
    
    TASK_COLLECTION = "lgTask"
    SCHEDULE_COLLECTION = "lgTaskSchedule"

    class states(object):
        """Various states and collections of states."""
        REQUEST = 'request'
        WORKING = 'working'
        SUCCESS = 'success'
        RETRIED = 'retried'
        ERROR = 'error'
        NOT_STARTED_GROUP = [ 'request' ]
        DONE_GROUP = [ 'success', 'retried', 'error' ]
    
    bindingsEncode = [ _encodePyMongo ]
    bindingsDecode = [ _decodePyMongo ]

    def _getDatabase(self):
        return self._database
    database = property(_getDatabase)
    
    def __init__(self, connString, **kwargs):
        if isinstance(connString, pymongo.database.Database):
            self._initPyMongoDb(connString)
        elif connString.startswith("pymongo://"):
            self._initPyMongo(connString)
        else:
            raise ValueError("Unrecognized connString: {0}".format(connString))


    def batchTask(self, runAt, taskClass, priority=0, **kwargs):
        """Runs a task after a period of time; does not re-add this task
        if a task already exists.  Uses either taskName from kwargs or a
        combination of taskClass and kwargs to distinguish batches.

        If the task is already scheduled and runAt comes before it was 
        previously scheduled to run, its schedule will be moved up to
        the new runAt time.

        Batched tasks will only run at most one instance at a time.

        Raises TaskKwargError if a scheduled batch task already exists, but
        the kwargs are different.
        """

        now = datetime.utcnow()
        runAt = self._getRunAtTime(now, runAt)
        originalKwargs = kwargs
        kwargs = self._kwargsEncode(originalKwargs)

        taskName = self._getTaskName(taskClass, kwargs)
        taskColl = self._database[self.TASK_COLLECTION]

        existing = taskColl.find_one(
            { '_id': taskName }
            , { 
              'batchQueued': 1
              , 'kwargs': 1
              , 'state': 1
              , 'tsRequest': 1 
              , 'priority': 1
            }
        )

        if existing:
            # Double check kwarg match or we have another problem
            if kwargs != existing.get('kwargs') \
                or priority < existing.get('priority', 0):
                raise TaskKwargError("Didn't match existing scheduled "
                    + "batch - {0} -- {1}".format(taskName, existing))

        if existing and existing['state'] in self.states.NOT_STARTED_GROUP:
            # Already an existing task; update the request time or priority
            # if we need to.
            oldRunAt = existing['tsRequest']
            oldPriority = existing.get('priority', 0)
            if runAt < oldRunAt or priority > oldPriority:
                # Even if this fails, the task will still run at a point after
                # when we were requested, so we take don't need to do
                # safe=True
                runAtToSet = min(runAt, oldRunAt)
                priorityToSet = max(priority, oldPriority)
                taskColl.update(
                    { '_id': taskName, 'tsRequest': oldRunAt }
                    , { '$set': { 
                        'tsRequest': runAtToSet
                        , 'priority': priorityToSet
                    } }
                )
        elif existing and existing['state'] in self.states.DONE_GROUP:
            # Already existing, but finished.  This is an error state, since
            # batch tasks should change their ID on completion.  However, we
            # want to handle this gracefully, so overwrite it with a requested
            # task.
            r = taskColl.find_and_modify(
                { '_id': taskName, 'state': existing['state'] }
                , { '$set': {
                    'state': 'request'
                    , 'tsRequest': runAt
                    , 'priority': priority
                }}
                , { '_id': 1 }
            )

            if r is None:
                # New state?  Re-evaluate to see what's up.  Should be 
                # very infrequent.
                return self.batchTask(
                    runAt, taskClass
                    , priority=priority, **originalKwargs
                )

        elif existing:
            # Existing task that is running
            oldRunAt = (existing.get('batchQueued') or datetime.max)
            oldPriority = existing.get('priority', 0)
            runAtToSet = min(runAt, oldRunAt)
            priorityToSet = min(priority, oldPriority)
            if runAtToSet < oldRunAt or priorityToSet > oldPriority:
                # Tell it to reschedule when it is finished.
                r = taskColl.find_and_modify(
                    { '_id': taskName }
                    , { '$set': { 
                        'batchQueued': runAtToSet
                        , 'priority': priorityToSet
                    } }
                )

                if r is None:
                    # Couldn't set queuing flag, need to retry whole
                    # procedure
                    return self.batchTask(
                        runAt, taskClass
                        , priority=priority, **originalKwargs
                    )

        if not existing:
            # Not existing or failed to queue; insert
            taskArgs = { 
                '_id': taskName
                , 'tsRequest': runAt 
                , 'batch': True
                , 'priority': priority
            }
            self._createTask(now, taskClass, taskArgs, kwargs)

        
    def createTask(self, taskClass, priority=0, **kwargs):
        """Creates a task to be executed immediately.
        """
        now = datetime.utcnow()
        kwargs = self._kwargsEncode(kwargs)
        self._createTask(now, taskClass, { 'priority': priority }, kwargs)

    def delayedTask(self, runAt, taskClass, priority=0, **kwargs):
        """Creates a task that runs after a given period of time, or at 
        a specific UTC datetime.
        """

        now = datetime.utcnow()
        runAt = self._getRunAtTime(now, runAt)

        taskArgs = {
          'tsRequest': runAt
          , 'priority': priority
        }
        kwargs = self._kwargsEncode(kwargs)
        self._createTask(now, taskClass, taskArgs, kwargs)

    def getNewId(self):
        return uuid.uuid4().hex
        
    def intervalTask(self, interval, taskClass, fromStart=False
        , priority=0, **kwargs):
        """Schedule (or assert that a schedule exists for) a task to be
        executed at the given interval.

        Raises a TaskKwargError if there is already a scheduler for this
        taskName (either user specified or combination of taskClass and kwargs)

        interval -- String or timedelta - The period of time to elapse between
        the end of one execution and the beginning of the next.  Pass the kwarg
        fromStart as True to start from the beginning of each execution.

        taskClass -- String - Name of the class to execute.

        fromStart -- Boolean - If set to True, causes this task to be 
        rescheduled when work starts rather than when work ends.  New intervals
        will not be allowed to run until a previous interval has finished, 
        however.  In other words, the fastest an intervalTask will be 
        scheduled is as fast as it can be executed.
        """

        now = datetime.utcnow()
        ti = TimeInterval(interval)
        # This is tacky, but we are going to integrate a test for TimeInterval
        # here.  It would be very bad if this conversion ever failed.  This
        # should be removed at some point, but it doubtlessly takes a very
        # small amount of time to run.
        if ti != TimeInterval(str(ti)):
            raise Exception("TimeInterval identity failed for " + str(interval))

        kwargs = self._kwargsEncode(kwargs)
        schedule = { 'interval': { 
            'every': str(ti)
            , 'fromStart': fromStart 
        }}

        taskName = self._getTaskName(taskClass, kwargs)

        schedDb = self._database[self.SCHEDULE_COLLECTION]
        old = schedDb.find_one({ '_id': taskName })
        if old:
            if \
                taskClass != old['taskClass'] \
                or kwargs != old['kwargs'] \
                or schedule != old['schedule'] \
                or priority != old.get('priority', 0) \
                :
                raise TaskKwargError(
                    ("Scheduled task with name {0} already "
                        + "exists with different parameters"
                    ).format(taskName)
                )
            return

        try:
            # The scheduler db must have enough information to re-launch the
            # task.
            schedDb.insert(
                { 
                    '_id': taskName
                    , 'kwargs': kwargs
                    , 'schedule': schedule
                    , 'taskClass': taskClass
                    , 'priority': priority
                }
                , safe=True
            )
        except pymongo.errors.DuplicateKeyError:
            # It was inserted in our midst; run again
            return self.intervalTask(interval, taskClass, fromStart=fromStart
                , **kwargs
            )

        # Create our task, which will perpetually be triggered on our 
        # schedule.
        taskArgs = { 
            '_id': taskName
            , 'schedule': True
            , 'priority': priority
        }
        self._createTask(now, taskClass, taskArgs, kwargs)

    def _startTask(self, availableTasks):
        """Gets a task from our database and marks it in the database as 
        started on this machine.
       
        Returns the taskData to be launched on success.  Returns None if no
        suitable task was found.
        
        availableTasks: a dict of (ClassName, ClassObj) pairs that determine
            what tasks may be started by this slave.
        """
        c = self._database[self.TASK_COLLECTION]
        now = datetime.utcnow()
        taskData = c.find_and_modify(
            {
                'taskClass': { '$in': availableTasks.keys() }
                , 'state': 'request'
                , 'tsRequest': { '$lte': now }
            }
            , { '$set': {
                'state': 'working'
                , 'tsStart': now
                , 'host': socket.gethostname()
            }}
            , new = True
            , sort = [ ('priority', -1), ('tsRequest', 1) ]
        )

        return taskData

    def taskDied(self, taskId, startedBefore):
        """Called when a task's process is detected as dead.  Updates the 
        database unless the status is success or error.

        The task must be running on the machine this is called on to have
        any effect.

        startedBefore - Since batched tasks can be re-inserted into the 
            database and potentially restarted before this function is called,
            we need to use a known upper bound on start time to prevent
            data corruption.
        """
        c = self._database[self.TASK_COLLECTION]
        now = datetime.utcnow()
        c.update(
            { 
                '_id': taskId
                , 'state': self.states.WORKING
                , 'tsStart': { '$lte': startedBefore }
            }
            , { '$set': { 
                'state': 'error'
                , 'lastLog': 'Task died - see log' 
                , 'tsStop': now
            } }
        )
        
    def taskStopped(self, task, success, lastLog, moveIdCallback):
        """Update the database noting that a task has stopped.  Must be callable
        multiple times; see Task._finished.
        
        task - the Task being stopped
        
        success - True for success, False for error.  May also be an instance
            of RetryTaskError for a retry.
        
        lastLog - The last log message.

        moveIdCallback - called when a batch task gets its id moved with the
            new ID as an argument.  Used to keep logs associated with ids.
        """
        c = self._database[self.TASK_COLLECTION]
        taskId = task.taskId

        now = datetime.utcnow()
        finishUpdates = {
            'tsStop': now
            , 'lastLog': lastLog
        }
        if isinstance(success, RetryTaskError):
            try:
                self._retryTask(task, success.delay)
                finishUpdates['state'] = self.states.RETRIED
            except Exception, e:
                # We still want to mark end state, etc.
                finishUpdates['state'] = 'error'
                finishUpdates['lastLog'] = 'Tried to retry; got exception: {0}'\
                    .format(e)
        else:
            finishUpdates['state'] = 'success' if success else 'error'

        if \
            task is not None \
            and (
                task.taskData.get('batch') \
                or task.taskData.get('schedule')
            ):
            # We need to change the identifier to make way for a new 
            # batch or interval task.  This isa destructive and we want it
            # to succeed always, so we set it up in a try loop to deal with 
            # StopTaskError
            taskData = [ None ]
            reinserted = [ False ]
            deletedData = [ None ]
            queueChecked = [ False ]
            rescheduled = [ False ]
            def tryFixTask():
                if taskData[0] is None:
                    taskData[0] = c.find_one({ '_id': taskId })
                if not reinserted[0]:
                    taskData[0]['_id'] = self.getNewId()
                    taskData[0].update(finishUpdates)
                    reinsertedOk = c.insert(taskData[0], safe=True)
                    moveIdCallback(taskData[0]['_id'])
                    reinserted[0] = reinsertedOk
                if not deletedData[0]:
                    # In case anything (specifically batchQueued) changed
                    # while we were copying the record, we want to atomically
                    # re-fetch data.
                    deletedData[0] = c.find_and_modify(
                        { '_id': taskId }
                        , remove=True
                    )
                    if deletedData[0] is None:
                        deletedData[0] = taskData[0]
                if not queueChecked[0] and deletedData[0].get('batchQueued'):
                    taskArgs = { 
                        '_id': taskId
                        , 'batch': True 
                        , 'tsRequest': deletedData[0]['batchQueued']
                        , 'priority': deletedData[0].get('priority', 0)
                    }
                    # Even if this gets run twice, it's fine since we 
                    # have a specific ID; only one task will be created.
                    self._createTask(now, deletedData[0]['taskClass'], taskArgs
                        , task._kwargsOriginal
                    )
                    queueChecked[0] = True
                if not rescheduled[0] and deletedData[0].get('schedule'):
                    nextTime = self._scheduleGetNextRunTime(
                        taskId
                        , deletedData[0]['tsStart']
                        , now
                    )
                    if nextTime is not None:
                        taskArgs = {
                            '_id': taskId
                            , 'schedule': True
                            , 'tsRequest': nextTime
                            , 'priority': deletedData[0].get('priority', 0)
                        }
                        # Same as with batchQueued; OK to run twice since we use
                        # _id.
                        self._createTask(
                            now
                            , deletedData[0]['taskClass']
                            , taskArgs
                            , task._kwargsOriginal
                        )
                    rescheduled[0] = True


            # A while loop would necessarily leave gaps in the try..except
            # block.  So for 100% coverage, we do it like this.
            try:
                try:
                    tryFixTask()
                except:
                    tryFixTask()
            except:
                tryFixTask()
        else:
            #Standard set-as-finished update; safe to be called repeatedly.
            c.update(
                { '_id': taskId }
                , { '$set': finishUpdates }
            )

    def _createTask(self, utcNow, taskClass, taskArgs, kwargsEncoded):
        """Creates a new task entry with some basic default task args that
        are updated with taskArgs, and kwargs.
        """
        taskDefaultArgs = {
            'tsInsert': utcNow
            ,'tsRequest': utcNow
            ,'tsStart': None
            ,'tsStop': None
            ,'state': 'request'
            ,'priority': 0
            ,'taskClass': taskClass
            ,'kwargs': kwargsEncoded
        }

        taskDefaultArgs.update(taskArgs)

        if '_id' not in taskDefaultArgs:
            taskDefaultArgs['_id'] = self.getNewId()

        taskColl = self._database[self.TASK_COLLECTION]
        taskColl.insert(taskDefaultArgs)
                
    @classmethod
    def _decodePyMongo(cls, connString):
        """Decodes a pymongo connection string into either a Connection or 
        Database object.
        """
        if not isinstance(connString, basestring):
            return

        m = re.match("^pymongo://([^:@]+(:[^@]+)?@)?([^/]+)(:[^/]+)?(/[^/]+)?(/[^/]+)?$"
                , connString)
        if m is None:
            raise ValueError("connString did not match format: {0}".format(
                    "pymongo://[user:password@]host[:port]/db"))
        user, password, host, port, db, coll = m.groups()
        
        c = pymongo.Connection(host=host, port=port)
        if db is not None:
            db = db[1:] # Trim off leading slash
            c = c[db]
            if user is not None:
                c.authenticate(user, password)
                
            if coll is not None:
                coll = coll[1:] # Trim leading slash
                c = c[coll]
                
        return c

    def _ensureIndexes(self):
        """Assert that all necessary indexes exist in the tasks collections 
        to maintain performance.  Typically called by a Processor.
        """
        db = self._database[self.TASK_COLLECTION]
        try:
            # Get rid of old index
            db.drop_index([ ('state', 1), ('taskClass', 1), ('tsRequest', -1) ])
        except pymongo.errors.OperationFailure:
            pass
        try:
            db.drop_index([ ('state', 1), ('priority', -1), ('tsRequest', -1) ])
        except pymongo.errors.OperationFailure:
            pass

        db.ensure_index([ ('state', 1), ('priority', -1), ('tsRequest', 1) ])

    def _getRunAtTime(self, utcNow, runAt):
        """Changes a runAt variable that might be an interval or datetime,
        and returns a utc datetime.
        """
        now = utcNow
        if isinstance(runAt, datetime):
            pass #Leave as a datetime
        elif isinstance(runAt, timedelta):
            runAt = now + runAt
        elif isinstance(runAt, basestring):
            runAt = now + TimeInterval(runAt)
        else:
            raise ValueError("runAt must be datetime, timedelta, or str")

        return runAt

    def _getTaskName(self, taskClass, kwargs):
        """Gets the fully qualified task name for a given task class and 
        kwargs.

        Modifies the dict kwargs in-place by removing the taskName kwarg.
        """
        className = taskClass
        suffix = kwargs.pop('taskName', None)
        if suffix:
            className += '-' + suffix
        elif kwargs == {}:
            # Ok, don't add any suffix.
            pass
        else:
            m = hashlib.sha256()
            m.update(repr(kwargs))
            className += '-' + m.hexdigest()
        return className

    def _initPyMongo(self, connString):
        """Open up a connection to the given pymongo resource.
        
        Example: pymongo://user:password@localhost:5555/testDb
        """
        cDb = self._decodePyMongo(connString)
        if isinstance(cDb, pymongo.Connection):
            raise ValueError("Must specify database name: {0}".format(
                    connString))
        elif isinstance(cDb, pymongo.database.Database):
            self._connection = cDb.connection
            self._database = cDb
        else:
            raise ValueError("Failed to parse: {0}".format(connString))
        
    def _initPyMongoDb(self, db):
        self._connection = db.connection
        self._database = db

    @classmethod
    def _kwargsDecode(cls, kwargs):
        """Decode kwargs from database (json) objects to python objects.
        """
        kwargsDecoded = {}
        for key,value in kwargs.items():
            # Py2.X requires string keys for dicts
            key = str(key)
            newValue = value
            for binding in cls.bindingsDecode:
                decodedValue = binding(value)
                if decodedValue is not None:
                    newValue = decodedValue
            kwargsDecoded[key] = newValue
        return kwargsDecoded

    @classmethod
    def _kwargsEncode(cls, kwargs):
        """Encode kwargs from python objects to database (json) objects
        """
        kwargsEncoded = {}
        for key,value in kwargs.items():
            newValue = value
            for binding in cls.bindingsEncode:
                encodedValue = binding(value)
                if encodedValue is not None:
                    newValue = encodedValue
                    break
            kwargsEncoded[key] = newValue
        return kwargsEncoded

    def _retryTask(self, task, delay):
        """Queue up an identical version of task after delay time.
        """
        now = datetime.utcnow()
        runAt = self._getRunAtTime(now, delay)

        taskArgs = {
          'tsRequest': runAt
          , 'retry': task.taskData.get('retry', 0) + 1
          , 'priority': task.taskData.get('priority', 0)
        }
        taskClass = task.taskData['taskClass']
        kwargs = task._kwargsOriginal
        self._createTask(now, taskClass, taskArgs, kwargs)

    def _scheduleGetNextRunTime(self, scheduleId, taskStart, taskStop):
        """Given the provided scheduler identifier, task working start time,
        and task working stop time, produce a datetime representing when
        the next iteration of this scheduled task should execute.

        taskStop time is always assumed to be == utcnow()
        """
        schedDb = self._database[self.SCHEDULE_COLLECTION]
        schedule = schedDb.find_one({ '_id': scheduleId }, { 'schedule': 1 })
        if not schedule:
            return None

        result = None
        schedule = schedule['schedule']
        if schedule['interval']:
            interval = schedule['interval']
            result = taskStop
            if interval['fromStart']:
                result = taskStart
            result += TimeInterval(interval['every'])
        else:
            raise ValueError("No appropriate scheduling method found")
            
        if result is not None:
            if taskStop > result:
                result = taskStop
        return result

    def _scheduleRestartTask(self, scheduleId):
        """Called by scheduleAuditor when the task indicated by scheduleId
        does not appear to be scheduled.  Since we don't know how long the
        task has been inactive, it is scheduled immediately.  
        """
        schedDb = self._database[self.SCHEDULE_COLLECTION]
        schedule = schedDb.find_one({ '_id': scheduleId })
        if not schedule:
            return

        now = datetime.utcnow()
        # Since we don't know if it's been inactive for a long time, schedule
        # now.
        nextTime = now
        # nextTime = self._scheduleGetNextRunTime(scheduleId, now, now)

        taskClass = schedule['taskClass']
        kwargs = schedule['kwargs']
        taskArgs = {
            '_id': scheduleId
            , 'schedule': True
            , 'tsRequest': nextTime
            , 'priority': schedule.get('priority', 0)
        }
        self._createTask(now, taskClass, taskArgs, kwargs)


