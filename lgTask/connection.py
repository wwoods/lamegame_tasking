
from datetime import datetime, timedelta
import pymongo
import re

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
    
    TASK_COLLECTION = "task"
    SCHEDULE_COLLECTION = "schedule"
    SINGLETON_COLLECTION = "taskSingle"

    class states(object):
        """Various states and collections of states."""
        REQUEST = 'request'
        WORKING = 'working'
        SUCCESS = 'success'
        ERROR = 'error'
        NOT_STARTED_GROUP = [ 'request' ]
        DONE_GROUP = [ 'success', 'error' ]
    
    bindingsEncode = [ _encodePyMongo ]
    bindingsDecode = [ _decodePyMongo ]
    
    def __init__(self, connString, **kwargs):
        if isinstance(connString, pymongo.database.Database):
            self._initPyMongoDb(connString)
        elif connString.startswith("pymongo://"):
            self._initPyMongo(connString)
        else:
            raise ValueError("Unrecognized connString: {0}".format(connString))


    def batchTask(self, runAt, taskClass, **kwargs):
        """Runs a task after a period of time; does not re-add this task
        if a task already exists.  Uses the taskName kwarg to distinguish
        batches.

        If the task is already scheduled and runAt comes before it was 
        previously scheduled to run, its schedule will be moved up to
        the new runAt time.

        Batched tasks have all of the properties of singleton tasks; the 
        difference is that batched tasks must run inside the task processing
        framework, and not as independent entities.

        Raises TaskKwargError if a scheduled batch task already exists, but
        the kwargs are different.
        """

        now = datetime.utcnow()
        runAt = self._getRunAtTime(now, runAt)
        kwargs = self._kwargsEncode(kwargs)

        taskName = self._getTaskName(taskClass, kwargs)
        taskColl = self._database[self.TASK_COLLECTION]

        existing = taskColl.find_one(
            { '_id': taskName }
            , { 'batchQueued': 1, 'kwargs': 1, 'state': 1, 'tsRequest': 1 }
        )

        if existing:
            # Double check kwarg match or we have another problem
            if kwargs != existing['kwargs']:
                raise TaskKwargError("Didn't match existing scheduled batch")

        if existing and existing['state'] in self.states.NOT_STARTED_GROUP:
            # Already an existing task; update the request time if we need to
            oldRunAt = existing['tsRequest']
            if runAt < oldRunAt:
                # Even if this fails, the task will still run at a point after
                # when we were requested, so we take don't need to do
                # safe=True
                taskColl.update(
                    { '_id': taskName, 'tsRequest': oldRunAt }
                    , { '$set': { 'tsRequest': runAt } }
                )
        elif existing and runAt < (existing.get('batchQueued') or datetime.max):
            # Already an existing task, but it's running; tell it to 
            # reschedule when finished.
            print("SETTING batchQueued TO " + str(runAt))
            r = taskColl.find_and_modify(
                { '_id': taskName }
                , { '$set': { 'batchQueued': runAt } }
            )

            if r is None:
                # We want to add the task; couldn't set queuing flag
                print ("SETTING failed, adding new")
                existing = False

        if not existing:
            # Not existing or failed to queue; insert
            taskArgs = { 
                '_id': taskName
                , 'tsRequest': runAt 
                , 'batch': True
            }
            self._createTask(now, taskClass, taskArgs, kwargs)

        
    def createTask(self, taskClass, **kwargs):
        """Creates a task to be executed immediately.
        """
        now = datetime.utcnow()
        kwargs = self._kwargsEncode(kwargs)
        self._createTask(now, taskClass, {}, kwargs)

    def delayedTask(self, runAt, taskClass, **kwargs):
        """Creates a task that runs after a given period of time, or at 
        a specific UTC datetime.
        """

        now = datetime.utcnow()
        runAt = self._getRunAtTime(now, runAt)

        taskArgs = {
          'tsRequest': runAt
        }
        kwargs = self._kwargsEncode(kwargs)
        self._createTask(now, taskClass, taskArgs, kwargs)
        
    def ensureIndexes(self):
        """Assert that all necessary indexes exist in the tasks collections 
        to maintain performance.  Typically called by a Processor.
        """
        db = self._database[self.TASK_COLLECTION]
        db.ensure_index( [ ('state', 1), ( 'taskClass', 1), ( 'tsRequest', -1 ) ] )

    def intervalTask(self, interval, taskClass, fromStart=False, **kwargs):
        """Schedule (or assert that a schedule exists for) a task to be
        executed at the given interval.

        Raises a TaskKwargError if there is already a scheduler for this
        taskClass and taskName.  

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
            if kwargs != old['kwargs'] or schedule != old['schedule']:
                raise TaskKwargError(
                    ("Scheduled task with name {0} already "
                        + "exists."
                    ).format(taskName)
                )
            return

        try:
            schedDb.insert(
                { '_id': taskName
                    , 'kwargs': kwargs
                    , 'schedule': schedule
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
        }
        self._createTask(now, taskClass, taskArgs, kwargs)

    def singletonAcquire(self, singleton):
        """Acquire the singleton running permissions for taskName or 
        raise a SingletonAlreadyRunningError exception.
        
        Returns the time that heartbeat is set to when we have acquired the 
        lock
        """
        taskName = singleton.taskName
        # getattr instead of direct; if this singleton is not spawned through
        # a processor, it will not have _kwargsOriginal.
        taskKwargs = getattr(singleton, '_kwargsOriginal', {})
        heartbeat = singleton.HEARTBEAT_INTERVAL
        c = self._database[self.SINGLETON_COLLECTION]
        # Check if already running, cautious upsert if not.
        now = datetime.utcnow()
        maxHeartbeat = now - heartbeat * 2
        current = c.find_one({ '_id': taskName })
        if current is None:
            # None of this singleton have ever run before... try inserting a
            # record.
            try: 
                c.insert(
                    { '_id': taskName, 'heartbeat': now, 'kwargs': taskKwargs }
                    , safe=True
                )
            except pymongo.errors.DuplicateKeyError:
                raise SingletonAlreadyRunningError()
        else:
            if current.get('heartbeat', datetime.min) >= maxHeartbeat:
                # Already running; a SingletonAlreadyRunningError should be
                # raised, but for added debugging support, we'll make sure
                # that our kwargs match theirs
                ck = current['kwargs']
                if ck != taskKwargs:
                    raise TaskKwargError("{0} != {1}".format(ck, taskKwargs))
                raise SingletonAlreadyRunningError()
            r = c.find_and_modify(
                {
                    '_id': taskName
                    , 'heartbeat': current.get('heartbeat', None) 
                }
                , {
                    '_id': taskName
                    , 'heartbeat': now
                }
            )
            if r is None:
                raise SingletonAlreadyRunningError()
            
        # Return our heartbeat
        return now
            
    def singletonHeartbeat(self, taskName, expectedLast):
        """Register a heartbeat for the given singleton.  Raises a 
        SingletonAlreadyRunningError error if the last heartbeat does not match the
        expected heartbeat; this indicates that, at some point, another version
        of our singleton was started, and we should abort.
        
        Returns the new heartbeat value if set OK.
        """
        c = self._database[self.SINGLETON_COLLECTION]
        now = datetime.utcnow()
        result = c.find_and_modify(
            { '_id': taskName, 'heartbeat': expectedLast }
            , { '$set': { 'heartbeat': now } }
        )
        if result is None:
            # NOTE - In tests, this can also happen when you forget to stop()
            # a processor!
            raise SingletonAlreadyRunningError()
        return now
        
    def singletonRelease(self, taskName, lastHeartbeat):
        """Assuming we have the singleton for taskName, release it.
        """
        # We already have the lock, so just releasing our heartbeat should
        # be fine.
        c = self._database[self.SINGLETON_COLLECTION]
        c.find_and_modify(
            { '_id': taskName, 'heartbeat': lastHeartbeat }
            , { '$unset': { 'heartbeat': 1 } }
        )
        
    def startTask(self, processorName, availableTasks):
        """Gets a task from our database and calls its start() method.
        
        Returns either the new Task object or None if no suitable task was
        found.  
        
        Exceptions are forwarded to the caller.

        processorName -- String - The full name of the processor.
        
        availableTasks: a dict of (ClassName, ClassObj) pairs that determines
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
                , 'processor': processorName
            }}
            , new = True
        )
        
        if taskData is None:
            return None
        
        taskId = taskData['_id']
        try:
            # This isn't the full task name; we let the task class handle
            # adding the class prefix.
            taskNameSuffix = taskData['kwargs'].get('taskName', None)
            task = availableTasks[taskData['taskClass']](
                    taskConnection=self
                    ,taskName=taskNameSuffix
                    ,taskData=taskData
                    ,taskId=taskId
                )
            # Since py2.X requires kwargs keys to be str types and not unicode,
            # we have to convert kwargs
            kwargsOriginal = taskData['kwargs']
            task._kwargsOriginal = kwargsOriginal
            kwargs = {}
            for key,value in kwargsOriginal.items():
                key = str(key)
                if key == 'taskName':
                    # Never put taskName into the decoded kwargs
                    continue
                kwargs[key] = value
                for binding in self.bindingsDecode:
                    newValue = binding(value)
                    if newValue is not None:
                        kwargs[key] = newValue
                        break
            task.kwargs = kwargs
            task.start(**kwargs)
            return task
        except SingletonAlreadyRunningError:
            # Maybe should just delete the task... 
            self.taskStopped(taskId, True, [ 'SingletonTask already running' ])
            raise
        except Exception as e:
            self.taskStopped(taskId, False, [ 'Error starting task: ' + str(e) ])
            raise
        
    def taskStopped(self, taskOrId, success, logs):
        """Update the database noting that a task has stopped.  Must be callable
        multiple times; see Task._finished.
        
        taskOrId - the task or ID being stopped
        
        success - True for success, False for error
        
        logs - array of log entries.
        """
        c = self._database[self.TASK_COLLECTION]
        if not isinstance(taskOrId, basestring):
            task = taskOrId
            taskId = task.taskId
        else:
            task = None
            taskId = taskOrId

        now = datetime.utcnow()
        finishUpdates = {
            'state': 'success' if success else 'error'
            , 'tsStop': now
            , 'lastLog': logs[-1] if len(logs) > 0 \
                else '(No log entries)'
        }

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
                    try:
                        del taskData[0]['_id']
                    except KeyError:
                        pass
                    taskData[0].update(finishUpdates)
                    reinserted[0] = c.insert(taskData[0], safe=True)
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
            ,'taskClass': taskClass
            ,'kwargs': kwargsEncoded
        }

        taskDefaultArgs.update(taskArgs)
            
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
        """
        className = taskClass
        suffix = kwargs.get('taskName', None)
        if suffix:
            className += suffix
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

    def _kwargsEncode(self, kwargs):
        """Encode kwargs from python objects to database objects
        """
        kwargsEncoded = {}
        for key,value in kwargs.items():
            newValue = value
            for binding in self.bindingsEncode:
                decodedValue = binding(value)
                if decodedValue is not None:
                    newValue = decodedValue
                    break
            kwargsEncoded[key] = newValue
        return kwargsEncoded

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

        now = taskStop # See assumption in docstring.
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
            if now > result:
                result = now
        return result
            
