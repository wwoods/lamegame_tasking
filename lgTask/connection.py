
from datetime import datetime, timedelta
import hashlib
import json
import pymongo
import re
import socket
import subprocess
import threading
import time
import uuid

from lgTask.errors import *
from lgTask.lib.timeInterval import TimeInterval
from lgTask.stats import StatsInterface

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

    Thread-safe.  Note, however, that the underlying mongodb connection object
    has auto_start_request set to False, meaning you might have to use code
    blocks with start_request if you (ab)use the database object directly.
    """
    
    STAT_COLLECTION = "lgTaskStat"
    TASK_COLLECTION = "lgTask"
    SCHEDULE_COLLECTION = "lgTaskSchedule"

    class states(object):
        """Various states and collections of states."""
        REQUEST = 'request'
        WORKING = 'working'
        KILL = 'kill'
        SUCCESS = 'success'
        RETRIED = 'retried'
        ERROR = 'error'
        NOT_STARTED_GROUP = [ 'request' ]
        RUNNING_GROUP = [ 'working', 'kill' ]
        DONE_GROUP = [ 'success', 'retried', 'error' ]
    
    bindingsEncode = [ _encodePyMongo ]
    bindingsDecode = [ _decodePyMongo ]

    def _getDatabase(self):
        return self._database
    database = property(_getDatabase)


    _stats = None
    def _getStatsInterface(self):
        if self._stats is None:
            self._stats = StatsInterface(self._database[self.STAT_COLLECTION])
        return self._stats
    stats = property(_getStatsInterface)


    def __init__(self, connString, **kwargs):
        """Create a new lgTask.Connection based off of a given connString or
        already initialized Connection.  
        
        NOTE: If connString is an object, then a _BRAND NEW_ connection to
        that object will be created.  This is to prevent pymongo.Connection,
        which is thread-safe but not for using the same socket on multiple
        threads, from re-using the same socket in different procs.
        """
        if isinstance(connString, Connection):
            connString = connString._database

        if isinstance(connString, pymongo.database.Database):
            self._initPyMongoDb(connString)
        elif connString.startswith("pymongo://"):
            self._initPyMongo(connString)
        else:
            raise ValueError("Unrecognized connString: {0}".format(connString))

        self._lock = threading.Lock()
        self._lock_doc = """Locking mechanism for cached data"""

        # Keeping track of the # of tasks in the table for statistical tracking
        # of collision likeliness.
        self._taskCount = 0
        self._taskCountLast = 0.0


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
            if (kwargs != existing.get('kwargs')
                or priority < existing.get('priority', 0)):
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
                , fields = { '_id': 1 }
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
            try:
                return self._createTask(now, taskClass, taskArgs, kwargs)
            except pymongo.errors.DuplicateKeyError:
                # Failed to insert, someone else batched it first!
                return self.batchTask(runAt, taskClass, priority = priority
                        , **originalKwargs
                )
        else:
            return existing['_id']


    def close(self):
        """Close the lgTask connection... mainly for forked processes.
        """
        if isinstance(self._connection, pymongo.Connection):
            self._connection.disconnect()
        else:
            raise NotImplementedError()


    def createTask(self, taskClass, priority=0, **kwargs):
        """Creates a task to be executed immediately.
        """
        now = datetime.utcnow()
        kwargs = self._kwargsEncode(kwargs)
        return self._createTask(now, taskClass, { 'priority': priority }, kwargs)

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
        return self._createTask(now, taskClass, taskArgs, kwargs)

    def getTalk(self, **kwargs):
        """Returns an lgTask.talk.TalkConnection object with the same 
        database and encoding / decoding as this TaskConnection.
        """
        from lgTask import talk
        tc = talk.TalkConnection(self, **kwargs)
        return tc
    
    def getTask(self, taskId):
        """Gets the document for the given task."""
        return self._database[self.TASK_COLLECTION].find_one(taskId)

    def getTasksToKill(self, taskIdList):
        """Gets tasks that should be killed in the given list."""
        r = self._database[self.TASK_COLLECTION].find({
            '_id': { '$in': taskIdList }
            , 'state': 'kill'
        }, fields = [])
        return [ t['_id'] for t in r ]

    def getWorking(self, host = False, taskClass = None):
        """Get pymongo cursor of working tasks' _id and tsStart.  Includes tasks
        in the "kill" state, since they are technically running.

        host -- If True, only on this host.  Also, don't include tasks with
            a splinterId attribute, since they are not actually "running"
            persay.
        taskClass -- Only show this taskClass
        """
        query = { 'state': { '$in': self.states.RUNNING_GROUP } }
        if host:
            query['host'] = socket.gethostname()
            query['splinterId'] = { '$exists': 0 }
        if taskClass:
            query['taskClass'] = taskClass
        return self._database[self.TASK_COLLECTION].find(
                query, [ 'tsStart' ]
        )
        
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
        return self._createTask(now, taskClass, taskArgs, kwargs)

    def killTask(self, taskId):
        """Marks the given task for death, if it is in a working state and is 
        not splintered.

        If the task is not splintered and is in a not started state, marks it
        as error and sets lastLog to "killed before start".
        """
        now = datetime.utcnow()
        self._database[self.TASK_COLLECTION].update(
            {
                '_id': taskId
                , 'state': { '$in': self.states.NOT_STARTED_GROUP }
                , 'splinterId': { '$exists': False }
            }
            , { '$set': { 
                'state': self.states.ERROR
                , 'lastLog': 'killed before start'
                , 'tsStop': now
            }}
        )
        self._database[self.TASK_COLLECTION].update(
            { 
                '_id': taskId
                , 'state': self.states.WORKING
                , 'splinterId': { '$exists': False } 
            }
            , { '$set': { 'state': self.states.KILL } }
        )

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
        
        # Ok... since we can't use the full index with find_and_modify, we'll
        # just use the find / update operations over and over
        task = None
        updates = dict(
            state = 'working'
            , tsStart = now
            , host = socket.gethostname()
        )
        while True:
            # SLOW
            #task = c.find_and_modify(
            #    {
            #        'taskClass': { '$in': availableTasks.keys() }
            #        , 'state': 'request'
            #        , 'tsRequest': { '$lte': now }
            #    }
            #    , {
            #        '$set': updates
            #    }
            #    , new = True
            #    , sort = [ ('priority', -1), ('tsRequest', 1) ]
            #)
            #return task
            task = c.find_one(
                {
                    'taskClass': { '$in': availableTasks.keys() }
                    , 'state': 'request'
                    , 'tsRequest': { '$lte': now }
                }
                , sort = [ ('priority', -1), ('tsRequest', 1) ]
            )
            if task is None:
                # No tasks are waiting to run
                break

            newUpdated = updates
            splinterUpdated = None
            if task.get('batch') or task.get('schedule'):
                # For batch and scheduled tasks, we'll need to create a task
                # that we're actually going to run, and point back to that from
                # the batch / scheduled task.
                splinterUpdated = updates.copy()
                splinterUpdated['splinterOf'] = task['_id']

                splinterTask = self._createTask(
                    now
                    , task['taskClass']
                    , splinterUpdated
                    , task['kwargs']
                )

                newUpdated = updates.copy()
                newUpdated['splinterId'] = splinterTask
                splinterUpdated['_id'] = splinterTask

            r = c.update(
                { '_id': task['_id'], 'state': 'request' }
                , { '$set': newUpdated }
                , safe = True
            )
            if r.get('updatedExisting') == True:
                # Successfully acquired the task
                if splinterUpdated is not None:
                    task.update(splinterUpdated)
                else:
                    task.update(newUpdated)
                break

        return task
    
    def talkGetTaskLog(self, taskId, blockIndex = 0, blockSize = 4*1024):
        """Requires lgTask.talk to work, and to be on a local network with the
        processors.  Gets the whole log for the given task.
        
        Note that if you'd like to reverse the log, simply start with 
        blockIndex = -1, and continue going negative until the size returned
        is less than blockSize.
        
        Internal note -- tested in test_talk.py, since it requires talk.
        """
        task = self._database[self.TASK_COLLECTION].find_one(taskId)
        self.batchTask('now', 'ProcessorInfoTask-' + task['host'])
        talk = self.getTalk()
        return talk.map('lgProcessorInfo-' + task['host']
                 , [ ( 'log', taskId, blockSize, blockIndex ) ]
                 , timeout = 10.0
        )[0]

    def taskDied(self, taskId, startedBefore, diedReason):
        """Called when a task's process is detected as dead.  Updates the 
        database unless the status is success or error.

        The task must be running on the machine this is called on to have
        any effect.

        startedBefore - Since batched tasks can be re-inserted into the 
            database and potentially restarted before this function is called,
            we need to use a known upper bound on start time to prevent
            data corruption.
        diedReason - Short description of why it's being marked dead.  Should
            not have a period at the end.
        """
        c = self._database[self.TASK_COLLECTION]
        taskData = c.find_one({ 
            '_id': taskId
            , 'state': { '$nin': self.states.DONE_GROUP }
            , 'tsStart': { '$lte': startedBefore }
        })
        if taskData is not None:
            msg = 'Task died - see log: ' + diedReason
            self.taskStopped(taskId, taskData, False, msg)
        
    def taskStopped(self, taskId, taskData, success, lastLog):
        """Update the database noting that a task has stopped.  Must be callable
        multiple times; for instance, if a task thread fails, and then in this
        method an exception is raised, then the Processor's monitor will call
        it again.
        
        taskId - the Task being stopped

        taskData - the data for the task being stopped
        
        success - True for success, False for error.  May also be an instance
            of RetryTaskError for a retry.
        
        lastLog - The last log message.
        """
        c = self._database[self.TASK_COLLECTION]

        now = datetime.utcnow()
        finishUpdates = {
            'tsStop': now
            , 'lastLog': lastLog
        }
        if isinstance(success, RetryTaskError):
            try:
                if taskData.get('splinterOf'):
                    # Just need to reset the splintered task to request... 
                    # probably
                    self._onSplinterRetry(taskData, now, success.delay)
                else:
                    self._retryTask(taskData, success.delay)
                finishUpdates['state'] = self.states.RETRIED
            except Exception, e:
                # We still want to mark end state, etc.
                finishUpdates['state'] = 'error'
                finishUpdates['lastLog'] = 'Tried to retry; got exception: {0}'\
                    .format(e)
        else:
            finishUpdates['state'] = 'success' if success else 'error'

        if (finishUpdates['state'] != self.states.RETRIED
                and taskData.get('splinterOf')):
            # Before we try to update our task, update the task we splintered
            # from.  If that fails, we'll get called again by a monitor, no big
            # deal.
            # But we want to be sure the next iteration of the splintering task
            # gets set up.
            self._onSplinterStop(taskData, now)

        #Standard set-as-finished update; safe to be called repeatedly.
        c.update(
            { '_id': taskId, 'state': { '$nin': self.states.DONE_GROUP } }
            , { '$set': finishUpdates }
        )

    def _cleanupTask(self, taskId):
        """Remove the db record for the given taskId.  Must already be 
        finished.
        """
        c = self._database[self.TASK_COLLECTION]
        c.remove({ '_id': taskId, 'state': { '$in': self.states.DONE_GROUP } })

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

        taskColl = self._database[self.TASK_COLLECTION]
        if '_id' not in taskDefaultArgs:
            while True:
                try:
                    taskDefaultArgs['_id'] = self._getNewId()
                    taskColl.insert(taskDefaultArgs, safe = True)
                    break
                except pymongo.errors.DuplicateKeyError:
                    # Same _id, pretty unlikely, but possible
                    continue
        else:
            taskColl.insert(taskDefaultArgs, safe = True)

        # If we get here, new task inserted OK
        with self._lock:
            self._taskCount += 1

        return taskDefaultArgs['_id']
                
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
        
        c = pymongo.Connection(host=host, port=port
            , auto_start_request = False
        )
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
        col = self._database[self.TASK_COLLECTION]

        desired = set()
        desired.add(( 
                ('state', 1)
                , ('priority', -1)
                , ('tsRequest', 1)
        ))
        desired.add((
                ('state', 1)
                , ('host', 1)
        ))
        desired.add((
                ('state', 1)
                , ('taskClass', 1)
        ))
        desired.add((
                ('tsStop', 1)
                , 
        ))

        indexes = col.index_information()
        # Clean up indexes
        for name, index in indexes.items():
            if name.startswith('_'):
                # System index
                continue
            if tuple(index['key']) not in desired:
                col.drop_index(name)

        for index in desired:
            col.ensure_index(list(index))

    def _getNewId(self):
        """Generate a new potential ID for a task.  Generates one such that
        the likelihood of collision with an existing task is less than one
        percent.
        """
        self._updateTaskCount()
        minChars = 2
        goodFor = 16 * 16.0
        while self._taskCount / goodFor >= 0.01:
            minChars += 1
            goodFor *= 16

        u = hashlib.sha1(uuid.uuid4().hex).hexdigest()
        return u[:minChars]
        # OLD code to actually get shortest possible... stochastic is a lot
        # faster though.
        ids = [ u[:i] for i in xrange(minChars, len(u)) ]
        # This IS an extra query just to keep the IDs nice, but oh well.
        existing = self._database[self.TASK_COLLECTION].find(
            { '_id': { '$in': ids } }
            , fields = []
        )
        existingIds = [ d['_id'] for d in existing ]
        for i in ids:
            if i not in existingIds:
                return i
        raise ValueError("Duplicate UUID?  UNLIKELY")

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
        # Use the same database; pymongo has good multiprocessing code, so
        # it's ok if we're a forked process.
        self._connection = db._connection
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

    def _onSplinterRetry(self, splinterData, now, delay):
        """Retry a task that was a splinter after the given delay.
        """
        runAt = self._getRunAtTime(now, delay)
        splinterId = splinterData['_id']
        taskId = splinterData['splinterOf']
        c = self._database[self.TASK_COLLECTION]
        c.update(
            { '_id': taskId, 'splinterId': splinterId }
            , {
                '$set': { 
                    'state': 'request'
                    , 'tsRequest': runAt
                    , 'lastLog': 'Scheduled for retry from ' + splinterId
                }
                , '$unset': {
                    'splinterId': 1
                }
            }
        )

    def _onSplinterStop(self, splinterData, now):
        """When a task with a splinterOf attribute gets stopped, update the
        splintering task to ensure that subsequent runs are executed.

        This needs to be designed so it can be called more than once.
        """
        # We really just need to pull down the splintering task, and execute
        # the next step there.
        c = self._database[self.TASK_COLLECTION]

        splinterId = splinterData['_id']
        taskId = splinterData['splinterOf']

        # We're going to do some sequential updates without checking if they
        # succeed, so we need a request here.
        with c.database.connection.start_request():
            # Note about these updates - if splinterId isn't our splinter's id,
            # then this call (_onSplinterStop) previously succeeded, so 
            # everything is working fine.
            if splinterData.get('batch'):
                # If it was a batch task, update the splinter like a batch
                c.update(
                    { 
                        '_id': taskId
                        , 'splinterId': splinterId
                        # We want to match only if batchQueued exists, since 
                        # we're setting the state back to request.
                        , 'batchQueued': { '$exists': True }
                    }
                    , {
                        '$set': {
                            'state': 'request'
                        }
                        , '$unset': {
                            'splinterId': 1
                        }
                        , '$rename': {
                            # Overwrite tsRequest with batched time
                            'batchQueued': 'tsRequest'
                        }
                    }
                )

            if splinterData.get('schedule'):
                nextTime = self._scheduleGetNextRunTime(
                    taskId
                    , splinterData['tsStart']
                    , now
                )
                if nextTime is not None:
                    c.update(
                        {
                            '_id': taskId
                            , 'splinterId': splinterId
                        }
                        , {
                            '$set': {
                                'state': 'request'
                                , 'tsRequest': nextTime
                            }
                            , '$unset': {
                                'splinterId': 1
                            }
                        }
                    )

            # Regardless, if we reach here, it shouldn't be in working state
            c.update(
                {
                    '_id': taskId
                    , 'splinterId': splinterId
                }
                , {
                    '$set': {
                        'state': 'success'
                        , 'lastLog': 'See splinter task ' + splinterId
                    }
                    , '$unset': {
                        'splinterId': 1
                    }
                }
            )

    def _retryTask(self, taskData, delay):
        """Queue up an identical version of task after delay time.
        """
        now = datetime.utcnow()
        runAt = self._getRunAtTime(now, delay)

        taskArgs = {
          'tsRequest': runAt
          , 'retry': taskData.get('retry', 0) + 1
          , 'priority': taskData.get('priority', 0)
        }
        self._createTask(
            now
            , taskData['taskClass']
            , taskArgs
            , taskData['kwargs']
        )

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

    def _updateTaskCount(self):
        """Update self._taskCount if it is old.
        """
        now = time.time()
        updateInterval = 60.0
        doUpdate = False
        if now - self._taskCountLast > updateInterval:
            # Ensure we should update
            with self._lock:
                if now - self._taskCountLast > updateInterval:
                    doUpdate = True
                    self._taskCountLast = now

        if doUpdate:
            taskColl = self._database[self.TASK_COLLECTION]
            self._taskCount = taskColl.count()


