
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
    
    bindingsEncode = [ _encodePyMongo ]
    bindingsDecode = [ _decodePyMongo ]
    
    def __init__(self, connString, **kwargs):
        if isinstance(connString, pymongo.database.Database):
            self._initPyMongoDb(connString)
        elif connString.startswith("pymongo://"):
            self._initPyMongo(connString)
        else:
            raise ValueError("Unrecognized connString: {0}".format(connString))
        
    def createTask(self, taskClass, runAt=None, **kwargs):
        kwargsEncoded = {}
        for key,value in kwargs.items():
            newValue = value
            for binding in self.bindingsEncode:
                decodedValue = binding(value)
                if decodedValue is not None:
                    newValue = decodedValue
                    break
            kwargsEncoded[key] = newValue

        taskArgs = {
            'tsStart': None
            ,'tsStop': None
            ,'state': 'request'
            ,'taskClass': taskClass
            ,'kwargs': kwargsEncoded
        }
        now = datetime.utcnow()
        taskArgs['tsSchedule'] = now
        if runAt is None:
            taskArgs['tsRequest'] = now
        else:
            if isinstance(runAt, datetime):
                pass
            elif isinstance(runAt, timedelta):
                runAt = now + runAt
            elif isinstance(runAt, basestring):
                runAt = now + TimeInterval(runAt)
            else:
                raise ValueError("runAt must be datetime, timedelta, or str")
            
            taskArgs['tsRequest'] = runAt
            
        taskColl = self._database[self.TASK_COLLECTION]
        taskColl.insert(taskArgs)
        
    def ensureIndexes(self):
        """Assert that all necessary indexes exist in the tasks collections 
        to maintain performance
        """
        db = self._database[self.TASK_COLLECTION]
        db.ensure_index( [ ('state', 1), ( 'taskClass', 1), ( 'tsRequest', -1 ) ] )
        
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
        
    def startTask(self, availableTasks):
        """Gets a task from our database and calls its start() method.
        
        Returns either the new Task object or None if no suitable task was
        found.  
        
        Exceptions are forwarded to the caller.
        
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
            }}
            , new = True
        )
        
        if taskData is None:
            return None
        
        taskId = taskData['_id']
        try:
            taskName = taskData['kwargs'].get('taskName', None)
            task = availableTasks[taskData['taskClass']](
                    taskConnection=self
                    ,taskName=taskName
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
        
    def taskStopped(self, taskId, success, logs):
        """Update the database noting that a task has stopped.  Must be callable
        multiple times; see Task._finished.
        
        taskId - the id of the task
        
        success - True for success, False for error
        
        logs - array of log entries.
        """
        c = self._database[self.TASK_COLLECTION]
        now = datetime.utcnow()
        c.update(
            { '_id': taskId }
            , { '$set': {
                'state': 'success' if success else 'error'
                , 'tsStop': now
                , 'lastLog': logs[-1] if len(logs) > 0 else '(No log entries)'
            }} 
        )
                
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

