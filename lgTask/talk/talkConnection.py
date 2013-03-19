
import datetime
import socket
import threading
import time
import uuid

# We use rfoo to communicate between nodes; I also tried Pyro4, but rfoo is
# much faster and more robust.
try:
    import rfoo
    def _rpcSpawnServer(talkServer):
        """Runs an rpc server in a new thread and returns a unique identifier
        that the rpcGetProxy() method can take as an argument to connect to
        that server.
        
        talkServer -- _Server
        
        Returns tuple: ( uri for connection, server object )
        """
        class _RfooRpcClient(rfoo.BaseHandler):
            def getObjects(self, *args, **kwargs):
                return talkServer.getObjects(*args, **kwargs)
        server = rfoo.InetServer(_RfooRpcClient)
        thread = threading.Thread(
            target = server.start,
            kwargs = dict(host = '0.0.0.0', port = 0)
        )
        thread.daemon = True
        thread.start()
        while True:
            # Wait until we know our port
            port = server._conn.getsockname()[1]
            if port != 0:
                break
            time.sleep(0.01)
        uri = socket.gethostname() + ':' + str(port)
        return ( uri, server )
    
    def _rpcGetProxy(uri):
        """Gets a proxy for the given uri.
        
        Returns: proxy obj
        """
        host, port = uri.split(':')
        conn = rfoo.InetConnection().connect(
                host = host
                , port = int(port)
        )
        proxy = rfoo.Proxy(conn)
        return proxy
    
    def _rpcCloseProxy(proxy):
        """Closes a proxy object previously returned by _rpcGetProxy.
        """
        proxy._conn.close()
    
    
except ImportError:
    # rfoo is definitely preferred (faster), but since Pyro4 is pure python
    # and easier to install (e.g. on Mac OS X), we'll support that too.
    try:
        import Pyro4
        
        Pyro4.config.HMAC_KEY = '897FHEHEfuheulhunvzus&#*uuh*^(#^@#%' * 88 + 'a'
        Pyro4.config.COMPRESSION = False #Faster without...
        
        def _rpcSpawnServer(talkServer):
            server = Pyro4.Daemon(host = '0.0.0.0')
            uri = server.register(talkServer)
            thread = threading.Thread(target = server.requestLoop)
            thread.daemon = True
            thread.start()
            return (uri.asString(), server)
        
        def _rpcGetProxy(uri):
            proxy = Pyro4.Proxy(uri)
            return proxy
        
        def _rpcCloseProxy(proxy):
            proxy._pyroRelease()
            
            
    except ImportError:
        raise ImportError("Requires either rfoo (preferred) or Pyro4")

from lgTask.talk.error import TalkError, TalkTimeoutError

def resetFork():
    """Since we use some class-level global state variables, forks can 
    become corrupt if we don't clean them up.
    """
    # Seems infeasible, but guess what?  A lot of times the fork() happens
    # within this lock, meaning that forked processes have a permalock on 
    # _Server._cls_lock.  So re-allocate it here to get around that.
    _Server._cls_lock = threading.Lock()
    _Server._servers = {}
    TalkConnection._proxies = {}


class _ServerKey(object):
    """Responsible to the _Server for handling dispatching to and from a
    key.
    """

    def __init__(self, server, key):
        self.server = server
        self.key = key
        self._lock = threading.Lock()
        self._requests = []
        self._requests_doc = """List of _SendRequest objects waiting to be 
                sent; kept in priority order, FIFO order."""

    
    def addRequest(self, request):
        """Adds an _SendRequest that needs to be filled."""
        with self._lock:
            added = False
            reqs = self._requests[:]
            for i in reversed(xrange(len(reqs))):
                r = reqs[i]
                if r.isClosed:
                    reqs.pop(i)
            ll = len(self._requests) - 1
            for i, r in enumerate(reversed(reqs)):
                # Convert to reversed order
                i = ll - i
                if request.priority <= r.priority:
                    reqs.insert(i + 1, request)
                    added = True
                    break
            if not added:
                reqs.insert(0, request)
            self._requests = reqs
        self._updateCount()


    def getObjects(self, desired, batchSize):
        """Receive as many objects that we're trying to send as we can."""
        objs = []
        # self._requests is immutable in-place, so..
        reqs = self._requests
        for r in reqs:
            desired -= r.sendTo(objs, desired, batchSize)
            if desired <= 0:
                break
        # Well, return what we could get
        return objs


    def _updateCount(self):
        """Updates server count with # of available items to send.

        Whether or not this is called in a lock is unimportant, since 
        self._requests is immutable.
        """
        priority = None
        count = 0
        for r in self._requests:
            if priority != r.priority:
                if priority is None:
                    priority = r.priority
                else:
                    # Different priority, don't include in our count
                    break
            count += r.count
        self.server.updateKeyCount(self.key, priority, count)


class _SendRequest(object):
    """Responsible for keeping track of some objects to send as well as their
    priority.  Represents a thread sitting on send()
    """

    def __init__(self, endTime, priority, objects):
        self._endTime = endTime
        self._priority = priority
        self._objects = objects
        self._sent = 0
        self._total = len(objects)
        self._eventDone = threading.Event()
        self._lock = threading.Lock()
        self._isClosed = False


    @property
    def count(self):
        return self._total - self._sent


    @property
    def isClosed(self):
        return self._isClosed


    @property
    def priority(self):
        return self._priority


    def poll(self):
        """Returns True if this request is done or should be done; False
        otherwise.  You must call wait(0) after this if you're using it to stop
        the request.
        """
        with self._lock:
            if self._eventDone.isSet() or time.time() >= self._endTime:
                return True
        return False


    def sendTo(self, results, desired, batchSize):
        """Add objects from ourselves into results as much as we can.  Allows
        an overflow of 50%; that is, if we have more than desired objects in
        our request, but batchSize * 1.5 > batchSize - desired + our objs, then
        still return everything.

        Returns the total # of objects added.
        """
        with self._lock:
            if self._isClosed:
                return 0
            nr = self._total - self._sent
            if nr > desired and nr + batchSize - desired > batchSize * 1.5:
                nr = desired
            oldSent = self._sent
            newSent = oldSent + nr
            self._sent = newSent
            if self._sent == self._total:
                # We're done being serviced, close out and flag done
                self._eventDone.set()
                self._isClosed = True

        # Commit the objects we're sending
        results.extend(self._objects[oldSent:newSent])
        return nr


    def wait(self, seconds = None):
        """Wait for up to seconds to get everything sent

        seconds [double] -- Time to wait before closing this request.  May be
                unspecified to wait for the actual endTime specified
        
        Returns number of items actually sent before time ran out.
        """
        timeToWait = seconds
        if timeToWait is None:
            timeToWait = max(0, self._endTime - time.time())
        if self._eventDone.wait(timeToWait) is False:
            with self._lock:
                # Avoid race condition, check again now that we have lock
                if not self._eventDone.isSet():
                    self._isClosed = True
        return self._sent


class _Server(object):
    """Local receive hub.  Responsible for accepting new messages and updating
    the TalkConnection.TALK_RECV_COLLECTION stats with our information.
    """
    _cls_lock = threading.Lock()
    _cls_lock_doc = """Class-level lock to get a server for a task db"""

    _servers = {}
    _servers_doc = """Class-level dict of servers for different db instances"""

    @classmethod
    def get(cls, talkDb):
        i = ( talkDb.connection.host, talkDb.connection.port, talkDb.name )
        with cls._cls_lock:
            r = cls._servers.get(i)
            if r is None:
                r = cls._servers[i] = _Server(talkDb)
        return r


    def __init__(self, talkDb):
        self._db = talkDb
        self._colSender = self._db[TalkConnection.TALK_SENDER_COLLECTION]
        self._sendId = uuid.uuid4().hex
        
        self._uri, self._server = _rpcSpawnServer(self)

        self._lock = threading.Lock()
        self._keys = {}


    def getObjects(self, key, desired, batchSize):
        """Called from remote.  Looks at pending objects we have to send and
        returns them.

        key -- Key to receive from
        desired -- Desired # of items returned
        batchSize -- Original # of items desired

        Returns an array of 0-batchSize objects.
        """
        kd = self._getKeyData(key)
        return kd.getObjects(desired, batchSize)


    def sendAndRecvObjects(self, priority, keysToObjects, recvKey,
            recvBatch, recvFunction, timeout):
        """Called locally; tries to send objects while simultaneously receiving.
        Returns early if and only if all objects are sent and the receive batch
        is full.

        Returns a tuple: (numSent, objsReceived)
        """
        # Keep track of when we'll be done
        e = time.time() + timeout

        srs = []
        for key, objs in keysToObjects.iteritems():
            kd = self._getKeyData(key)
            sr = _SendRequest(e, priority, objs)
            kd.addRequest(sr)
            srs.append(sr)

        # Now do the recv
        objsReceived = recvFunction(recvKey, batchSize = recvBatch,
                batchTime = timeout, timeout = 0)

        r = srs[0].wait(e - time.time())
        for sr in srs[1:]:
            r += sr.wait(0)
        return (r, objsReceived)

    def sendObjects(self, priority, keysToObjects, timeout, async):
        """Called locally.  Hands off objects to a local listener when asked
        to send objects.  Returns # actually sent
        """
        e = time.time() + timeout

        srs = []
        for key, objs in keysToObjects.iteritems():
            kd = self._getKeyData(key)
            sr = _SendRequest(e, priority, objs)
            kd.addRequest(sr)
            srs.append(sr)

        if async:
            return srs

        r = srs[0].wait(timeout)
        for sr in srs[1:]:
            r += sr.wait(0)
        return r


    def updateKeyCount(self, key, priority, count):
        """Called when we want to send objects; used to adjust database stats so
        that we can keep track of which server is the busiest.
        """
        dId = self._getColSendId(key)
        newDoc = {
            '_id': dId,
            'key': key,
            'priority': priority,
            'count': count,
            'uri': self._uri
        }
        self._colSender.update({ '_id': dId }, newDoc, upsert = True)


    def _getColSendId(self, key):
        """Gets the _id for a document in self._colRecv for the given key.
        """
        return self._sendId + '_' + key


    def _getKeyData(self, key):
        with self._lock:
            r = self._keys.get(key)
            if r is None:
                r = self._keys[key] = _ServerKey(self, key)
        return r


class _Proxy(object):
    """Locking proxy - proxies into a _Server object"""

    POOL_TIME = 10.0
    POOL_TIME_doc = """Seconds before closing connection after done with it"""

    def __init__(self, uri):
        self._uri = uri
        self._lock = threading.Lock()
        self._proxy = _rpcGetProxy(uri)
        self._timer = None


    def __enter__(self):
        """Ensures that we were already locked, and returns self.
        """
        if not self._lock.locked():
            raise AssertionError("Should already be locked")
        return self


    def __exit__(self, exc, excType, tb):
        """Release our lock"""
        self.release()


    def __getattr__(self, name):
        return getattr(self._proxy, name)


    def acquire(self):
        """Returns True if lock was acquired, or False if it was already
        locked.

        Also cancels the pooling timer.
        """
        r = self._lock.acquire(0)
        if r and self._timer is not None:
            self._timer.cancel()
        return r


    def close(self):
        """Closes this proxy, removes from TalkConnection."""
        if self._lock.locked():
            # We're locked, can't be closed
            return

        with TalkConnection._lock:
            ps = TalkConnection._proxies[self._uri]
            ps.remove(self)
            if len(ps) == 0:
                del TalkConnection._proxies[self._uri]

        # Get rid of the connection
        _rpcCloseProxy(self._proxy)


    def release(self):
        """Releases the lock; also, searches the proxy records in 
        TalkConnection to bump ourselves up to the top of the list.
        Sets a timer to close this proxy if we're not used again in X seconds.
        """
        self._timer = threading.Timer(self.POOL_TIME, self.close)
        self._timer.daemon = True
        self._timer.start()
        with TalkConnection._lock:
            ps = TalkConnection._proxies[self._uri]
            ps.remove(self)
            ps.insert(0, self)
        self._lock.release()


class TalkConnection(object):
    """A connection to a talk database, which is always adjacent to an
    lgTask database.

    lgTaskTalk is used to stream minor (non-task-level) work in a queue to
    workers that can do the processing.  The major difference between using it
    and something like redis as the intermediary is that lgTaskTalk's send()
    method deliberately does not return until all of the data has sent or a 
    timeout is reached.  In other words, if send() succeeds, the work is being
    done *right now*.  If it fails, the application can decide whether it's 
    overloaded and needs to back off, spawn more workers, or discard the data
    entirely.

    What you also gain over using something like redis is the ability to 
    specify a priority when you are sending work along -- that is, if the
    system enters a loaded state, high-priority work will still be done in
    as timely a manner as possible.

    And, it's faster than redis, at least in my tests.  But I might just suck
    at using redis.
    """

    TALK_SENDER_COLLECTION = 'lgTaskTalkSender'
    TALK_HANDLER_COLLECTION = 'lgTaskTalkHandler'
    
    _HMAC_KEY_DUMMY = (
        'lgTaskDummyHmac6&^#()6891625HUHuhefhzsJKH#2235&&!9-_'*71 + 'hwie3')

    # Class vars
    _lock = threading.Lock()

    _proxies = {}
    _proxies_doc = """{ uri : [ proxy ] } for all active proxies."""

    def __init__(self, taskConnection, sendTimeout = 60.0,
            recvBatchTime = 0.4, recvTimeout = 60.0):
        self._tc = taskConnection
        self._colSender = self._tc._database[self.TALK_SENDER_COLLECTION]
        self._colHandler = self._tc._database[self.TALK_HANDLER_COLLECTION]
        self._sendTimeout = sendTimeout
        self._recvBatchTime = recvBatchTime
        self._recvTimeout = recvTimeout


    def send(self, key, objects, **kwargs):
        """Sends the given objects to a receiver with the specified key.

        Returns # of objects sent, which will be len(objects).

        Raises a TalkTimeoutError if not all objects are sent.
        """
        return self.sendMultiple({ key: objects }, **kwargs)


    def sendMultiple(self, keysToObjects, timeout = None, priority = 0,
            noRaiseOnTimeout = False, _async = False):
        """Sends the given objects to a receiver with the specified keys.

        keysToObjects -- { key: [ objects ]} objects to send

        _async -- If True, returns the _SendRequest objects.  Internally used
                by tasks.

        Raises a TalkTimeoutError if not all objects are sent, unless 
        noRaiseOnTimeout is specified as True.  

        Returns the number of objects actully sent.
        """
        if timeout is None:
            timeout = self._sendTimeout

        allObjs = sum([ len(o) for o in keysToObjects.itervalues() ])
        if allObjs == 0:
            # Nothing to do
            return 0
        if not hasattr(self, '_server'):
            self._server = _Server.get(self._tc._database)
        r = self._server.sendObjects(priority, keysToObjects, timeout,
                async = _async)
        if not _async and r != allObjs and not noRaiseOnTimeout:
            raise TalkTimeoutError("Sent {0}".format(r))
        return r


    def map(self, key, objects, timeout = None, priority = 0):
        """Sends the given objects to a receiver, and waits for a response
        on a special queue.  Note that objects are sent in a tuple like:
        
        ( 'responseQueue', responseId, object )

        They are then paired up before being returned to the caller.  For
        use with tasks deriving from lgTask.talk.MappingTask; think things like
        signing services or gateways to controlled resources.

        Returns a list like [ objReceived ].  objReceived may be None
        if an item did not receive a response in the timeout
        """
        if timeout is None:
            timeout = self._sendTimeout

        myResponseQueue = 'map-' + uuid.uuid4().hex
        numObjs = len(objects)
        objsToSend = [ ( myResponseQueue, i, o) 
                for i, o in enumerate(objects) ]

        if not hasattr(self, '_server'):
            self._server = _Server.get(self._tc._database)
        sent, objsReceived = self._server.sendAndRecvObjects(
                priority, { key: objsToSend },
                recvKey = myResponseQueue, recvBatch = numObjs,
                recvFunction = self.recv,
                timeout = timeout
        )
        results = [ None ] * numObjs
        for r in objsReceived:
            i = r[0]
            results[i] = r[1]
        return results


    def recv(self, keyOrKeys, batchSize = 1, batchTime = None,
            timeout = None, raiseOnTimeout = False):
        """Receive zero or up to batchSize objects with the specified key or 
        keys.

        keyOrKeys -- Key or keys to receive objects from
        batchSize -- Ideal number of objects to receive; there is a soft limit
                50% over this.
        batchTime -- After the first item, wait this long for more items to
                arrive before returning (even if we don't get batchSize items).
        timeout -- Maximum seconds to wait for next item, or raise a
                TalkTimeoutError.  May be None for no limit.  May be 0 to
                imply that we just want any items recv'd within batchTime
                returned, even if that list is empty.
        raiseOnTimeout -- If True, raise an Exception on timeout rather than
                returning an empty list.
        """
        if batchTime is None:
            batchTime = self._recvBatchTime

        keys = keyOrKeys
        if type(keys) != list:
            keys = [ keys ]

        e = None
        isBatchOnly = False
        if timeout <= 0:
            isBatchOnly = True
            e = time.time() + batchTime
        elif timeout is not None:
            e = time.time() + timeout

        objs = []
        while True:
            left = batchSize - len(objs)
            # Due to pymongo's find_and_modify not being developed enough to
            # support a key_or_list for sort, we have to do a find..update 
            # trick.
            recvFrom = self._colSender.find_one(
                {
                    'key': { '$in': keys },
                    'count': { '$gt': 0 }
                },
                sort = [ ('priority', -1), ('count', -1) ]
            )
            if recvFrom is not None:
                r = self._colSender.update(
                    { '_id': recvFrom['_id'], 'count': recvFrom['count'] },
                    { '$inc': { 'count': -left } },
                    safe = True
                )
                if not r.get('updatedExisting'):
                    # Someone else got there first
                    continue
                    '''
            recvFrom = self._colSender.find_and_modify(
                {
                    'key': { '$in': keys }
                    , 'count': { '$gt': 0 }
                }
                , {
                    '$inc': { 'count': -left }
                }
                , sort = { 'count': -1 }
            )
            if recvFrom is not None:'''
                try:
                    with self._getAndLockProxy(recvFrom['uri']) as p:
                        newObjs = p.getObjects(recvFrom['key'], left, batchSize)
                except socket.error:
                    # Proxy no longer exists, try another
                    continue
                objs.extend(newObjs)
                if len(newObjs) == 0:
                    # We didn't do anything...
                    pass
                elif len(objs) >= batchSize or batchTime <= 0.0:
                    # Done!
                    return objs
                elif not isBatchOnly and left == batchSize:
                    # These were the first objects
                    e = time.time() + batchTime
                    continue
            else:
                # There was nothing to do (no senders found), so sit around
                timeLeft = e - time.time()
                if timeLeft > 0:
                    time.sleep(min(0.2, timeLeft * 0.5))
                    continue

            if time.time() >= e:
                # We're done...
                if raiseOnTimeout:
                    raise TalkTimeoutError("Waited {0} seconds, nothing recv'd"
                            .format(timeout))
                return objs


    def registerHandler(self, key, taskClass, taskKwargs = None, max = 1):
        """Register a task to spawn if there aren't enough currently running
        to handle the load from send()s in the system.
        """
        self._colHandler.update(
            { '_id': key },
            {
                '_id': key,
                'taskClass': taskClass,
                'taskKwargs': taskKwargs or {},
                'max': max
            },
            upsert = True
        )


    def unregisterHandler(self, key):
        """Unregister any taskClass assigned to key.
        """
        self._colHandler.remove({ '_id': key })


    def _createHandler(self, key):
        """Checks the registered handlers for key, and sees if one can be
        spawned.

        Returns True if one is made.
        """
        handler = self._colHandler.find_one({ '_id': key })
        if handler is None:
            return

        cnt = self._tc.getWorking(taskClass = handler['taskClass'])
        if cnt.count() >= handler['max']:
            return

        self._tc.createTask(handler['taskClass'], **handler['taskKwargs'])
        return True


    def _getAndLockProxy(self, uri):
        """Gets and locks a proxy to the given URI.

        While rpc proxies are thread-safe, we implement our own locking around
        it to get an idea of how many concurrent requests we're pushing
        to each server.  Also, sendObjects() might block for a long time to
        prevent a bunch of round trips.
        """
        cls = self.__class__
        with cls._lock:
            ps = cls._proxies.setdefault(uri, [])
            for i, p in enumerate(ps):
                if p.acquire():
                    # We got a good proxy that's not in use
                    return p

            # No such proxy exists
            p = _Proxy(uri)
            p.acquire()
            ps.append(p)
            return p


