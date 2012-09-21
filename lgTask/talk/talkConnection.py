
import datetime
import socket
import threading
import time
import uuid
import Pyro4

from lgTask.talk.error import TalkError, TalkTimeoutError

class _ServerKey(object):
    """Responsible to the _Server for handling dispatching to and from a
    key.
    """

    RECV_POLL = 1.0

    def __init__(self, server, key):
        self.server = server
        self.key = key
        self._lock = threading.Lock()
        self._requests = []
        self._requests_doc = """List of _ServerRequest objects waiting for
                more objects."""
        self._recvTimeout = self.server._RECV_TIMEOUT
        self._recvTimeout_doc = """Time in seconds after which a recvObjects()
                call will time out, leaving the sender to figure out what to
                do with the remaining items.
                """

    
    def addRequest(self, request):
        """Adds an _ServerRequest that needs to be filled."""
        with self._lock:
            self._requests.append(request)


    def recvObjects(self, objects):
        """Receive objects into our available requests, as we can.  Timeout
        with self._recvTimeout.
        """
        e = time.time() + self._recvTimeout
        received = 0
        while True:
            # Poll!
            with self._lock:
                for r in self._requests[:]:
                    received += r.addResults(objects[received:])
                    if received == len(objects):
                        # We're done!  Everything was accepted
                        self._updateQueued()
                        return received
                    elif r.isClosed:
                        # Remove from requests list
                        self._requests.remove(r)

            # See if we should stop, and sleep for next poll
            left = e - time.time()
            if left <= 0:
                break
            time.sleep(min(self.RECV_POLL, left))
        return received


    def _updateQueued(self):
        """Calls self.server.updateKeyQueued so that the database estimate
        doesn't get screwed up.
        """
        self.server.updateKeyQueued(self.key, 0)


class _ServerRequest(object):
    """Responsible for waiting for a certain number of objects and storing
    them.
    """

    def __init__(self, batchSize, batchTime):
        self._batchSize = batchSize
        self._batchTime = batchTime
        self._eventFirst = threading.Event()
        self._eventFull = threading.Event()
        self._lock = threading.Lock()
        self._isClosed = False
        self._results = []
        self._results_doc = """List of objects retrieved"""


    @property
    def isClosed(self):
        return self._isClosed


    def addResults(self, objects):
        """Accepts as many objects into _results as it can.  Allows a soft limit
        of up to 50%; that is, if a request is waiting on 10 more objects, but
        has capacity for 100 objects total, then if addResults() is called with
        60 objects, it will take all 60.  61 objects, and it will only take 10.

        Returns the number of objects accepted.
        """
        with self._lock:
            lr = len(self._results)
            if self._isClosed or lr >= self._batchSize:
                # We won't accept any
                return 0

            # How many should we take?
            nr = len(objects)
            if lr + nr > self._batchSize * 1.5:
                # Even with overflow, can't fit everything
                nr = self._batchSize - lr

            self._results.extend(objects[:nr])

            # Regardless of if we had any or not, set the first event
            self._eventFirst.set()
            # If we're full, set that too
            if len(self._results) >= self._batchSize:
                self._eventFull.set()

            return nr


    def getResults(self):
        """Gets the results of the request; can only be called once."""
        with self._lock:
            if self._isClosed:
                raise RuntimeError("Can only be called on open request")
            assert not self._isClosed
            self._isClosed = True
            return self._results


    def wait(self, time):
        """Wait for up to time seconds to get results triggered.
        """
        if self._eventFirst.wait(time) is False:
            with self._lock:
                # Avoid race condition, check again once we have lock
                if not self._eventFirst.isSet():
                    self._isClosed = True # Accept no more
                    raise TalkTimeoutError(
                            "Waited {0} seconds, nothing received".format(time))
        # We got at least one, wait up to batchTime for full, then return
        self._eventFull.wait(self._batchTime)


class _Server(object):
    """Local receive hub.  Responsible for accepting new messages and updating
    the TalkConnection.TALK_RECV_COLLECTION stats with our information.
    """

    _AVAIL_TIMEOUT = 10.0
    _AVAIL_TIMEOUT_doc = """Seconds to advertise being available to receive
        keys of a specific object type when we get a call to recv().
        """

    _RECV_TIMEOUT = 60.0
    _RECV_TIMEOUT_doc = """Default seconds before timing out any part of
            send()"""

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
        self._colRecv = self._db[TalkConnection.TALK_RECV_COLLECTION]
        self._recvId = uuid.uuid4().hex
        self._pyroDaemon = Pyro4.Daemon(host = socket.gethostname())
        self._pyroThread = threading.Thread(
                target = self._pyroDaemon.requestLoop
        )
        self._pyroThread.daemon = True
        self._pyroThread.start()
        self._uri = self._pyroDaemon.register(self)
        self._lock = threading.Lock()
        self._keys = {}


    def recvObjects(self, keyOrKeys, batchSize, batchTime, timeout):
        """Called locally.  Waits for and receives objects on any of the 
        keys specified.

        keyOrKeys -- String or list of strings to receive one.
        batchSize -- Desired # of objects; receive up to this many.  Not always
                100% enforced; may return a few more.
        batchTime -- After receiving first object, if we're not full, how long
                should we wait to receive more objects?
        timeout -- Maximum seconds to wait for anything before raising a
                TalkTimeoutError

        Returns an array of 0-batchSize objects.
        """
        if isinstance(keyOrKeys, basestring):
            keys = [ keyOrKeys ]
        else:
            keys = keyOrKeys
        
        request = _ServerRequest(batchSize, batchTime)
        for key in keys:
            dId = self._getColRecvId(key)
            myUntil = datetime.datetime.utcnow() + datetime.timedelta(
                    seconds = timeout)
            d = self._colRecv.find_and_modify(
                { '_id': dId }
                , {
                    # With modifiers, _id is implicit
                    '$set': {
                        'key': key
                        , 'tsUntil': myUntil
                        , 'batchSize': batchSize
                        , 'uri': self._uri.asString()
                    }
                    , '$inc': { 'queued': 0 }
                }
                , upsert = True
                , fields = { 'tsUntil': 1 }
                , new = False # We want to see if we clobbered a newer tsUntil
            )
            if d and d.get('tsUntil', myUntil) > myUntil:
                # There's another service here that is valid longer, keep it
                self._colRecv.update({ '_id': dId }
                    , { '$set': { 'tsUntil': d['tsUntil'] }})
            kd = self._getKeyData(key)
            kd.addRequest(request)

        request.wait(timeout)
        return request.getResults()


    def sendObjects(self, key, objects):
        """Called remotely.  Hands off objects to a local listener when asked
        for more objects.
        """
        kd = self._getKeyData(key)
        return kd.recvObjects(objects)


    def updateKeyQueued(self, key, queued):
        """Called when we receive objects; used to adjust database stats so
        that we can keep track of which server is the busiest.
        """
        self._colRecv.update({ '_id': self._getColRecvId(key) }
            , { '$set': { 'queued': queued }}
        )


    def _getColRecvId(self, key):
        """Gets the _id for a document in self._colRecv for the given key.
        """
        return self._recvId + '_' + key


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
        self._proxy = Pyro4.Proxy(uri)
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
        self._proxy._pyroRelease()


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
    """

    TALK_RECV_COLLECTION = 'lgTaskTalkRecv'
    TALK_HANDLER_COLLECTION = 'lgTaskTalkHandlers'
    
    _HMAC_KEY_DUMMY = (
        'lgTaskDummyHmac6&^#()6891625HUHuhefhzsJKH#2235&&!9-_'*71 + 'hwie3')

    # Class vars
    _lock = threading.Lock()

    _proxies = {}
    _proxies_doc = """{ uri : [ proxy ] } for all active proxies."""

    def __init__(self, taskConnection, sendTimeout = 60.0
            , recvBatchTime = 0.4, recvTimeout = 10.0, hmacKey = None):
        self._tc = taskConnection
        self._colRecv = self._tc._database[self.TALK_RECV_COLLECTION]
        self._colHandler = self._tc._database[self.TALK_HANDLER_COLLECTION]
        self._sendTimeout = sendTimeout
        self._recvBatchTime = recvBatchTime
        self._recvTimeout = recvTimeout

        # Global config for Pyro
        Pyro4.config.COMPRESSION = True
        Pyro4.config.HMAC_KEY = hmacKey or self._HMAC_KEY_DUMMY


    def send(self, key, objects, timeout = None):
        """Sends the given objects to a receiver with the specified key.
        """
        if timeout is None:
            timeout = self._sendTimeout

        e = time.time() + timeout
        totalSent = 0
        while objects:
            sent = self._sendBatch(key, objects)
            objects = objects[sent:]
            totalSent += sent
            if time.time() >= e:
                raise TalkTimeoutError('After sending {0} OK'.format(
                        totalSent))


    def recv(self, keyOrKeys, batchSize = 1, batchTime = None
            , timeout = None):
        """Receive zero or up to batchSize objects with the specified key or 
        keys.

        keyOrKeys -- Key or keys to receive objects from
        batchSize -- Ideal number of objects to receive; there is a soft limit
                50% over this.
        batchTime -- After the first item, wait this long for more items to
                arrive before returning (even if we don't get batchSize items).
        timeout -- Maximum seconds to wait for next item, or raise a
                TalkTimeoutError.  May be None for no limit.
        """
        if batchTime is None:
            batchTime = self._recvBatchTime

        keys = keyOrKeys
        if type(keys) != list:
            keys = [ keys ]

        server = _Server.get(self._tc._database)
        return server.recvObjects(
                keyOrKeys
                , batchSize = batchSize
                , batchTime = batchTime
                , timeout = timeout
        )


    def registerHandler(self, key, taskClass, taskKwargs = None, max = 1):
        """Register a task to spawn if there aren't enough currently running
        to handle the load from send()s in the system.
        """
        self._colHandler.update(
            { '_id': key }
            , {
                '_id': key
                , 'taskClass': taskClass
                , 'taskKwargs': taskKwargs or {}
                , 'max': max
            }
            , upsert = True
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

        While Pyro4.Proxy is thread-safe, we implement our own locking around
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

    
    def _sendBatch(self, key, objects):
        """Send as many of objects as we can on the given key to a single
        receiver.

        Returns the number of objects actually sent.  If no objects are sent,
        returns 0.
        """
        now = datetime.datetime.utcnow()
        r = self._colRecv.find_and_modify(
            {
                'key': key
                , 'tsUntil': { '$gte': now }
            }
            , {
                '$inc': { 'queued': len(objects) }
            }
            , sort = [ ('queued', 1) ]
            , new = True
        )
        if r is None:
            if self._createHandler(key):
                # Let it spin up
                time.sleep(1.0)
            else:
                time.sleep(0.1)
            return 0

        count = r['batchSize']
        with self._getAndLockProxy(r['uri']) as p:
            p.sendObjects(key, objects[:count])
        return count

