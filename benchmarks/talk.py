
import cPickle as pickle
import os
import random
import shutil
import sys
import threading
import time

sys.path.insert(0, '.')

import lgTask
from benchmarks.common import createAndChdir, runProcessor

TEST_TIME = 15.0

class DumpTaskBase(lgTask.Task):
    def dump(self, key, objs):
        raise NotImplementedError()


    @classmethod
    def getObjects(cls):
        """Return object array for sending"""
        if hasattr(cls, '_pkg'):
            return cls._pkg
        val = random.random() * 10.0
        val = val ** 3
        pkg = []
        t = time.time()
        for _ in xrange(200):
            o = { 'value': val
                    , 'extraBytes': random.sample(xrange(10000000), 128)
                    , 'tsInserted': t
            }
            pkg.append(o)
        cls._pkg = pkg
        return pkg


    def run(self, key):
        """Dump!"""
        e = time.time() + TEST_TIME + 5.0
        # Dump values from 1 to 10, but cube them.  This way, if messages are
        # routinely dropped, the overall average will change dramatically
        tsent = 0
        while time.time() < e:
            pkg = self.getObjects()
            self.dump(key, pkg)
            tsent += len(pkg)
        #print("TOTAL SENT: {0}".format(tsent))
        self.taskConnection.createTask(self.__class__.__name__)


class DumpTaskMongo(DumpTaskBase):
    def dump(self, key, objs):
        self.taskConnection._database['test2'].insert({ 'objects': objs })


class DumpTaskRedis(DumpTaskBase):
    def run(self, key):
        port = 6379
        # I tried redis with 2 instances, no faster.
        #if key == '2':
        #    port = 8888
        import redis
        self._redis = redis.StrictRedis(port=port, db=0)
        DumpTaskBase.run(self, key)
        
    def dump(self, key, objs):
        self._redis.lpush(key, pickle.dumps(objs))


class DumpTaskTalk(DumpTaskBase):
    def __init__(self, *args, **kwargs):
        DumpTaskBase.__init__(self, *args, **kwargs)
        self.tc = self.taskConnection.getTalk()


    def dump(self, key, objs):
        self.tc.send(key, objs, timeout = 15.0, noRaiseOnTimeout=True)


class ReadTaskBase(lgTask.Task):
    def getObjs(self, key):
        raise NotImplementedError()


    def run(self, key):
        """Read!"""
        e = time.time() + TEST_TIME + 5.0
        tr = 0
        while time.time() < e:
            getStart = time.time()
            try:
                objs = self.getObjs(key)
            except lgTask.talk.TalkTimeoutError:
                continue
            getEnd = time.time()
            tr += len(objs)
            totalVal = 0.0
            totalCount = 0
            #lag = time.time() - objs[0]['tsInserted']
            lag = getEnd - getStart
            for o in objs:
                totalVal += o['value']
                totalCount += 1
            self.taskConnection._database['test'].insert(
                { 'value': totalVal, 'count': totalCount, 'latency': lag }
            )
        #print("TOTAL RECV'D {0}".format(tr))
        self.taskConnection.createTask(self.__class__.__name__)


class ReadTaskMongo(ReadTaskBase):
    def getObjs(self, key):
        cc = self.taskConnection._database['test2']
        objs = cc.find_and_modify(remove = True)
        if objs is None:
            return []
        return objs['objects']


class ReadTaskRedis(ReadTaskBase):
    def run(self, key):
        port = 6379
        #if key == '2':
        #    port = 8888
        import redis
        self._redis = redis.StrictRedis(port=port, db=0)
        ReadTaskBase.run(self, key)


    def getObjs(self, key):
        o = self._redis.rpop(key)
        if o is None:
            return []
        o = pickle.loads(o)
        return o


class ReadTaskTalk(ReadTaskBase):
    def __init__(self, *args, **kwargs):
        ReadTaskBase.__init__(self, *args, **kwargs)
        self.tc = self.taskConnection.getTalk()


    def getObjs(self, key):
        objs = self.tc.recv(key, batchSize = 200, timeout = 5.0)
        return objs


class ReadTaskFastest(ReadTaskBase):
    def getObjs(self):
        return DumpTaskBase.getObjects()


def doTest():
    x = 6
    y = 6
    print("""Benchmarking # of messages through pipe with {0} pushing 
            tasks and {1} pulling tasks with object sizes of 0.5 KB spread 
            over 3 keys and batch size of 200 objects""".format(x, y))
    c = createAndChdir([ 'talk' ], threaded = True)

    cc = c._database['test']
    #cc.save({ '_id': 'count', 'value': 0, 'count': 0 })

    testType = 'talk'
    keys = [ '1', '2', '3' ]
    def getKey(index):
        return keys[index % len(keys)]

    if testType == 'talk':
        # 16000 /s  
        ##OLD, PUSH BEHAVIOR: 2570 /s avg (before opt, needs run again)
        for _ in range(x):
            c.createTask("DumpTaskTalk", key = getKey(x))
        for _ in range(y):
            c.createTask("ReadTaskTalk", key = getKey(y))
    elif testType == 'redis':
        # 3500 /s avg
        import redis
        r = redis.StrictRedis(db=0)
        for k in keys:
            r.delete(k)
        r.connection_pool.disconnect()
        for _ in range(x):
            c.createTask("DumpTaskRedis", key=getKey(x))
        for _ in range(y):
            c.createTask("ReadTaskRedis", key=getKey(y))
    elif testType == 'mongo':
        # 2000 / s avg...
        for _ in range(x):
            c.createTask("DumpTaskMongo", key=getKey(x))
        for _ in range(y):
            c.createTask("ReadTaskMongo", key=getKey(y))
    elif testType == 'speed':
        # 400173 /s avg
        for _ in range(y):
            c.createTask("ReadTaskFastest", key=getKey(y))

    a = time.time()
    runProcessor(TEST_TIME)
    b = time.time()
    print('=' * 80)

    totalCount = 0
    totalVal = 0.0
    totalLag = 0.0
    for doc in cc.find():
        totalCount += doc['count']
        totalVal += doc['value']
        totalLag += doc['latency'] * doc['count']
    totalLag /= totalCount
    print("{0} / {1:.2f} s avg lag".format(totalCount, totalLag))
    print("Avg {0}".format(totalCount / (b - a)))


if __name__ == '__main__':
    doTest()

