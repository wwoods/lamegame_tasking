
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
    def dump(self, objs):
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


    def run(self):
        """Dump!"""
        e = time.time() + TEST_TIME + 5.0
        # Dump values from 1 to 10, but cube them.  This way, if messages are
        # routinely dropped, the overall average will change dramatically
        tsent = 0
        while time.time() < e:
            pkg = self.getObjects()
            self.dump(pkg)
            tsent += len(pkg)
        #print("TOTAL SENT: {0}".format(tsent))
        self.taskConnection.createTask(self.__class__.__name__)


class DumpTaskMongo(DumpTaskBase):
    def dump(self, objs):
        self.taskConnection._database['test2'].insert({ 'objects': objs })


class DumpTaskRedis(DumpTaskBase):
    def __init__(self, *args, **kwargs):
        DumpTaskBase.__init__(self, *args, **kwargs)
        import redis
        self._redis = redis.StrictRedis(db=0)
        
    def dump(self, objs):
        self._redis.lpush('queue', pickle.dumps(objs))


class DumpTaskTalk(DumpTaskBase):
    def __init__(self, *args, **kwargs):
        DumpTaskBase.__init__(self, *args, **kwargs)
        self.tc = self.taskConnection.getTalk()


    def dump(self, objs):
        self.tc.send('talkTest', objs, timeout = 15.0, noRaiseOnTimeout=True)


class ReadTaskBase(lgTask.Task):
    def getObjs(self):
        raise NotImplementedError()


    def run(self):
        """Read!"""
        e = time.time() + TEST_TIME + 5.0
        tr = 0
        while time.time() < e:
            getStart = time.time()
            try:
                objs = self.getObjs()
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
    def getObjs(self):
        cc = self.taskConnection._database['test2']
        objs = cc.find_and_modify(remove = True)
        if objs is None:
            return []
        return objs['objects']


class ReadTaskRedis(ReadTaskBase):
    def __init__(self, *args, **kwargs):
        ReadTaskBase.__init__(self, *args, **kwargs)
        import redis
        self._redis = redis.StrictRedis(db=0)


    def getObjs(self):
        o = self._redis.rpop('queue')
        if o is None:
            return []
        o = pickle.loads(o)
        return o


class ReadTaskTalk(ReadTaskBase):
    def __init__(self, *args, **kwargs):
        ReadTaskBase.__init__(self, *args, **kwargs)
        self.tc = self.taskConnection.getTalk()


    def getObjs(self):
        objs = self.tc.recv('talkTest', batchSize = 200, timeout = 5.0)
        return objs


class ReadTaskFastest(ReadTaskBase):
    def getObjs(self):
        return DumpTaskBase.getObjects()


def doTest():
    x = 4
    y = 4
    print("""Benchmarking # of messages through pipe with {0} pushing 
            tasks and {1} pulling tasks with object sizes of 0.5 KB spread 
            over 3 keys and batch size of 200 objects""".format(x, y))
    c = createAndChdir([ 'talk' ], threaded = True)

    cc = c._database['test']
    #cc.save({ '_id': 'count', 'value': 0, 'count': 0 })

    testType = 'talk'

    if testType == 'talk':
        # 16000 /s  
        ##OLD, PUSH BEHAVIOR: 2570 /s avg (before opt, needs run again)
        for _ in range(x):
            c.createTask("DumpTaskTalk")
        for _ in range(y):
            c.createTask("ReadTaskTalk")
    elif testType == 'redis':
        # 3500 /s avg
        import redis
        r = redis.StrictRedis(db=0)
        r.delete('queue')
        r.connection_pool.disconnect()
        for _ in range(x):
            c.createTask("DumpTaskRedis")
        for _ in range(y):
            c.createTask("ReadTaskRedis")
    elif testType == 'mongo':
        # 2000 / s avg...
        for _ in range(x):
            c.createTask("DumpTaskMongo")
        for _ in range(y):
            c.createTask("ReadTaskMongo")
    elif testType == 'speed':
        # 400173 /s avg
        for _ in range(y):
            c.createTask("ReadTaskFastest")

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

