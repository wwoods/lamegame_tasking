"""Tests that cover running a task within a processor.
"""

import datetime
import os
import pymongo
import shutil
import time
import traceback
from unittest import skip

import lgTask
from lgTask.errors import *
from lgTask.lib.reprconf import Config
from lgTask.lib.timeInterval import TimeInterval
from lgTask.test.processorTestCase import ProcessorTestCase

class TestRunning(ProcessorTestCase):
    EXTRA_CONFIG = ""
    EXTRA_CONFIG_doc = """Extra lines in processor.cfg for test"""

    def setUp(self):
        """Write our processor and processor2 cfgs
        """
        ProcessorTestCase.setUp(self)
        # We're now in the testProcessor cfg
        with open('processor.cfg', 'w') as f:
            f.write("""[processor]
{0}
taskDatabase = "pymongo://localhost/test_lgTask"
pythonPath = [ "./tasks2" ]
""".format(self.EXTRA_CONFIG))
        with open('../testProcessor2/processor.cfg', 'w') as f:
            f.write("""[processor]
{0}
taskDatabase = "pymongo://localhost/test_lgTask"
""".format(self.EXTRA_CONFIG))


    def test_batch(self):
        p = lgTask.Processor()
        p.start()
        try:
            db = self.conn._database['test']
            db.insert({ 'id': 'c', 'value': 0 })
            self.conn.batchTask('0.5 seconds', 'IncValueTask', db=db, id='c')
            self.conn.batchTask('0.5 seconds', 'IncValueTask', db=db, id='c')
            self.conn.batchTask('0.5 seconds', 'IncValueTask', db=db, id='c')
            time.sleep(0.75)
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(1, d['value'])
            self.conn.batchTask('0.1 seconds', 'IncValueTask', db=db, id='c')
            time.sleep(0.5)
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(2, d['value'])
            # Test queueing
            # Start an IncValue, while it's running batch another
            self.conn.batchTask('0.05 seconds', 'IncValueTask', db=db, id='c'
                , delay=0.5
            )
            time.sleep(0.25) #0.25 - in IncValueTask
            self.conn.batchTask('1.0 seconds', 'IncValueTask', db=db, id='c'
                , delay=0.5 # We don't want a delay, but want same kwargs
            )
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(2, d['value'])
            time.sleep(0.5) #0.75 - First finished
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(3, d['value'])
            time.sleep(0.75) #1.5 - Second should be in delay
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(3, d['value'])
            time.sleep(0.5) #2.0 - second should have effected
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(4, d['value'])
        finally:
            p.stop()

    def test_batchHistory(self):
        bTid = self.conn.batchTask('now', 'IncValueTask', taskName='j', id='c')
        startTime = datetime.datetime.utcnow()
        p = lgTask.Processor()
        p.start()
        try:
            time.sleep(0.5)
            taskDb = self.conn.database[self.conn.TASK_COLLECTION]
            tasks = { t['_id']: t for t in taskDb.find(
                { 'taskClass': 'IncValueTask' }
            ) }
            # Should be a main task, and a splinter task
            for tid in tasks.iterkeys():
                if tid != bTid:
                    splinterId = tid
                    break
            task = tasks[splinterId]
            self.assertNotEqual('IncValueTask-j', splinterId)
            self.assertTrue(task['tsStop'] >= startTime, "Wrong task")
            self.assertTrue(os.path.isfile('logs/{0}.log'.format(splinterId)))
            self.assertFalse(os.path.isfile('logs/{0}.log'.format(bTid)))
        finally:
            p.stop()

    def test_batchRunOnError(self):
        # See if a previously failed batch task allows a new one to run.
        taskDb = self.conn.database[self.conn.TASK_COLLECTION]
        taskDb.insert({
            '_id': 'IncValueTask-k'
            ,'taskClass': 'IncValueTask'
            ,'state': 'success'
            ,'kwargs': { 'id': 'a' }
        })
        self.conn.batchTask('1 second', 'IncValueTask', id='a', taskName='k')
        newTask = taskDb.find_one({ '_id': 'IncValueTask-k' })
        self.assertEqual('request', newTask['state'])

    def test_consumeOne(self):
        db = self.conn._database['test']
        db.insert({ 'id': 'a', 'value': 6 })
        db.insert({ 'id': 'b', 'value': 6 })
        self.conn.createTask("IncValueTask", db=db, id='a')
        p = lgTask.Processor()
        p.start()
        p.stop()
        
        self.assertEqual(7, db.find_one({ 'id': 'a' })['value'])
        self.assertEqual(6, db.find_one({ 'id': 'b' })['value'])
        
        # We shouldn't be able to consume again
        self.assertFalse(p._consume())
        
        # Also assert that all tasks in the db have 'success' status
        nonSuccess = self.conn._database[self.conn.TASK_COLLECTION].find(
            { 
                'state': { '$ne': 'success' }
                , 'taskClass': { '$ne': 'ScheduleAuditTask' } 
            }
        )
        self.assertEqual(0, nonSuccess.count())
        
        # And make sure last log works...
        doc = self.conn._database[self.conn.TASK_COLLECTION].find_one(
            { 'state': 'success' }        
        )
        self.assertEqual("Changed a from 6 to 7", doc['lastLog'])

    def test_consumeOtherPath(self):
        db = self.conn._database['test']
        db.insert({ 'id': 'a', 'value': 3 })
        self.conn.createTask("IncValueTask2", db=db, id='a')
        p = lgTask.Processor(home="../testProcessor2")
        p.start()
        p.stop()

        self.assertEqual(4, db.find_one({ 'id': 'a' })['value'])
        
    def test_consumeDelayed(self):
        db = self.conn._database['test']
        db.insert({ 'id': 'a', 'value': 6 })
        p = lgTask.Processor()
        p.start()
        try:
            self.conn.delayedTask(
                '1 second', "IncValueTask"
                , db=db, id='a'
            )
            time.sleep(0.5)
            doc = db.find_one({ 'id': 'a' })
            self.assertEqual(6, doc['value'])
            time.sleep(0.3)
            doc = db.find_one({ 'id': 'a' })
            self.assertEqual(6, doc['value'])
            time.sleep(0.5)
            doc = db.find_one({ 'id': 'a' })
            self.assertEqual(7, doc['value'])
        finally:
            p.stop()

    def test_intervalTask(self):
        db = self.conn._database['test']
        p = lgTask.Processor()
        p.start()
        try:
            db.insert({ 'id': 'a', 'value': 0 })
            self.conn.intervalTask('1.0 seconds', 'IncValueTask', db=db, id='a'
                , delay=1.0
            )

            # Expected time frame:
            # 0 - 1st task launched
            # 1 - 1st task does operation and exits, scheduling 2nd
            # 2 - 2nd task launched
            # 3 - 2nd task does operation..
            # 4 - 3rd task launched

            #0.0
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(0, d['value'])

            time.sleep(0.5) #0.5
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(0, d['value'])

            time.sleep(1.0) #1.5
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(1.0) #2.5
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(1.0) #3.5
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(2, d['value'])

            time.sleep(1.0) #4.5
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(2, d['value'])

            # At this point, we should have run 2 tasks and have a 3rd in its
            # sleep; remember that the non-splintered task takes up a slot too
            taskDb = self.conn._database[self.conn.TASK_COLLECTION]
            spec = { 'taskClass': { '$ne': 'ScheduleAuditTask' } }
            tasks = taskDb.find(spec)
            if 4 != tasks.count():
                for t in tasks:
                    print("Found: {0}".format(t['taskClass']))
                self.fail("Expected 2 tasks executed and a 3rd queued")
        finally:
            p.stop()


    def test_intervalTask_fromStart(self):
        db = self.conn._database['test']
        p = lgTask.Processor()
        p.start()
        try:
            db.insert({ 'id': 'a', 'value': 0 })
            self.conn.intervalTask('1.0 seconds', 'IncValueTask', fromStart=True
                , db=db, id='a', delay=0.5
            )

            #0.0
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(0, d['value'])

            time.sleep(1.0) #1.0 - first iteration completed at 0.5
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(0.75) #1.75 - second should have started at 1.0
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(2, d['value'])

        finally:
            p.stop()

    def test_killTask(self):
        taskDb = self.conn._database[self.conn.TASK_COLLECTION]
        db = self.conn._database['test']
        id = self.conn.createTask("IncValueTask", db=db, id="a", delay = 4.0)

        p = lgTask.Processor()
        p.KILL_INTERVAL = 0.1
        p.start()
        try:
            self.assertWaitFor(
                lambda: taskDb.find_one({ '_id': id, 'state': 'working' })
                , 1.0
                , "Failed to work"
            )
            self.conn.killTask(id)
            self.assertWaitFor(
                lambda: taskDb.find_one({ '_id': id, 'state': 'error' })
                , 8.0
                , "Failed to kill"
            )
            lastLog = taskDb.find_one(id)['lastLog']
            if 'KillTaskError' not in lastLog:
                print('== LAST LOG ==')
                print(lastLog)
                self.fail("KillTaskError not in lastLog")
        finally:
            p.stop()


    def test_retryTask(self):
        taskDb = self.conn._database[self.conn.TASK_COLLECTION]

        p = lgTask.Processor()
        p.start()
        try:
            self.conn.createTask('FailTask', maxRetries=2)
            time.sleep(0.5) # First one failed
            self.assertEqual(0, taskDb.find({ 'state': 'error' }).count())
            self.assertEqual(1, taskDb.find({ 'state': 'retried' }).count())
            time.sleep(1) #Second failed
            self.assertEqual(0, taskDb.find({ 'state': 'error' }).count())
            self.assertEqual(2, taskDb.find({ 'state': 'retried' }).count())
            time.sleep(1) #Third failure, not retried
            self.assertEqual(1, taskDb.find({ 'state': 'error' }).count())
            self.assertEqual(2, taskDb.find({ 'state': 'retried' }).count())
            self.assertEqual(
                0
                , taskDb
                    .find({ 'state': { '$nin': [ 'error', 'retried' ] } })
                    .count()
            )
        finally:
            p.stop()

    def test_taskStopped_multiple(self):
        # Ensure that taskStopped being called multiple times only effects
        # the first one
        c = self.conn._database[self.conn.TASK_COLLECTION]
        tid = self.conn.createTask("IncValueTask")
        taskData = c.find_one(tid)

        self.conn.taskStopped(tid, taskData, False, 'First error')
        d1 = c.find_one(tid)
        self.assertEqual('error', d1['state'])
        self.assertEqual('First error', d1['lastLog'])
        self.conn.taskStopped(tid, taskData, False, 'Second error')
        d2 = c.find_one(tid)
        self.assertEqual('error', d2['state'])
        self.conn.taskStopped(tid, taskData, True, 'OK!')
        d3 = c.find_one(tid)
        self.assertEqual('error', d3['state'])
        self.assertEqual('First error', d3['lastLog'])


