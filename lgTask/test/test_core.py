"""Tests that cover material in the basic usage of the README
"""

import datetime
import os
import pymongo
import shutil
import time
from unittest import TestCase, skip

import lgTask
from lgTask.errors import *
from lgTask.lib.reprconf import Config
from lgTask.lib.timeInterval import TimeInterval

class TestCore(TestCase):
    def setUp(self):
        self.oldpath = os.getcwd()
        os.chdir(os.path.abspath(
            os.path.join(__file__, '../testProcessor')
        ))

        try:
            pass#shutil.rmtree('logs')
        except OSError:
            pass
        try:
            pass#shutil.rmtree('pids')
        except OSError:
            pass
        try:
            os.remove('processor.lock')
        except OSError:
            pass

        self.conf = Config('processor.cfg')
        self.conn = lgTask.Connection(self.conf['processor']['taskDatabase'])
        self.conn._database.drop_collection('test')
        self.conn._database.drop_collection(self.conn.TASK_COLLECTION)
        self.conn._database.drop_collection(self.conn.SCHEDULE_COLLECTION)

    def tearDown(self):
        os.chdir(self.oldpath)

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

    def test_batchKwargs(self):
        # See if scheduling two batches with the same name but different
        # kwargs raises a TaskKwargError
        self.conn.batchTask('1 second', 'IncValueTask', id='a')
        try:
            self.conn.batchTask('1 second', 'IncValueTask', id='b')
            self.fail("Different kwargs did not raise TaskKwargError")
        except TaskKwargError:
            pass
    
            time.sleep(0.2)
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(4, d['value'])
            time.sleep(0.1) 
            # Shouldn't have changed, if we respected batch delay
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(4, d['value'])
            time.sleep(0.2)
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(5, d['value'])
        finally:
            p.stop()

    def test_batchKwargs(self):
        # See if scheduling two batches with the same name but different
        # kwargs raises a TaskKwargError
        self.conn.batchTask('1 second', 'IncValueTask', taskName='h', id='a')
        try:
            self.conn.batchTask('1 second', 'IncValueTask', taskName='h'
                , id='b')
            self.fail("Different kwargs did not raise TaskKwargError")
        except TaskKwargError:
            pass

    def test_batchKwargsName(self):
        # See if scheduling two different batches but not specifying a name is
        # OK since the name should be derived from kwargs
        self.conn.batchTask('1 second', 'IncValueTask', id='a')
        self.conn.batchTask('1 second', 'IncValueTask', id='b')

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
        self.assertEqual("Changed from 6 to 7", doc['lastLog'])

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

            # At this point, we should have run 2 tasks and have a 3rd queued
            taskDb = self.conn._database[self.conn.TASK_COLLECTION]
            spec = { 'taskClass': { '$ne': 'ScheduleAuditTask' } }
            self.assertEqual(3, taskDb.find(spec).count())

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

    def test_intervalTask_kwargs(self):
        # Two unnamed tasks with the same kwargs but diff intervals should
        # raise an error.
        self.conn.intervalTask('1 second', 'IncValueTask')
        try:
            self.conn.intervalTask('2 seconds', 'IncValueTask')
            self.fail(
                "Did not raise TaskKwargError on diff interval for unnamed"\
            )
        except TaskKwargError:
            pass

        self.conn.intervalTask('1 second', 'IncValueTask', taskName='a')
        try:
            self.conn.intervalTask('2 seconds', 'IncValueTask', taskName='a')
            self.fail('Did not raise TaskKwargError on diff interval')
        except TaskKwargError:
            pass
        try:
            self.conn.intervalTask('1 second', 'IncValueTask', taskName='a'
                , a='a')
            self.fail('Did not raise TaskKwargError on diff kwargs')
        except TaskKwargError:
            pass
        try:
            self.conn.intervalTask('1 second', 'IncValueTask', taskName='a'
                , fromStart=True)
            self.fail('Did not raise TaskKwargError on diff fromStart')
        except TaskKwargError:
            pass
        try:
            self.conn.intervalTask('1 second', 'IncValueTask', taskName='a')
        except TaskKwargError:
            self.fail('Did raise TaskKwargError on identical task')

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

    def test_scheduleAudit(self):
        taskDb = self.conn._database[self.conn.TASK_COLLECTION]

        from lgTask.scheduleAuditTask import ScheduleAuditTask
        audit = ScheduleAuditTask(self.conn)
        self.conn.intervalTask('1 hour', 'IncValueTask')
        # Remove the task entry
        taskDb.remove({ '_id': 'IncValueTask' })
        etime = datetime.datetime.utcnow() + TimeInterval('1 second ago')
        audit.run()

        # Ensure that the task entry now exists, one hour from now
        d = taskDb.find_one({ '_id': 'IncValueTask' })
        self.assertNotEqual(None, d)

        self.assertTrue(etime < d['tsRequest'], 'Interval time not respected')
        ftime = etime + TimeInterval('2 seconds')
        self.assertTrue(d['tsRequest'] < ftime, 'Interval time not respected')
    
    def test_processorOnlyOneInstance(self):
        p = lgTask.Processor()
        p.start()
        try:
            p2 = lgTask.Processor()
            p2._stopOnNoTasks = True
            p2.run()
            self.fail("Two processors were running in same dir")
        except lgTask.errors.ProcessorAlreadyRunningError:
            pass
        finally:
            p.stop()

    @skip("Max tasks not currently implemented")
    def test_processorMaxTasks(self):
        p = lgTask.Processor()
        p.start()

        try:
            db = self.conn._database['test']
            db.insert({ 'id': 'a', 'value': 6 })
            self.conn.createTask("IncValueTask", db=db, id='a', delay=0.5)
            self.conn.createTask("IncValueTask", db=db, id='a', delay=0.5)

            time.sleep(0.1)
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(6, d['value'])
            time.sleep(0.5)
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(7, d['value'])
            time.sleep(0.2)
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(7, d['value'])
            time.sleep(0.3)
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(8, d['value'])

            # Now test with 2
            p.MAX_TASKS = 2
            self.conn.createTask("IncValueTask", db=db, id='a', delay=0.5)
            self.conn.createTask("IncValueTask", db=db, id='a', delay=0.5)
            time.sleep(0.6)
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(10, d['value'])
        finally:
            p.stop()

