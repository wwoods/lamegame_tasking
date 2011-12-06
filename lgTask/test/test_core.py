"""Tests that cover material in the basic usage of the README
"""

import datetime
import os
import pymongo
import time
from unittest import TestCase

import lgTask
from lgTask.errors import *
from lgTask.lib.reprconf import Config
from lgTask.lib.timeInterval import TimeInterval

class TestCore(TestCase):
    def setUp(self):
        self.path = os.path.dirname(os.path.abspath(__file__))
        self.conf = Config(os.path.join(self.path, 'testProcessor.cfg'))
        lgp = self.conf['lgTaskProcessor']
        lgp['taskDir'] = lgp['taskDir'].replace("./", self.path + "/")
        self.conn = lgTask.Connection(lgp['taskDatabase'])
        self.conn._database.drop_collection('test')
        self.conn._database.drop_collection(self.conn.TASK_COLLECTION)
        self.conn._database.drop_collection(self.conn.SINGLETON_COLLECTION)
        self.conn._database.drop_collection(self.conn.SCHEDULE_COLLECTION)

    def test_batch(self):
        p = lgTask.Processor(self.conf)
        p.start()
        try:
            db = self.conn._database['test']
            db.insert({ 'id': 'c', 'value': 1 })
            self.conn.batchTask('0.1 seconds', 'IncValueTask', db=db, id='c')
            self.conn.batchTask('0.1 seconds', 'IncValueTask', db=db, id='c')
            self.conn.batchTask('0.1 seconds', 'IncValueTask', db=db, id='c')
            time.sleep(0.2)
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(2, d['value'])
            self.conn.batchTask('0.1 seconds', 'IncValueTask', db=db, id='c')
            time.sleep(0.2)
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(3, d['value'])
            # Test queueing
            # 0 - delay 0.05 inc value
            # 0.1 - IV1 is in delay but running, IV2 is batch queued
            # 0.2 - IV1 should still be in delay
            # 0.3 - IV1 changed value and exits
            # 0.4 - IV2 should still be in batch delay
            # 0.5 - IV2 should start running
            # 0.6 - IV2 should be in task delay
            # 0.7 - IV2 should finish task delay
            self.conn.batchTask('0.05 seconds', 'IncValueTask', db=db, id='c'
                , delay=0.2
            )
            time.sleep(0.1) #0.1
            self.conn.batchTask('0.4 seconds', 'IncValueTask', db=db, id='c'
                , delay=0.2 # We don't want a delay, but want same kwargs
            )
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(3, d['value'])
            time.sleep(0.1) #0.2
            # Shouldn't have changed, if we respected batch delay
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(3, d['value'])
            time.sleep(0.1) #0.3
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(4, d['value'])
            time.sleep(0.1) #0.4
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(4, d['value'])
            time.sleep(0.2) #0.6
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(4, d['value'])
            time.sleep(0.2) #0.8
            d = db.find_one({ 'id': 'c' })
            self.assertEqual(5, d['value'])
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
    
    def test_consumeOne(self):
        db = self.conn._database['test']
        db.insert({ 'id': 'a', 'value': 6 })
        db.insert({ 'id': 'b', 'value': 6 })
        self.conn.createTask("IncValueTask", db=db, id='a')
        p = lgTask.Processor(self.conf, taskName="test_consumeOne")
        p.start()
        p.stop(onNoTasksToConsume=True)
        
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
        
    def test_consumeDelayed(self):
        db = self.conn._database['test']
        db.insert({ 'id': 'a', 'value': 6 })
        p = lgTask.Processor(self.conf, taskName='test_consumeDelayed')
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
            time.sleep(0.3)
            doc = db.find_one({ 'id': 'a' })
            self.assertEqual(7, doc['value'])
        finally:
            p.stop()

    def test_intervalTask(self):
        db = self.conn._database['test']
        p = lgTask.Processor(self.conf)
        p.start()
        try:
            db.insert({ 'id': 'a', 'value': 0 })
            self.conn.intervalTask('0.2 seconds', 'IncValueTask', db=db, id='a'
                , delay=0.1
            )

            #0.0
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(0, d['value'])

            time.sleep(0.15) #0.15
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(0.2) #0.35
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(0.1) #0.45
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(2, d['value'])

            time.sleep(0.35) #0.80 (0.75 was failing sometimes)
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(3, d['value'])

            # At this point, we should have run 3 tasks and have a 4th queued.
            taskDb = self.conn._database[self.conn.TASK_COLLECTION]
            spec = { 'taskClass': { '$ne': 'ScheduleAuditTask' } }
            self.assertEqual(4, taskDb.find(spec).count())

        finally:
            p.stop()


    def test_intervalTask_fromStart(self):
        db = self.conn._database['test']
        p = lgTask.Processor(self.conf)
        p.start()
        try:
            db.insert({ 'id': 'a', 'value': 0 })
            self.conn.intervalTask('0.2 seconds', 'IncValueTask', fromStart=True
                , db=db, id='a', delay=0.1
            )

            #0.0
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(0, d['value'])

            time.sleep(0.15) #0.15
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(0.1) #0.25
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(1, d['value'])

            time.sleep(0.1) #0.35
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(2, d['value'])

            time.sleep(0.1) #0.45
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(2, d['value'])

            time.sleep(0.1) #0.55
            d = db.find_one({ 'id': 'a' })
            self.assertEqual(3, d['value'])

            time.sleep(0.2) #0.75

            # At this point, we should have run 4 tasks and have a 5th queued.
            taskDb = self.conn._database[self.conn.TASK_COLLECTION]
            spec = { 'taskClass': { '$ne': 'ScheduleAuditTask' } }
            self.assertEqual(5, taskDb.find(spec).count())

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
    
    def test_singletonAssert(self):
        p = lgTask.Processor(self.conf, taskName="test_singletonAssert")
        p.start()
        try:
            p2 = lgTask.Processor(self.conf, taskName="test_singletonAssert")
            p2.start()
            p2.stop()
            self.fail("Two singletons were running under same name.")
        except lgTask.errors.SingletonAlreadyRunningError:
            pass
        finally:
            p.stop()

    def test_singletonDiffKwargs(self):
        # Assert that a singleton trying to start during the run of the 
        # same singleton raises TaskKwargError rather than 
        # SingletonAlreadyRunningError (for added visibility at system 
        # instabilities; that is, we don't want to throw away data)
        p = lgTask.Processor(self.conf, taskName="test_singletonDiffKwargs")
        p2 = lgTask.Processor(self.conf, taskName="test_singletonDiffKwargs")
        # Emulate different kwargs
        p2._kwargsOriginal = { 'a': 'b' }
        p.start()
        try:
            p2.start()
            p2.stop()
            self.fail("Two singletons with different kwargs did not raise "
                + "TaskKwargError"
            )
        except lgTask.errors.TaskKwargError:
            pass
        finally:
            p.stop()

            
    def test_singletonHeartbeat(self):
        p = lgTask.Processor(self.conf, taskName='test_singletonHeartbeat')
        p.HEARTBEAT_INTERVAL = TimeInterval('0.1 seconds')
        p.start()
        try:
            time.sleep(0.3)
            p2 = lgTask.Processor(self.conf, taskName='test_singletonHeartbeat')
            p2.HEARTBEAT_INTERVAL = p.HEARTBEAT_INTERVAL
            try:
                p2.start()
                p2.stop()
                self.fail("Should have raised SingletonAlreadyRunningError")
            except lgTask.errors.SingletonAlreadyRunningError:
                pass
        finally:
            p.stop()
            
    def test_singletonReleaseOnStop(self):
        p = lgTask.Processor(self.conf, taskName="test_singletonRelease")
        p2 = lgTask.Processor(self.conf, taskName="test_singletonRelease")
        p.start()
        p.stop()
        p2.start()
        p2.stop()
        
    def test_singletonReleaseOnTimeout(self):
        p = lgTask.Processor(self.conf, taskName="test_singletonReleaseTime")
        p2 = lgTask.Processor(self.conf, taskName="test_singletonReleaseTime")
        p.HEARTBEAT_INTERVAL = TimeInterval('0.2 seconds')
        p2.HEARTBEAT_INTERVAL = p.HEARTBEAT_INTERVAL
        p.start()
        # Timeout while circumventing singleton release
        lgTask.Task.stop(p)
        p._stopHeartbeat()
        time.sleep(0.5)
        p2.start()
        p2.stop()

    def test_processorConfigTaskName(self):
        self.conf['lgTaskProcessor']['taskName'] = 'test'
        p = lgTask.Processor(self.conf)
        self.assertEqual('Processor-test', p.taskName)

    def test_processorMaxTasks(self):
        p = lgTask.Processor(self.conf, taskName="test_processorMaxTasks")
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

