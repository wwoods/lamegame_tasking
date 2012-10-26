"""Tests that cover material in the README aside from the actual running of
tasks.

In other words, if it actually runs a task, it shouldn't be in here
"""

import datetime
import os
import pymongo
import shutil
import time
import traceback
from unittest import TestCase, skip

import lgTask
from lgTask.errors import *
from lgTask.lib.reprconf import Config
from lgTask.lib.timeInterval import TimeInterval
from lgTask.test.processorTestCase import ProcessorTestCase

class TestCore(ProcessorTestCase):
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

    def test_processorCleanup(self):
        # Ensure that a processor will clean up old logs properly
        c = self.conn._database[self.conn.TASK_COLLECTION]
        c.insert({ '_id': 'fakeExists', 'state': 'success' })
        c.insert({ '_id': 'fakeWorking', 'state': 'working' })
        p = lgTask.Processor()
        # fs has min 1 second precision
        p.KEEP_LOGS_FOR = 1.0
        try:
            shutil.rmtree("logsCleanup")
        except OSError:
            pass
        os.mkdir("logsCleanup")
        p._LOG_DIR = "logsCleanup/"

        open('logsCleanup/fakeNoTask.log', 'w').close()
        open('logsCleanup/fakeExists.log', 'w').close()
        open('logsCleanup/fakeWorking.log', 'w').close()
        time.sleep(1.0)
        open('logsCleanup/fakeNew.log', 'w').close()
        p._cleanupThreadTarget()
        filesLeft = os.listdir('logsCleanup')
        filesLeft.sort()
        self.assertEqual(
            [ 'fakeNew.log', 'fakeWorking.log' ]
            , filesLeft
        )
        shutil.rmtree('logsCleanup')

    def test_consumeError(self):
        # Test that an error in processor consume does not block the processor.
        # This is a regression test
        crashCount = [ 0 ]
        def crash():
            crashCount[0] += 1
            raise Exception("crash()")
        p = lgTask.Processor()
        c = p._consume
        p._consume = crash

        db = self.conn._database['test']
        db.insert({ 'id': 'a', 'value': 99 })

        p.start()
        time.sleep(0.1)
        p.stop()

        self.assertTrue(crashCount[0] > 1, '_consume() called at most once')

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

    def test_processorKillExisting(self):
        # Fork a test processor, and verify that passing killExisting kills it.
        pkill = lgTask.Processor.fork(self.newpath)
        time.sleep(1)
        p = lgTask.Processor()
        try:
            p._stopOnNoTasks = True
            p.run(killExisting=True)
        except lgTask.errors.ProcessorAlreadyRunningError:
            self.fail("Failed to kill fork")
        finally:
            pkill()

    def test_processorForkNotDefunct(self):
        # Run a forked processor, kill it, and make sure that the processor
        # lock has been released appropriately (a new processor can spawn).
        pkill = lgTask.Processor.fork(self.newpath)
        time.sleep(1)
        # Try to get a lock, ensure it's running and got lock
        p = lgTask.Processor()
        try:
            p._stopOnNoTasks = True
            p.run()
            self.fail("fork either failed or did not get lock")
        except lgTask.errors.ProcessorAlreadyRunningError:
            pass
        pkill()

        p = lgTask.Processor()
        p._stopOnNoTasks = True
        # If this raises ProcessorAlreadyRunningError, then the fork probably
        # didn't release its lock on termination.
        p.run()

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


