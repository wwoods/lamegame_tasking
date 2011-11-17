"""Tests that cover material in the basic usage of the README
"""

import os
import pymongo
import time
from unittest import TestCase

import lgTask
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
            { 'state': { '$ne': 'success' } }        
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
            self.conn.createTask("IncValueTask", runAt=TimeInterval('1 second')
                , db=db, id='a')
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
        self.assertEqual('test', p.taskName)

    def test_processorMaxTasks(self):
        p = lgTask.Processor(self.conf, taskName="test_processorMaxTasks")
        p.start()

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

