
import pymongo
import shutil
import os
import time
from unittest import TestCase

import lgTask
from lgTask.lib.reprconf import Config

class ProcessorTestCase(TestCase):
    def setUp(self):
        self.oldpath = os.getcwd()
        self.newpath = os.path.abspath(
            os.path.join(__file__, '../testProcessor')
        )
        self._fudgePath()

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

        self.conn = lgTask.Connection('pymongo://localhost/test_lgTask')
        self.conn._database.drop_collection('test')
        self.conn._database.drop_collection(self.conn.TASK_COLLECTION)
        self.conn._database.drop_collection(self.conn.SCHEDULE_COLLECTION)

    def _restorePath(self):
        os.chdir(self.oldpath)

    def _fudgePath(self):
        os.chdir(self.newpath)

    def tearDown(self):
        self._restorePath()


    def assertWaitFor(self, condition, timeout = 0.1, msg = None):
        """Wait for the given condition function to return True for up to 
        timeout seconds"""
        n = time.time()
        success = False
        while not success and time.time() - n < timeout:
            success = condition()
        if not success:
            self.fail(msg)
    
