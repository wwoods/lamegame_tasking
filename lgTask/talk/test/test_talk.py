
import os
import shutil
import time
from unittest import TestCase, skip
import uuid

import lgTask

baseDir = os.path.dirname(os.path.abspath(__file__))

class TestTalk(TestCase):

    MONGODB_URL = "pymongo://localhost/testLamegameTasking"

    @classmethod
    def setUpClass(cls):
        # All tests need a Processor
        ph = os.path.join(baseDir, 'testProcessor')
        try:
            shutil.rmtree(ph)
        except OSError:
            # Didn't exist
            pass
        os.mkdir(ph)
        with open(os.path.join(ph, 'processor.cfg'), 'w') as f:
            f.write('[processor]\n')
            # A note about testing talk with threaded processors: since the
            # Processor.start() command launches a new thread, that means that
            # all of the global state will be shared with the executing 
            # processes for a threaded processor.  Since that means globals
            # like lgTask.talk._Server._servers will be maintained, this ruins
            # various test results (turn it on and see!)  So for now, it's
            # best left off..
            f.write('threaded = True\n')
            f.write('taskDatabase = "{0}"\n'.format(cls.MONGODB_URL))
            f.write('pythonPath = [ "../" ]\n')
        cls.p = lgTask.Processor(home = ph)
        cls.p.start()


    @classmethod
    def tearDownClass(cls):
        cls.p.stop(timeout = 0)


    def setUp(self):
        self.tc = lgTask.Connection(self.MONGODB_URL)
        self.tc._database.drop_collection(self.tc.TASK_COLLECTION)
        self.talk = self.tc.getTalk()
        self.tc._database.drop_collection(self.talk.TALK_SENDER_COLLECTION)


    def test_talk(self):
        recvKey = uuid.uuid4().hex
        self.tc.createTask('RouterTask')
        #self.talk.registerHandler('toRoute', 'RouterTask', max = 2)
        self.talk.send('toRoute', [ [ 'a', recvKey ], [ 'b', recvKey ] ]
                , timeout = 5.0)
        r = self.talk.recv(recvKey, batchSize = 2, timeout = 5.0)
        print("GOT: {0}".format(r))
        self.assertEqual('a', r[0])
        self.assertEqual('b', r[1])

        # Round 2!
        self.talk.send('toRoute', [ [ 'a', recvKey ], [ 'b', recvKey ] ] * 900
                , timeout = 10.0)
        for _ in range(9):
            r = self.talk.recv(recvKey, batchSize = 200, timeout = 5.0)
            self.assertEqual([ 'a', 'b' ] * 100, r)


    def test_mappingTask(self):
        self.tc.createTask('Calculator')
        self.tc.createTask('Calculator')
        self.tc.createTask('Calculator')
        self.tc.createTask('Calculator')
        # Be sure to test with a larger number than Calculator.BATCH_SIZE
        input = [ '3+5', '3**5', '1/2.0' ] * 3
        r = self.talk.map('calc', input, timeout = 5.0)
        expected = zip(input, [ 8, 3**5, 0.5 ]) * 3
        self.assertEqual(expected, r)


