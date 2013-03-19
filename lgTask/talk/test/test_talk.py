
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
        # Put it in an array to get around python thinking it should be
        # bound to this class
        cls.processor = lgTask.Processor(home = ph)
        cls.processor.start()


    @classmethod
    def tearDownClass(cls):
        cls.processor.stop()


    def setUp(self):
        self.tc = lgTask.Connection(self.MONGODB_URL)
        self.tc._database.drop_collection(self.tc.TASK_COLLECTION)
        self.talk = self.tc.getTalk()
        self.tc._database.drop_collection(self.talk.TALK_SENDER_COLLECTION)


    def test_talk(self):
        recvKey = uuid.uuid4().hex
        self.tc.createTask('RouterTask')
        #self.talk.registerHandler('toRoute', 'RouterTask', max = 2)
        try:
            self.talk.send('toRoute', [ [ 'a', recvKey ], [ 'b', recvKey ] ],
                    timeout = 5.0)
        finally:
            print(list(self.talk._colSender.find()))
            print(list(self.tc._database[self.tc.TASK_COLLECTION].find()))
        r = self.talk.recv(recvKey, batchSize = 2, timeout = 5.0)
        print("GOT: {0}".format(r))
        self.assertEqual('a', r[0])
        self.assertEqual('b', r[1])

        # Round 2!
        self.talk.send('toRoute', [ [ 'a', recvKey ], [ 'b', recvKey ] ] * 900,
                timeout = 10.0)
        for _ in range(9):
            r = self.talk.recv(recvKey, batchSize = 200, timeout = 5.0)
            self.assertEqual([ 'a', 'b' ] * 100, r)


    def test_talkBufferFailure(self):
        tid = self.tc.createTask('RouterBufferFailureTask')
        while self.tc.getTask(tid)['state'] not in self.tc.states.DONE_GROUP:
            time.sleep(0.01)
        log = open(self.processor.getPath("logs/{}.log".format(tid))).read()
        self.assertIn(
                "TalkTimeoutError: 1 buffered sends did not send 2 objects",
                log)


    def test_mappingTask(self):
        self.tc.createTask('Calculator')
        self.tc.createTask('Calculator')
        self.tc.createTask('Calculator')
        self.tc.createTask('Calculator')
        # Be sure to test with a larger number than Calculator.BATCH_SIZE
        input = [ '3+5', '3**5', '1/2.0' ] * 3
        r = self.talk.map('calc', input, timeout = 5.0)
        expected = [ 8, 3**5, 0.5 ] * 3
        self.assertEqual(expected, r)
        
        
    def test_getTaskLog(self):
        taskId = self.tc.createTask('LoggerTask')
        print("Started task " + taskId)
        while self.tc.getTask(taskId)['tsStop'] is None:
            time.sleep(0.1)

        logFirst = self.tc.talkGetTaskLog(taskId, 0)
        if len(logFirst) != 4*1024 or logFirst[0] != '{':
            print("Got: " + str(logFirst))
            self.fail("Log block was bad")
        
        log1 = self.tc.talkGetTaskLog(taskId, -1)
        if len(log1) != 4*1024:
            print("Got: " + str(log1))
            self.fail("Log block was bad")
        lines = [ l.split(' ', 1)[1] for l in log1.split('\n')
                if ' ' in l ]
        expected = [ 'Message ' + str(i) for i in range(898, 1000) ]
        expected += [ 'All done' ]
        self.assertEqual(expected, lines)
        
        # The task spits out 1000 even-length messages, so the block size
        # should be constant
        
        log0 = self.tc.talkGetTaskLog(taskId, -2)
        self.assertEqual(4*1024, len(log0))
        self.assertTrue('Message 839' in log0)
        
        logLast = self.tc.talkGetTaskLog(taskId, -10)
        self.assertGreater(4*1024, len(logLast))
        self.assertEqual('{"', logLast[:2])
        self.assertTrue('Message 0' in logLast)
        
        logf = self.tc.talkGetTaskLog(taskId, -11)
        self.assertEqual('', logf)

