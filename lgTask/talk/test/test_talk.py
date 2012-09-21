
import os
import shutil
import time
from unittest import TestCase
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
            f.write('threaded = True\n')
            f.write('taskDatabase = "{0}"\n'.format(cls.MONGODB_URL))
            f.write('pythonPath = [ "../" ]\n')
        cls.p = lgTask.Processor(home = ph)
        cls.p.start()


    @classmethod
    def tearDownClass(cls):
        cls.p.stop()


    def setUp(self):
        self.tc = lgTask.Connection(self.MONGODB_URL)
        self.tc._database.drop_collection(self.tc.TASK_COLLECTION)
        self.talk = self.tc.getTalk()
        self.tc._database.drop_collection(self.talk.TALK_RECV_COLLECTION)


    def test_talk(self):
        recvKey = uuid.uuid4().hex
        self.talk.registerHandler('toRoute', 'RouterTask', max = 2)
        self.talk.send('toRoute', [ [ 'a', recvKey ], [ 'b', recvKey ] ])
        r = self.talk.recv(recvKey, batchSize = 2)
        print("GOT: {0}".format(r))
        self.assertEqual('a', r[0])
        self.assertEqual('b', r[1])

        # Round 2!
        self.talk.send('toRoute', [ [ 'a', recvKey ], [ 'b', recvKey ] ] * 900)
        r = self.talk.recv(recvKey, batchSize = 2000)
        print("GOT: {0}".format(r))
        self.assertEqual([ 'a', 'b' ] * 900, r)


