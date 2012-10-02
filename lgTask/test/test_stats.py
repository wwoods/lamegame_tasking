
import datetime
import os
import pymongo
from unittest import TestCase

import lgTask
from lgTask.lib.reprconf import Config

class TestStats(TestCase):
    def setUp(self):
        confPath = os.path.abspath(os.path.join(__file__, '../testProcessor'))
        self.conf = Config(confPath + '/processor.cfg')
        self.conn = lgTask.Connection(self.conf['processor']['taskDatabase'])
        self.conn._database.drop_collection(self.conn.STAT_COLLECTION)
        self.stats = self.conn.stats
        self.now = datetime.datetime.utcnow()
        # Next timeslot...
        self.nmin = self.now + datetime.timedelta(seconds = 3.0*60)


    def test_basic(self):
        self.stats.addStat('test', 54, time = self.now)
        self.stats.addStat('test', 57, time = self.now)
        self.stats.addStat('test', 12, time = self.nmin)
        self.stats.addStat('test-total', 120, time = self.now)
        self.stats.addStat('test-total', 150, time = self.now)
        self.stats.addStat('test-total', 60, time = self.now)
        self.stats.addStat('test-total', 30, time = self.nmin)
        r = self.stats.getStat('test', start = self.now, stop = self.nmin)
        self.assertEqual(2, len(r['values']))
        self.assertEqual(111, r['values'][0])
        self.assertEqual(12, r['values'][1])
        r = self.stats.getStat('test-total', start = self.now
                , stop = self.nmin)
        self.assertEqual(60, r['values'][0])
        self.assertEqual(30, r['values'][1])


    def test_noDataForAwhile(self):
        # This timestamp always has the same base value
        self.now = datetime.datetime(2012, 10, 2, 7, 31, 46, 21662)
        firstVal = 43

        old = self.now - datetime.timedelta(seconds = 50*60)
        self.stats._tryNewStat(self.stats._getSchemaFor('test'), 'test',
                self.stats._getTimeVal(old), randomize = True)
        # Since the collection is marked as having latest data from old, if
        # we get stats from then on, we should get a bunch of zeroes.
        r = self.stats.getStat('test', start = old, stop = self.now)
        for v in r['values'][1:]:
            self.assertEqual(0, v)
        self.assertEqual(firstVal, r['values'][0])

        # Now write and see what happens - ensure that the old value wasn't
        # overwritten, the in-betweens were, and the new value is ok
        self.stats.addStat('test', 1000, time = self.now)
        r = self.stats.getStat('test', start = old, stop = self.now)
        for v in r['values'][1:-1]:
            self.assertEqual(0, v)
        self.assertEqual(firstVal, r['values'][0])
        self.assertEqual(1000, r['values'][-1])


