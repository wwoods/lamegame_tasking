
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
        
        
    def getCountByBlock(self, statName):
        """Query the database in self.stats for the given stat, and return
        a dict of { int(layerName) : sum of all components in layer }
        
        Used to check data operations
        """
        r = self.stats._col.find_one(statName, fields = [ 'block' ])
        results = {}
        for layer, data in r['block'].iteritems():
            total = 0
            for d in data:
                total += d
            results[int(layer)] = total
        return results


    def test_basic(self):
        self.stats.addStat('test', 54, time = self.now)
        self.stats.addStat('test', 57, time = self.now)
        self.stats.addStat('test', 12, time = self.nmin)
        self.stats.addStat('test-sample', 120, time = self.now)
        self.stats.addStat('test-sample', 150, time = self.now)
        self.stats.addStat('test-sample', 60, time = self.now)
        self.stats.addStat('test-sample', 30, time = self.nmin)
        r = self.stats.getStat('test', start = self.now, stop = self.nmin)
        self.assertEqual(2, len(r['values']))
        self.assertEqual(111, r['values'][0])
        self.assertEqual(12, r['values'][1])
        r = self.stats.getStat('test-sample', start = self.now
                , stop = self.nmin)
        self.assertEqual(60, r['values'][0])
        self.assertEqual(30, r['values'][1])


    def test_series(self):
        self.now = datetime.datetime.utcfromtimestamp(
            self.stats._getBlocks(self.stats._getTimeVal(None), [(6*60*60,100)]
                )[0][3] + 1.5)
        v = [ 20 ]
        n = [ self.now ]
        expected = []
        expectedDaySet = []
        expectedDayAdd = []
        expectedDayAddVal = [0]
        def add(i):
            self.stats.addStat('test', v[0], time = n[0])
            # Add to the "set" value twice, to ensure it is setting and not
            # summing
            self.stats.addStat('test-sample', v[0], time = n[0])
            self.stats.addStat('test-sample', v[0], time = n[0])
            expected.append(v[0])
            expectedDayAddVal[0] += v[0]
            if i % 5 == 4:
                expectedDaySet.append(v[0])
                expectedDayAdd.append(expectedDayAddVal[0])
                expectedDayAddVal[0] = 0
            v[0] += 1
            n[0] += datetime.timedelta(seconds = 3 * 60.0)
        for i in range(20):
            add(i)
        r = self.stats.getStat('test', self.now, n[0])
        r2 = self.stats.getStat('test-sample', self.now, n[0])
        expected2 = expected[:]
        expected.append(0)
        # samples add None instead of 0
        expected2.append(None)
        self.assertEqual(expected, r['values'])
        self.assertEqual(expected2, r2['values'])
        self.assertEqual(180.0, r['tsInterval'])

        # Test the next interval up
        # (Note we add 1 since getStat() returns inclusive for both bounds)
        for i in range(24*60*60 / (15*60) - len(expectedDayAdd) + 1):
            expectedDayAdd.append(0)
            expectedDaySet.append(None)
        r = self.stats.getStat('test', self.now
            , self.now + datetime.timedelta(seconds = 24*60*60))
        self.assertEqual(expectedDayAdd, r['values'])
        r2 = self.stats.getStat('test-sample', self.now
            , self.now + datetime.timedelta(seconds = 24*60*60))
        self.assertEqual(expectedDaySet, r2['values'])


    def test_noDataForAwhile(self, statName = 'test', statZero = 0):
        # This timestamp always has the same base value - it is over a split
        nnow = datetime.datetime(2012, 11, 7, 5, 16, 14, 164807)
        self.stats._TEST_TIME_NOW = nnow
        nnow = (nnow - datetime.datetime.utcfromtimestamp(0)).total_seconds()
        self.now = datetime.datetime.utcfromtimestamp(
            self.stats._getBlocks(nnow, [(6*60*60,100)])[0][3] + 30)
        firstVal = 43

        old = self.now - datetime.timedelta(seconds = 50*60)
        ancient = old - datetime.timedelta(seconds = 180)
        self.stats._tryNewStat(self.stats._getSchemaFor(statName), statName,
                self.stats._getTimeVal(ancient), randomize = True)
        # Since the collection is marked as having latest data from old, if
        # we get stats from then on, we should get a bunch of zeroes.
        self.stats.addStat(statName, firstVal, time = old)
        r = self.stats.getStat(statName, start = old, stop = self.now)
        print("interval: " + str(r['tsInterval']))
        print(r['values'])
        for i, v in enumerate(r['values'][1:]):
            print("looking at {0} of {1}".format(i, len(r['values']) - 1))
            self.assertEqual(statZero, v)
        self.assertEqual(firstVal, r['values'][0])

        # Now write and see what happens - ensure that the old value wasn't
        # overwritten, the in-betweens were, and the new value is ok
        self.stats.addStat(statName, 1000, time = self.now)
        r = self.stats.getStat(statName, start = old, stop = self.now)
        for v in r['values'][1:-1]:
            self.assertEqual(statZero, v)
        self.assertEqual(firstVal, r['values'][0])
        self.assertEqual(1000, r['values'][-1])
        
        
    def test_noDataForSample(self):
        # Ensure that "None" is the default replacement value for "set" schemas
        self.test_noDataForAwhile(statName = 'test-sample', statZero = None)
        
        
    def test_older(self):
        # See what happens when we set data older than the data scheme... 
        # should just drop off
        now = datetime.datetime.utcnow()
        old = now - datetime.timedelta(days = 2)
        ancient = datetime.datetime.utcfromtimestamp(0)
        self.stats._tryNewStat(self.stats._getSchemaFor('test'), 'test',
                self.stats._getTimeVal(now))
        
        # NOTE - 2 days in the past is old enough that the first block shouldn't
        # get written for this
        self.stats.addStat('test', 23, time = old)
        
        stat = self.stats._col.find_one('test', fields = [ 'tsLatest' ])
        self.assertEqual(self.stats._getTimeVal(now), stat['tsLatest'])
        
        # Now assign some data WAYYY in the past and be sure it doesn't take
        self.stats.addStat('test', 888, time = ancient)
        self.assertEqual(
                { 0: 0, 1: 23, 2: 23 }
                , self.getCountByBlock('test')
        )


    def test_reset(self):
        # See if writing data to two points wildly apart resets appropriate 
        # layers
        epoch = datetime.datetime.utcfromtimestamp(0)
        twoBlocksAgo = self.now - datetime.timedelta(days = 30)
        # Initialize with ancient variable
        self.stats._TEST_TIME_NOW = epoch
        self.stats.addStat('test', 100)
        self.assertEqual(
                { 0: 100, 1: 100, 2: 100 }
                , self.getCountByBlock('test')
        )
        # Write with newer, ensure ancient no longer exists
        self.stats._TEST_TIME_NOW = twoBlocksAgo
        self.stats.addStat('test', 200)
        self.assertEqual(
                { 0: 200, 1: 200, 2: 200 }
                , self.getCountByBlock('test')
        )
        # Write with newer, but the oldest layer should be preserved
        self.stats._TEST_TIME_NOW = self.now
        self.stats.addStat('test', 300)
        self.assertEqual(
                { 0: 300, 1: 300, 2: 500 }
                , self.getCountByBlock('test')
        )


    def test_wrongSchema(self):
        # Since the stats interface caches schemas for docs, we want to be
        # sure that we don't update a stat based on the wrong schema.
        self.stats.addStat('test', 100, time = self.now)
        self.stats.addStat('test', 100, time = self.now)
        self.stats._col.update(
            { '_id': 'test' }
            , { '$set': { 'schema.version': 2, 'schema.type': 'set' }}
        )
        self.stats.addStat('test', 100, time = self.now)
        # This operation should perform a 'set' now, even though 'add' is 
        # what is cached
        r = self.stats.getStat('test', self.now, self.now)
        self.assertEqual(100, r['values'][0])


