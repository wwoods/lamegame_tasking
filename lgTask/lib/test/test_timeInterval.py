
from unittest import TestCase
from datetime import timedelta

from lgTask.lib.timeInterval import TimeInterval

class TestTimeInterval(TestCase):
    def test_Units(self):
        # Units
        self.assertEqual(timedelta(days=1), TimeInterval('1 day'))
        self.assertEqual(timedelta(hours=1), TimeInterval('1 hour'))
        self.assertEqual(timedelta(minutes=1), TimeInterval('1 minute'))
        self.assertEqual(timedelta(seconds=1), TimeInterval('1 second'))
        self.assertEqual(timedelta(microseconds=1), TimeInterval('1 microsecond'))
        
        # Other tests
        self.assertEqual(timedelta(days=2), TimeInterval('2 days'))
        self.assertEqual(timedelta(days=2, minutes=30), TimeInterval('2 days 30 minutes'))
        
        self.assertEqual(timedelta(seconds=5, microseconds=250000), TimeInterval('5.25 seconds'))
        self.assertEqual(timedelta(microseconds=200000), TimeInterval('0.2 seconds'))

        self.assertEqual(timedelta(seconds=-5), TimeInterval('5 seconds ago'))
        self.assertEqual(timedelta(days=-2), TimeInterval('2 days ago'))

        self.assertEqual(timedelta(days=0), TimeInterval('now'))

    def test_strAndRepr(self):
        a = TimeInterval('36 hours 4 minutes 1 second 2 microseconds')
        self.assertEqual(
            '1 day 12 hours 4 minutes 1 second 2 microseconds'
            , str(a)
        )
        self.assertEqual(
            'TimeInterval("1 day 12 hours 4 minutes 1 second 2 microseconds")'
            , repr(a)
        )

        # Now verify that TimeInterval(str(otherTI)) == otherTI
        def testInterval(ti):
            self.assertEqual(ti, TimeInterval(str(ti)))
        testInterval(TimeInterval('12 days 1 hour 4 microseconds'))
        testInterval(TimeInterval('0.5 days 2 hours'))
        testInterval(TimeInterval('0.25 seconds'))

