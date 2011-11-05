
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
        
        # Other tests
        self.assertEqual(timedelta(days=2), TimeInterval('2 days'))
        self.assertEqual(timedelta(days=2, minutes=30), TimeInterval('2 days 30 minutes'))
        
        self.assertEqual(timedelta(seconds=5, microseconds=250000), TimeInterval('5.25 seconds'))

