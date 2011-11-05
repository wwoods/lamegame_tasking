
from unittest import TestCase
from datetime import timedelta

from lgTask.lib.timeInterval import TimeInterval

class TestTimeInterval(TestCase):
    def test_Units(self):
        self.assertEqual(timedelta(days=1), TimeInterval('1 day'))
        self.assertEqual(timedelta(days=2), TimeInterval('2 days'))

