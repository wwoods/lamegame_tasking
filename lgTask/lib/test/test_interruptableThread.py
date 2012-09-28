import time
import unittest

from lgTask.lib import InterruptableThread

class TestInterruptableThread(unittest.TestCase):
    def test_interrupt(self):
        class ItError(Exception):
            pass

        class It(InterruptableThread):
            def run(self):
                self.correct = False
                a = 0
                try:
                    while not hasattr(self, '_stop'):
                        a = a + 1
                except ItError:
                    self.correct = True

            def stop(self):
                self.raiseException(ItError)
                self.join(0.2)
                self._stop = True

        t = It()
        t.start()
        t.stop()
        self.assertEqual(True, t.correct)

