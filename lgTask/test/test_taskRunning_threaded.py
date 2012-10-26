
from lgTask.test.test_taskRunning import TestRunning as _TestRunning

class TestRunningThreaded(_TestRunning):
    EXTRA_CONFIG = "threaded = True"
