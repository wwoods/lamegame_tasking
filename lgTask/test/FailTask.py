import lgTask

class FailTask(lgTask.Task):
    """Demonstration task that fails the requested number of times before
    giving up.
    """

    def run(self, maxRetries):
        self.retryTask(delay='1 second', maxRetries=maxRetries)

