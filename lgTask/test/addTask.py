import lgTask

class AddTask(lgTask.Task):
    """Adds one to the passed 'value' param and prints it.
    """

    def run(self, value):
        value += 1
        self.log("New value: {0}".format(value))

