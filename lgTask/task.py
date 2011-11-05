
import pymongo

class Task(object):
    """Runs the run() function with our given kwargs in a new thread. 
    Manages communication with that thread.
    """

    def __init__(self, taskDb, name=None, **kwargs):

