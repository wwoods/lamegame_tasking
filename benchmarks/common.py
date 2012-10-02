
import os
import shutil
import sys
import time

p = os.path.dirname(os.path.abspath(__file__))
lgp = os.path.dirname(p)
sys.path.insert(0, lgp)

import lgTask

def createAndChdir(tasks, threaded = False):
    """Makes ./tmp and the necessary processor files there.  Also resets the
    lgTask collections in the  test database.  Chdir's to ./tmp and returns.

    tasks -- Array of modules to import containing tasks (e.g. 
            [ 'lgTask.test.addTask' ])
    threaded -- True to use threading, False for multiprocessing

    Returns an lgTask.Connection instance to the test db
    """
    path = os.path.abspath(os.path.join(__file__, '../tmp'))
    try:
        shutil.rmtree(path)
    except OSError:
        # Didn't exist
        pass
    os.makedirs(path)
    os.chdir(path)

    db = "pymongo://localhost/test_lgTask"
    os.system('ln -s ../../lgTask lgTask')
    with open('processor.cfg', 'w') as f:
        f.write('[processor]\n')
        f.write('threaded = {0}\n'.format(threaded))
        f.write('taskDatabase = "{0}"\n'.format(db))
        f.write('pythonPath = [ "../" ]\n')
    with open('tasks.py', 'w') as f:
        for m in tasks:
            f.write('from {0} import *'.format(m))
    c = lgTask.Connection(db)
    c._database.connection.drop_database(c._database.name)
    # Re-make the Connection, since we've killed any init stuff
    c = lgTask.Connection(db)
    return c


def runProcessor(maxRunTime = 5.0):
    """Run a processor out of the current dir for up to X seconds (it is
    launched with debug flag "stopOnNoTasks").
    """
    #ph = 'p{0}'.format(i)
    #os.mkdir(ph)
    #os.system('ln -s ../processor.cfg {0}/processor.cfg'.format(ph))
    lgTask.Processor.LOAD_SLEEP_SCALE = 0.0
    p = lgTask.Processor(home = '.')
    p.start()
    if maxRunTime <= 0.0:
        maxRunTime = 3600*2
    p.stop(maxRunTime)
    p._thread.join()


