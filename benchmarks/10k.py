
import os
import shutil
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__ + '/..')))
from benchmarks.common import createAndChdir, runProcessor

def doTest():
    c = createAndChdir([ 'lgTask.test.addTask' ], threaded = True)
    c._database.drop_collection('test')

    col = c._database['test']
    col.insert({ 'id': 'a', 'value': 0 })

    for i in range(10000):
        c.createTask("AddTask", value = i)

    a = time.time()
    runProcessor(5.0)

    if col.find_one({ 'id': 'a' })['value'] != 10000:
        print("FAIL")
    b = time.time()
    print("Time: {0}".format(b - a))

if __name__ == '__main__':
    doTest()

