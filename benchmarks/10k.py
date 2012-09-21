
import os
import shutil
import sys
import threading
import time

sys.path.insert(0, '.')

import lgTask

def doTest():
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
        f.write('threaded = True\n')
        f.write('taskDatabase = "{0}"\n'.format(db))
        f.write('pythonPath = [ "../" ]\n')
    with open('tasks.py', 'w') as f:
        f.write('from lgTask.test.addTask import AddTask\n')
    c = lgTask.Connection(db)
    c._database.drop_collection('test')
    c._database.drop_collection(c.TASK_COLLECTION)
    c._database.drop_collection(c.SCHEDULE_COLLECTION)

    col = c._database['test']
    col.insert({ 'id': 'a', 'value': 0 })

    for i in range(10000):
        c.createTask("AddTask", value = i)

    a = time.time()
    ps = []
    for i in range(1):
        ph = 'p{0}'.format(i)
        os.mkdir(ph)
        os.system('ln -s ../processor.cfg {0}/processor.cfg'.format(ph))
        p = lgTask.Processor(home = ph)
        p.start()
        ps.append(p)
        def killMe(p):
            p.stop()
        t = threading.Thread(target = killMe, args = (p,))
        t.start()

    while ps:
        p = ps.pop()
        p._thread.join()

    if col.find_one({ 'id': 'a' })['value'] != 10000:
        print("FAIL")
    b = time.time()
    print("Time: {0}".format(b - a))

if __name__ == '__main__':
    doTest()

