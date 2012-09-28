
from lgTask import Task
from lgTask.talk import MappingTask

class RouterTask(Task):
    def run(self):
        talk = self.taskConnection.getTalk()
        while True:
            vals = talk.recv('toRoute', batchSize = 2000)
            #queue = talk.getEnqueuer()
            toSend = {}
            for v in vals:
                # v[1] is routing key, v[0] is value
                toSend.setdefault(v[1], []).append(v[0])
            print("SENDING OUT {0}".format(toSend))
            talk.sendMultiple(toSend)


class Calculator(MappingTask):
    TALK_KEY = 'calc'
    BATCH_SIZE = 1
    LOOP_TIME = 10
    def mapObjects(self, objects):
        return [ eval(o) for o in objects ]

