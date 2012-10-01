
from lgTask import LoopingTask, Task
from lgTask.talk import MappingTask

class RouterTask(LoopingTask):
    LOOP_TIME = 10
    def loop(self):
        talk = self.taskConnection.getTalk()
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
    
    
class LoggerTask(Task):
    def run(self):
        for i in range(1000):
            self.log('Message ' + str(i))
        self.log('All done')
        

