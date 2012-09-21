
from lgTask import Task

class RouterTask(Task):
    def run(self):
        talk = self.taskConnection.getTalk()
        while True:
            vals = talk.recv('toRoute', batchSize = 2000)
            #queue = talk.getEnqueuer()
            #for v in vals:
            #    # v[1] is routing key, v[0] is value
            #    queue.append(v[1], v[0])
            #queue.send()
            talk.send(vals[0][1], [ v[0] for v in vals ])

