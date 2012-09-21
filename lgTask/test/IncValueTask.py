
import lgTask

class IncValueTask(lgTask.Task):
    def run(self, db, id, delay=0):
        if delay:
            import time
            time.sleep(delay)

        old = db.find_one({ 'id': id })['value']
        self.log("Old: {0}".format(old))
        db.update({ 'id': id }, { '$inc': { 'value': 1 }})
        new = db.find_one({ 'id': id })['value']
        self.log("New: {0}".format(new))
        
        self.log("Changed {id} from {0} to {1}".format(old, new, id = id))
