
import lgTask

class IncValueTask(lgTask.Task):
    def run(self, db, id):
        old = db.find_one({ 'id': id })['value']
        self.log("Old: {0}".format(old))
        db.update({ 'id': id }, { '$inc': { 'value': 1 }})
        new = db.find_one({ 'id': id })['value']
        self.log("New: {0}".format(new))
        
        self.log("Changed from {0} to {1}".format(old, new))