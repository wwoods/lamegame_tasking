
from lgTask.task import Task

class ScheduleAuditTask(Task):
    """Iterates over all scheduled tasks and attempts to correct schedules 
    without any scheduled task by restarting them.

    Ran as a batched task.
    """

    def run(self):
        tc = self.taskConnection
        sDb = tc._database[tc.SCHEDULE_COLLECTION]
        tDb = tc._database[tc.TASK_COLLECTION]

        for d in sDb.find():
            if tDb.find_one({ '_id': d['_id'] }) is None:
                tc._scheduleRestartTask(d['_id'])

