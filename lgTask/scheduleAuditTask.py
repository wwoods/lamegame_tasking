
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
            oldTask = tDb.find_one({ '_id': d['_id'] })
            if oldTask is not None and oldTask['state'] in tc.states.DONE_GROUP:
                # Task is in invalid state; move it and reschedule.
                # State is invalid because tasks should change their ID on
                # completion.  But, we want to handle this semi-gracefully.
                oldTask['_id'] = tc.getNewId()
                tDb.insert(oldTask, safe=True)
                tDb.remove(d['_id'], safe=True)
                oldTask = None

            if oldTask is None:
                # Reschedule the task
                tc._scheduleRestartTask(d['_id'])

