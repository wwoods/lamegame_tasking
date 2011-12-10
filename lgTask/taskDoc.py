"""Storage format / fields for the Task collection:

Fields
======

_id = String - User specified or combination of taskClass and kwargs that must
    be unique for any taskClass and kwarg combination.  Can also just be a
    random unique identifier for ordinary tasks.
host = String - hostname of machine that is running or ran the task.
kwargs = dict - Parameters to pass to task run()
lastLog = String - the last message logged.  Often this will be an exception or
    a summary of work accomplished.
state = (request|working|success|error) - The current state of this task
tsSchedule = (datetime) - The time this task was scheduled
tsRequest = (datetime) - The time requested OR the minimum time to start task
tsStart = (datetime) - The time processing began
tsStop = (datetime) - The time processing ended


"""
