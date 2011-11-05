"""Storage format / fields for the Task collection:

Fields
======

kwargs = dict - Parameters to pass to task run()
lastLog = String - the last message logged.  Often this will be an exception or
    a summary of work accomplished.
state = (request|working|success|error) - The current state of this task
tsSchedule = (datetime) - The time this task was scheduled
tsRequest = (datetime) - The time requested OR the minimum time to start task
tsStart = (datetime) - The time processing began
tsStop = (datetime) - The time processing ended


"""
