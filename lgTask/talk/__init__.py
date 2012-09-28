"""Low-latency inter-task talking functionality.

Examples:

talk = lgTaskConnection.getTalk()
if you want a timeout other than 60 seconds:
talk = lgTaskConnection.getTalk(sendTimeout = X)

Register a specific message receiver to spawn when there isn't enough buffer
space to receive our message, and not enough tasks of this type exist:
talk.register('typeKey', 'taskClass', { taskKwargs }, max = N)

Send a message containing one or more objects
talk.send('typeKey', [ jsonEncodableObjects ])
or if something's important...
talk.send('typeKey', [ jsonEncodableObjects ], priority = n>0)

Also, each send() call may take an optional timeout argument that overrides the
TalkConnection's timeout.

Note - Throttling is performed through the send() message; if there are not
    enough client tasks receiving the messages being sent, or those processes
    are simply not receiving fast enough, then the send method will block until
    it can go through.

    If send's timeout is reached, will raise lgTask.talk.TalkTimeoutError

Receive one or several objects (send: only, does not receive asks)
CRITICAL - If your task is going to call this EVEN ONCE, it is expected to
continue calling this with the same batch size.  Specifically, the controller
for the Processor will announce that it is accepting this type of 
communication, so if you never call recv() again, you'll end up buffering
some messages that will need to be rerouted.

talk.recv('typeKey', upTo = 5, timeout = None) -> [ de-jsonified objects ]
or
talk.recv([ 'typeKey1', ... ], upTo = 5, timeout = None)

Send a message & expect a response:
talk.send('typeKey', [ jsonEncodableObject with response routing key ])
talk.recv(response key from send, timeout = X (expected processing time + n))

"""

from error import TalkTimeoutError
from mappingTask import MappingTask
from talkConnection import TalkConnection

