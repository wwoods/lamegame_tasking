
import lgTask

class MappingTask(lgTask.LoopingTask):
    """For use with lgTask.talk.TalkConnection.map().  Allows external tasks
    to get this task to map a set of objects for them.

    There are class-level constants for controlling the mapper:
    
    BATCH_SIZE -- designates the preferred batch size for objects to map
    BATCH_TIME -- designates the batch time for receiving objects to map.
    TALK_KEY -- designates the key to recv from

    Define mapObjects(objs) to perform the mapping.
    """

    BATCH_SIZE = 1
    BATCH_TIME = 0.2
    TALK_KEY = None

    _RECV_TIMEOUT = 60.0
    _SEND_TIMEOUT = 20.0

    def run(self, **kwargs):
        self._talk = self.taskConnection.getTalk()
        if self.TALK_KEY is None:
            raise ValueError("Need to specify TALK_KEY for MappingTask")
        lgTask.LoopingTask.run(self, **kwargs)


    def mapObjects(self, objects):
        """Override to perform the mapping operation on "objects".  Should
        return a new list of objects in the same order.
        """
        raise NotImplementedError()


    def loop(self):
        objs = self._talk.recv(self.TALK_KEY, self.BATCH_SIZE,
                self.BATCH_TIME, self._RECV_TIMEOUT)
        if not objs:
            # We didn't get anything, but that's ok.  Sleep till the next 
            # iteration
            return
        toMap = [ o[2] for o in objs ]
        mapped = self.mapObjects(toMap)
        toSend = {}
        for i, o in enumerate(objs):
            # We may have objects with different return addresses, so make
            # sure we keep them separate
            toSend.setdefault(o[0], []).append(( o[1], mapped[i] ))
        self.talk_sendMultipleBuffered(toSend, timeout = self._SEND_TIMEOUT)

