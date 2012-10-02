
import os
import socket

from lgTask.talk.mappingTask import MappingTask

class ProcessorInfoTask(MappingTask):
    """ProcessorInfoTask is a task that is based around lgTask.talk, and 
    serves up chunks of local log files to those asking for them.  Uses 
    include a local logging frontend.
    """
    
    TALK_KEY = 'lgProcessorInfo-'
    
    def __init__(self, *args, **kwargs):
        self.TALK_KEY += socket.gethostname()
        MappingTask.__init__(self, *args, **kwargs)
    
    
    def mapObjects(self, objs):
        """Objs come in as a (cmd, *args) tuple.
        """
        results = []
        for request in objs:
            try:
                if request[0] == 'log':
                    results.append(self._getLog(*request[1:]))
            except Exception, e:
                results.append(e.__class__.__name__ + ': ' + str(e))
        return results


    def _getLog(self, taskId, blockSize, blockIndex):
        logPath = os.path.dirname(self._lgTask_logFile)
        with open(os.path.join(logPath, taskId + '.log')) as f:
            if blockIndex >= 0:
                f.seek(blockSize * blockIndex)
                return f.read(blockSize)
            else:
                f.seek(0, 2)
                end = f.tell()
                desired = end + blockSize * blockIndex
                if desired >= 0:
                    f.seek(desired)
                    return f.read(blockSize)
                elif desired > -blockSize:
                    f.seek(0)
                    return f.read(desired + blockSize)
                else:
                    # Past record, return empty string
                    return ''
