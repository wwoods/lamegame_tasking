
import os
import socket

from lgTask.talk.mappingTask import MappingTask

class FetchLogTask(MappingTask):
    """FetchLogTask is a talk that is based around lgTask.talk, and serves up
    chunks of local log files to those asking for them.  Uses include a local
    logging frontend.
    """
    
    TALK_KEY = 'lgTaskLogs-'
    
    def __init__(self, *args, **kwargs):
        self.TALK_KEY += socket.gethostname()
        MappingTask.__init__(self, *args, **kwargs)
    
    
    def mapObjects(self, objs):
        """Objs come in as a (taskId, blockSize, blockIndex) tuple.
        """
        results = []
        logPath = os.path.dirname(self._lgTask_logFile)
        for taskId, blockSize, blockIndex in objs:
            try:
                with open(os.path.join(logPath, taskId + '.log')) as f:
                    if blockIndex >= 0:
                        f.seek(blockSize * blockIndex)
                        results.append(f.read(blockSize))
                    else:
                        f.seek(0, 2)
                        end = f.tell()
                        desired = end + blockSize * blockIndex
                        if desired >= 0:
                            f.seek(desired)
                            results.append(f.read(blockSize))
                        elif desired > -blockSize:
                            f.seek(0)
                            results.append(f.read(desired + blockSize))
                        else:
                            # Past record, return empty string
                            results.append('')
            except Exception, e:
                results.append(e.__class__.__name__ + ': ' + str(e))
        return results
