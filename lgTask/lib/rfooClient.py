"""When using rfooUtil.connect(), this module is transmitted over the wire and
compiled in the client process.
"""

import sys as _sys
import threading as _threading
import traceback as _traceback

class FrameHelper(object):
    """Wraps a frame object and allows interactive investigation.
    
    Example usage:
    >>> q = getFrame()
    >>> q
    Frame 1
    ...Stack trace...
    >>> q.locals()
    {'prompt': '...',
        'self': <IPython.frontend...>}
    >>> q['self']
    <IPython.frontend...>
    >>> q.up()
    Frame 2
    ...Stack trace...
    >>> q.down()
    Frame 1
    ...Stack trace...
    
    """
    
    def __init__(self, frame):
        self.frame = frame
        self._oldFrames = []
        
        
    def __getitem__(self, name):
        """Get the value of a variable"""
        if name in self.frame.f_locals:
            return self.frame.f_locals[name]
        if name in self.frame.f_globals:
            return self.frame.f_globals[name]
        if name in self.frame.f_builtins:
            return self.frame.f_builtins[name]
        raise NameError(name + " not found")
        
        
    def __setitem__(self, name, value):
        raise NotImplementedError("rfooUtilClient is for investigation only!")
        
        
    def __repr__(self):
        return repr(self.context())
        
    
    def context(self, limit = 1):
        """Show the current frame and (optionally) parent frames.  If limit is
        1, try to get more surrounding lines.
        """
        result = [ 'Frame {0}\n'.format(len(self._oldFrames) + 1) ]
        for filename, lineno, name, line in reversed(_traceback.extract_stack(
                self.frame, limit)):
            result.append('File: {0}:{1} - {2}\n'.format(filename, lineno, 
                    name))
            try:
                if limit > 1:
                    # Hack to just print one line
                    raise IOError
                
                with open(filename) as f:
                    lines = f.readlines()
                    start = lineno - 7
                    end = lineno + 7
                    for i, line in enumerate(lines[start - 1:end]):
                        line = line.rstrip('\n')
                        linestr = str(i + start)
                        if i + start == lineno:
                            linestr = '>' + linestr
                        result.append('%4s    %s\n' % (linestr, line))
                
            except IOError:
                # Couldn't read file, just print one line
                if line:
                    result.append('    ' + line.strip())
            result.append('\n')
        return RfooPrint(''.join(result))
    
    
    def globals(self):
        return self.frame.f_globals
    
    
    def locals(self):
        return self.frame.f_locals
        
        
    def down(self):
        """Move down a frame"""
        if len(self._oldFrames) != 0:
            self.frame = self._oldFrames.pop()
        return self
    
    
    def up(self):
        """Move up a frame"""
        if self.frame.f_back is not None:
            self._oldFrames.append(self.frame)
            self.frame = self.frame.f_back
        return self


class RfooPrint(object):
    """rfoo.utils.rconsole uses the repr of an object to send it over the wire;
    if we just return a string, then newlines won't be expanded.  So use a 
    dummy object to expand strings.
    """
    def __init__(self, text):
        self.text = text

    def __repr__(self):
        return self.text
    
    
def getFrame(matching):
    """Return first matching frame"""
    idToName = { t.ident: t.name for t in _threading.enumerate() }
    for threadId, stack in _sys._current_frames().items():
        if (matching != threadId
                and (
                    not isinstance(matching, basestring)
                    or matching not in idToName.get(threadId, 'Unknown'))
                ):
            # Not this one!
            continue
        return FrameHelper(stack)


def showStacks(matching = None):
    idToName = { t.ident: t.name for t in _threading.enumerate() }
    result = [ 'Stack Traces for all threads (most recent first)\n' ]
    for threadId, stack in _sys._current_frames().items():
        if matching is not None:
            if (matching != threadId
                    and (
                        not isinstance(matching, basestring) 
                        or matching not in idToName.get(threadId, 'Unknown'))
                    ):
                # Skip this thread
                continue
            
        result.append('=' * 10)
        result.append(' Thread {0} ({1})'.format(
                threadId, idToName.get(threadId, 'Unknown')))
        result.append(' ' + '=' * 10 + '\n')
        for filename, lineno, name, line in reversed(
                _traceback.extract_stack(stack)):
            result.append('File: {0}:{1} - {2}'.format(filename, lineno, name))
            if line:
                result.append('\n    ' + line.strip())
            result.append('\n')
        result.append('\n\n')
    return RfooPrint(''.join(result))


def showHeap():
    import guppy
    return guppy.hpy().heap()
