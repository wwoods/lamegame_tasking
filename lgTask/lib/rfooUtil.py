"""
Module to import and debug with rfoo
"""

import os
import pprint
import rfoo
from rfoo.utils import rconsole
import socket
import sys
import threading
import time
import traceback

_dir = os.path.dirname(os.path.abspath(__file__))

def connect(port, host = '127.0.0.1'):
    """Connects an rconsole to the given port, uploads our commands.
    """
    rfooClient = open(os.path.join(_dir, 'rfooClient.py')).read()
    console = rconsole.ProxyConsole(port)
    
    # Taken from rconsole.py:interact()
    try:
        import readline
        readline.set_completer(console.complete)
        readline.parse_and_bind('tab: complete')
    except ImportError:
        pass
    
    console.conn = rfoo.InetConnection().connect(host = host, 
            port = console.port)
    console.runsource('import sys, imp')
    console.runsource('clientModule = imp.new_module("rfooClient")')
    console.runsource("exec {0} in clientModule.__dict__".format(
            repr(rfooClient)))
    console.runsource('globals().update(clientModule.__dict__)')
    return rconsole.code.InteractiveConsole.interact(console, banner = None)
    

def spawnServer(port = None):
    """Spawns an rconsole with our namespace.

    Returns the port hosted on.
    """
    if port is None:
        port = os.getpid()
        if port < 1024:
            port += 1000

    try:
        port = _realSpawn(port)
    except socket.error:
        port = _realSpawn(0)
    return port


class _RfooUtilConsoleHandler(rconsole.ConsoleHandler):
    def complete(self, phrase, state):
        """Re-defined to stop @rfoo.restrict_local declaration"""
        return self._completer.complete(phrase, state)
    
    def runsource(self, source, filename="<input>"):
        """Again, re-defined to stop @rfoo.restrict_local declaration"""
        self._namespace['_rcon_result_'] = None
        try:
            compile(source, '<input>', 'eval')
            source = '_rcon_result_ = ' + source
        except SyntaxError:
            pass
        more = self._interpreter.runsource(source, filename)
        result = self._namespace.pop('_rcon_result_')
        if more is True:
            return True, ''
        output = self._interpreter.buffout
        self._interpreter.buffout = ''
        
        if result is not None:
            result = pprint.pformat(result)
            output += result + '\n'
        return False, output


def _realSpawn(port):
    # Methods are loaded on client connect
    methods = dict()
    server = rfoo.InetServer(_RfooUtilConsoleHandler, methods)
    hadError = [ False ]
    def _wrapper():
        try:
            threading.current_thread().setName('rfooConsoleUtilServer')
            server.start(host = '0.0.0.0', port = port)
        except:
            hadError[0] = True

    t = threading.Thread(target = _wrapper)
    t.daemon = True
    t.start()
    realPort = 0
    while not hadError[0]:
        realPort = server._conn.getsockname()[1]
        if realPort != 0:
            break
        time.sleep(0.01)
    if hadError[0]:
        raise socket.error('Could not bind port ' + str(port))
    return realPort


if __name__ == '__main__':
    if len(sys.argv) == 3:
        host, port = sys.argv[1:]
        connect(port = int(port), host = host)
    else:
        port = sys.argv[1]
        connect(port = int(port))

