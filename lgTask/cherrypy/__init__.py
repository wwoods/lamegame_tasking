
import cherrypy

import lgTask
from lgTaskRoot import LgTaskRoot

def quickstart(connString, port = 8080):
    conn = lgTask.Connection(connString)
    root = LgTaskRoot(conn)
    cherrypy.tree.mount(root, '/')
    cherrypy.config.update({ 'server.socket_port': port })
    cherrypy.engine.start()
    cherrypy.engine.block()
