
import cherrypy

import lgTask
from lgTaskRoot import LgTaskRoot

def quickstart(connString):
    conn = lgTask.Connection(connString)
    root = LgTaskRoot(conn)
    cherrypy.tree.mount(root, '/')
    cherrypy.engine.start()
    cherrypy.engine.block()
