
import cherrypy
import os

class StaticServer(object):
    """For testing - serves static files out of a given root.
    """

    def __init__(self, rootDir):
        self.rootDir = rootDir
        if not os.path.isabs(self.rootDir):
            self.rootDir = os.path.abspath(rootDir)

    @cherrypy.expose
    def default(self, *args):
        file = os.path.join(self.rootDir, *args)
        return cherrypy.lib.static.serve_file(file)

