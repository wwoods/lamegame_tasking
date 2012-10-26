
import cherrypy
import datetime
import lgTask
from lgTask.cherrypy.staticServer import StaticServer
from lgTask.cherrypy.stats import StatsRoot
from lgTask.lib.timeInterval import TimeInterval
import os

from controls import *

class TaskView(Control):
    template = """<h2>{title}</h2>{children}"""
    
    # Define these so they go to class args rather than kwargs
    query = None
    sort = None
    limit = None
    conn = None
    showKill = False
        
    def build(self):
        # Run query, fill in table!
        tasks = self.conn._database[self.conn.TASK_COLLECTION].find(
                self.query
                , sort = self.sort
                , limit = self.limit
        )
        cells = [ ('taskClass', 'Task Class')
                 , ('kwargs', 'Task Kwargs') 
                 , ('state', 'State')
                 , ('tsStart', 'Started')
                 , ('tsStop', 'Stopped'), ('lastLog', 'Last Log') ]
        cols = len(cells) + 1
        if self.showKill:
            cols += 1
        table = Table(cols)
        if self.showKill:
            table.add_cell(TextControl(text = 'kill'))
        table.add_cell(TextControl(text = 'Task ID'))
        for c in cells:
            table.add_cell(TextControl(text = c[1]))
        for t in tasks:
            i = t['_id']
            if self.showKill:
                table.add_cell(LiteralControl(
                    html = '<a href="killTask?id={0}">kill</a>'.format(i)))
            table.add_cell(LiteralControl(
                    html = '<a href="taskLog?id={0}">{0}</a>'.format(i)))
            for c in cells:
                table.add_cell(TextControl(text = str(t.get(c[0], ''))))
            
        self.append(table)
    

class LgTaskRoot(object):
    """Cherrypy-based object for serving up stats about a cluster of lgTask
    processors.
    """

    static = StaticServer(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__))
            , 'static'))
    
    def __init__(self, connection):
        """Creates a root object capable of serving up information about the
        given lgTask.Connection and its processors.
        """
        self._conn = connection
        self.stats = StatsRoot(self._conn)


    @cherrypy.expose
    def index(self):
        body = LgTaskPage()
        body.append(TaskView(
            title = 'Recently Failed Tasks'
            , conn = self._conn
            , query = dict(
                state = 'error'
                , tsStop = { 
                    '$gt': datetime.datetime.utcnow() - TimeInterval('7 days') 
                }
            )
            , sort = [ ('tsStop', -1 ) ]
            , limit = 10
        ))
        body.append(TaskView(
            title = 'Oldest Running Tasks'
            , conn = self._conn
            , query = dict(
                state = 'working'
            )
            , sort = [ ('tsRequest', 1 ) ]
            , limit = 10
            , showKill = True
        ))
        body.append(TaskView(
            title = 'Upcoming Tasks'
            , conn = self._conn
            , query = dict(
                state = 'request'
            )
            , sort = [ ('tsRequest', 1 ) ]
            , limit = 10
        ))
        body.append(TaskView(
            title = 'Recently Completed Tasks'
            , conn = self._conn
            , query = dict(
                state = { '$in': lgTask.Connection.states.DONE_GROUP }
            )
            , sort = [ ('tsStop', -1 ) ]
            , limit = 10
        ))
        return body.gethtml()


    @cherrypy.expose
    def killTask(self, id):
        """Kill task with id"""
        self._conn.killTask(id)
        raise cherrypy.HTTPRedirect('../')


    @cherrypy.expose
    def taskLog(self, id):
        """Return the log for task with ID.
        """
        blocks = []
        while True:
            block = self._conn.talkGetTaskLog(id, len(blocks))
            blocks.append(block)
            if len(block) == 0:
                break

        page = LgTaskPage()
        page.append(LiteralControl(
            html='<h1>Task {0}</h1><pre style="white-space:pre-wrap;">{1}</pre>'
                .format(
                    id, ''.join(blocks))))
        return page.gethtml()
    
