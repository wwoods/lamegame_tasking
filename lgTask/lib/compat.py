
# Built in support for cherrypy... Because I like them.  Essentially, custom
# threads running alongside a cherrypy engine get noticed as threads and the
# engine waits patiently for them to close.  But if we don't signal a close
# for e.g. processor, then the task goes on forever.
try:
    import cherrypy
    class _CpTaskSubscriber(cherrypy.process.plugins.SimplePlugin):
        def __init__(self):
            cherrypy.process.plugins.SimplePlugin.__init__(self, cherrypy.engine)
            self.tasks = []
        def addTask(self, task):
            self.tasks.append(task)
            for t in self.tasks[:]:
                if not t._thread.is_alive():
                    self.tasks.remove(t)
        def stop(self):
            for t in self.tasks:
                t.stop()
    cp = _CpTaskSubscriber()
    cp.subscribe()
    def cherrypy_subscribe(task):
        cp.addTask(task)
except ImportError:
    def cherrypy_subscribe(task):
        pass
    
