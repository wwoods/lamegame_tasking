import cherrypy

from .common import *

def get_abs_path(path):
    """Returns the actual absolute path for a path beginning in "/"
    """
    return path


class LgTaskPage(Control):
    """An lgTask page.  This class overrides template rendering such
    that it will also check cherrypy.serving.lg_authority for 
    various arguments and render its children accordingly.
    """

    template = "{children}"

    class DefaultStyle(Control):
        template = """
{{{ style
  body {
    background-color: #d0d0ff;
    font-size: 12pt;
    line-height: 150%;
  }
  td {
    padding-right: 1em;
  }
  .lg-form {
    background-color: #ffffff;
    border-radius: 0.5em;
    padding: 1em;
    margin-top: 1em;
    margin-bottom: 1em;
  }
}}}
        """

    def build(self):
        self.prepend(LgMenuControl())

        #Use PageControl
        children = self._children[:]
        self._children = []
        p = PageControl(title='lgTask Dashboard').appendto(self)
        NoIndexControl().appendto(p) #Don't index auth pages...
        CssResetControl().appendto(p)
        self.DefaultStyle().appendto(p)
        center = CenterControl(width='800px').appendto(p)
        form = DivControl(cls="lg-form").appendto(center)
        form.extend(children)


class LgMenuControl(Control):
    """The lg_authority menu"""

    template = """
{{{ style
  .lg-menu {
    margin-bottom: 0.5em;
  }
  .lg-menu a {
    color: #000000;
    text-decoration: underline;
    display: inline-block;
    margin-right: 1em;
    padding: 0.25em;
  }
  .lg-menu a:hover {
    background-color: #efffef;
  }
}}}
<div class="lg-menu">
  {children}
</div>
    """

    def __init__(self, **kwargs):
        Control.__init__(self, **kwargs)

    def build(self):
        #if not getattr(cherrypy, 'user', None):
        #    self.template = ''
        #    return
        
        class Link(Control):
            template = '<a href="{path}">{name}</a>'
            def prerender(self, kwargs):
                kwargs['path'] = get_abs_path(kwargs['path'])
        self.append(Link(path='/', name='Dashboard'))
        #self.append(Link(path='/', name='Dashboard'))


@Control.Kwarg('error', '', 'The error text to display')
class LgErrorControl(Control):
    template = """
{{{ style
  .lg-error {
    color: #ff0000;
  }
}}}
<div class="lg-error">{error}</div>
    """

