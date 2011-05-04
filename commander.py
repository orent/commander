r"""
Extended interface to subprocess creation, dataflow oriented programming

Note: in the following examples ">> stdout" is required because
otherwise doctest cannot see your standard output. It's not
necessary in normal use.

>>> from commander import *
>>> Cmd.true().call()
0
>>> Cmd.false().call()
1
>>> from sys import stdout
>>> Cmd.echo('Hello, World!') >> stdout
Hello, World!
>>> Cmd.echo('Hello, World!') / Cmd.rev >> stdout
!dlroW ,olleH
>>> list(xrange(128,132) / Cmd.rev / float)
[821.0, 921.0, 31.0, 131.0]
>>> l=[]
>>> Cmd.sh('-c', 'echo aaa; echo bbb; echo ccc') / (lambda x: '@'+x) >> l
>>> l
['@aaa\n', '@bbb\n', '@ccc\n']

"""
from subprocess2 import *

# This module combines nicely with dataflow, but is useful without it
try:
    from dataflow import *
    from dataflow import DataflowOps as _CmdBase
except ImportError:
    _CmdBase = object

class CmdMeta(type):
    """ Make Cmd.name a shortcut to Cmd('name') """
    def __getattr__(cls, name):
        if name.startswith('_'):
            raise AttributeError
        return Cmd(name)

class Cmd(_CmdBase):
    """ Object describing an executable command """
    # Implementation: all object attrs are names of arguments to 
    # Subprocess (i.e. subprocess.Popen)
    __metaclass__ = CmdMeta

    def __init__(self, *args, **kw):
        self.args = list(args)
        vars(self).update(kw)

    def __repr__(self):
        """ Make eval(repr(command)) == command """
        argrepr = [repr(a) for a in self.args]
        argrepr += [
                "%s=%r" % (k,v)
                for k,v in sorted(vars(self).items())
                if k != 'args']
        return "%s(%s)" % (self.__class__.__name__, ', '.join(argrepr))

    def _updateargs(self, *newargs, **newkw):
        """ Return new command object with additional arguments """
        kw = vars(self).copy()
        args = kw.pop('args')
        kw.update(newkw)
        args = args + list(newargs)
        return Cmd(*args, **kw)

    __call__ = _updateargs

    def subprocess(self):
        """ Start a subprocess object described by this command """
        return Subprocess(**vars(self))

    def producer(self):
        """ Start a Producer subprocess """
        return Producer(**vars(self))

    def consumer(self):
        """ Start a Consumer subprocess """
        return Producer(**vars(self))

    def call(self, *args, **kw):
        """ Bind args, start subprocess, wait for completion """
        return self(*args, **kw).subprocess().wait()

    # implements the iterator protocol - use this command as data source
    __iter__ = producer

    # implements the feed protocol - use this command as data sink 
    def __feed__(self, source):
        return self(stdin=source).call()

    # implements filter protocol - apply this command to upstream source 
    def __filt__(self, upstream):
        return iter(self(stdin=upstream))

__all__ = ['Cmd']

if __name__ == '__main__':
    import doctest
    doctest.testmod()
