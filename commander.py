r"""
Extended interface to subprocess creation.

Note: in the following examples ">> stdout" is required because
otherwise doctest cannot see your standard output. It's not
necessary in normal use.

>>> from commander import *
>>> Cmd.true()
0

>>> Cmd.false()
1

>>> from sys import stdout
>>> Cmd.echo['Hello, World!'] >> stdout
Hello, World!

>>> Cmd.echo['Hello, World!'] / Cmd.rev >> stdout
!dlroW ,olleH

# equivalent, but fails doctest:
#>>> Cmd.echo['Hello, World!'] >> Cmd.rev
#!dlroW ,olleH

>>> list(xrange(128,132) / Cmd.rev / float)
[821.0, 921.0, 31.0, 131.0]

>>> l=[]
>>> Cmd.sh['-c', 'echo aaa; echo bbb; echo ccc'] / (lambda x: '@'+x) >> l
>>> l
['@aaa\n', '@bbb\n', '@ccc\n']

"""
from subprocess2 import *

# This module combines nicely with dataflow, but is still useful without it
try:
    from dataflow import *
    from dataflow import DataflowOps as _CmdBase
except ImportError:
    _CmdBase = object

class Cmd(_CmdBase):
    """ Object describing an executable command """
    # Object attrs are named arguments to Subprocess (i.e. subprocess.Popen)

    # Make Cmd.name shortcuts to Cmd('name')
    class __metaclass__(type):
        def __getattr__(cls, name):
            if name.startswith('_'):
                raise AttributeError
            return Cmd(name)

    def __init__(self, *args, **kw):
        self.args = list(args)
        vars(self).update(kw)

    def __repr__(self):
        """ Make eval(repr(command)) == command """
        argrepr = ["%s=%r" % item for item in sorted(vars(self).items())]
        return "%s(%s)" % (self.__class__.__name__, ', '.join(argrepr))

    def update(self, *newargs, **newkw):
        """ Return new command object with additional arguments """
        kw = dict(vars(self))
        args = list(kw.pop('args'))

        for newarg in newargs:
            if isinstance(newarg, dict):
                kw.update(newarg)
            else:
                args.append(newarg)
        kw.update(newkw)

        return Cmd(*args, **kw)

    def __getitem__(self, arg):
        """ Syntactic sugar for .update(). Use dict() for keyword args """
        if not isinstance(arg, tuple):
            arg = (arg,)
        return self.update(*arg)

    def subprocess(self):
        """ Start a subprocess object described by this command """
        return Subprocess(**vars(self))

    def __call__(self, *args, **kw):
        """ Add arguments, start subprocess, wait for completion """
        return self.update(*args, **kw).subprocess().wait()

    # implements the iterator protocol - use this command as data source
    def __iter__(self):
        """ Start a Producer subprocess """
        return Producer(**vars(self))

    # implements the feed protocol - use this command as data sink 
    def __feed__(self, source):
        return self.update(stdin=source)()

    # implements filter protocol - apply this command to upstream source 
    def __filt__(self, upstream):
        return iter(self.update(stdin=upstream))

if __name__ == '__main__':
    import doctest
    doctest.testmod()
