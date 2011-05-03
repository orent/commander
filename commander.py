"""
Extended interface to subprocess creation, dataflow oriented programming

TODO:
exception propagation
exit status errorlevel checks
serialized safe wrapper threads

"""

import subprocess, io, os, threading
from subprocess import PIPE, STDOUT


class Subprocess(subprocess.Popen):
    """ Similar to subprocess.Popen with the following enhancements:
        * expands iterable arguments recursively (except strings)
        * expanded arguments are stripped of newlines to allow output of
          one process to be used as args to another (like shell backticks)
        * stdin may be another subprocess (producer) or any python iterable
    """
    def __init__(self, args, **kw):
        args = self._processargs(args)
        if 'stdin' in kw:
            kw['stdin'] = self._asfiledesc(kw['stdin'])
        subprocess.Popen.__init__(self, args, **kw)

    @staticmethod
    def _asfiledesc(arg):
        """ Convert argument into something that has a file descriptor """
        if hasattr(arg, 'fileno') or isinstance(arg, int):
            return arg                      # use as-is

        if isinstance(arg, str):
            # special case strings to avoid iteration char by char
            return Iter2Pipe(arg)

        iterator = iter(arg)

        if hasattr(iterator, 'fileno'):     # an iterator with a fileno?
            return iterator                 # (e.g. file object or producer)
        else:
            return Iter2Pipe(iterator)      # no, wrap in bridge thread

    @staticmethod
    def _processargs(args, maxdepth=3):
        """ Expand iterable arguments """

        def recurse(args, depth=1):
            """ recurse except strings, strip newlines except top level """
            for arg in args:
                if isinstance(arg, str):
                    if depth > 1:
                        arg = arg.rstrip('\n')
                    yield arg
                elif depth > maxdepth:
                    yield str(arg)
                else:
                    try:
                        iterator = iter(arg)
                    except TypeError:
                        yield str(arg)
                    else:
                        for x in recurse(iterator, depth + 1):
                            yield x

        return list(recurse(args))


class Producer(Subprocess, io.BufferedReader):
    """ Exposes a readable file-like interface to stdout of a subprocess """
    def __init__(self, args, **kw):
        if 'stdout' in kw:
            raise ValueError("Producer: stdout already overridden")
        kw['stdout'] = subprocess.PIPE
        Subprocess.__init__(self, args, **kw)

        fd = os.dup(self.stdout.fileno())   # prevents closing of pipe
        self.stdout = None                  #  when stdout file object dies

        filemode = 'rU' if kw.get('universal_newlines', False) else 'r'
        io.BufferedReader.__init__(self, io.FileIO(fd, filemode))


class Consumer(Subprocess, io.BufferedWriter):
    """ Exposes a writable file-like interface to stdin of a subprocess """
    def __init__(self, args, **kw):
        if 'stdin' in kw:
            raise ValueError("Consumer: stdin already overridden")
        kw['stdin'] = subprocess.PIPE
        Subprocess.__init__(self, args, **kw)

        fd = os.dup(self.stdin.fileno())    # prevents closing of pipe
        self.stdin = None                   #  when stdin file object dies
        io.BufferedWriter.__init__(self, io.FileIO(fd), 'w')


class _RawIterIO(io.BufferedIOBase):
    """ Helper class for turning python iterator to a file-like object """
    def __init__(self, iterable):
        self.iterator = self.sourcereader(iterable)
        self.remainder = None

    def sourcereader(self, iterable):
        for x in iterable:
            if not isinstance(x, str):
                x = '%s\n' % x
            yield x
        while True:
            yield ''

    def readable(self):
        return True

    def read(self, n=None):
        """ Read no more than n bytes from source """
        if self.remainder:
            data = self.remainder
            self.remainder = None
        else:
            data = next(self.iterator)
        if n is not None and 0 < n < len(data):
            data, self.remainder = data[:n], data[n:]
        return data

    def close(self):
        io.BufferedIOBase.close(self)
        self.iterator = iter(())


def make_readable(obj):
    """ Adapt object into something that has a .read() method """
    if hasattr(obj, 'read'):
        return obj
    elif isinstance(obj, str):
        from cStringIO import StringIO
        return StringIO(obj)
    else:
        # assume it's somehow iterable:
        return io.BufferedReader(_RawIterIO(obj))


class Iter2Pipe(threading.Thread):
    """ Bridge thread from python iterator to a pipe """

    def __init__(self, obj):
        self._source = make_readable(obj)
        threading.Thread.__init__(self)

    def fileno(self):
        """ File descriptor through which data may be read. """
        self._initthread()
        return self._readfd

    ### internal methods:

    def _initthread(self):
        """ Create thread to read from iterable and write to pipe """
        if self.is_alive():
            return

        self._readfd, self._writefd = os.pipe()

        # ensure write side of pipe is not inherited by child process
        import fcntl
        flags = fcntl.fcntl(self._writefd, fcntl.F_GETFD)
        fcntl.fcntl(self._writefd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

        # Get native pipe block size (not critical, but nice for performance)
        try:
            self._bufsize = os.fstat(self._writefd).st_blksize
        except (os.error, AttributeError):
            self._bufsize = io.DEFAULT_BUFFER_SIZE
        self.start()

    def run(self):
        """ Thread main function """
        try:
            while True:
                try:
                    buf = self._source.read(self._bufsize)
                except Exception as e:
                    self._pending_exception = e
                    break
                if not buf:
                    break
                try:
                    os.write(self._writefd, buf)
                except OSError:
                    return
        finally:
            os.close(self._writefd)
            self._source.close()


class CmdMeta(type):
    """ Make Cmd.name a shortcut to Cmd('name') """
    def __getattr__(cls, name):
        if name.startswith('_'):
            raise AttributeError
        return Cmd(name)


class DataflowOps(object):
    """ Adds / and >> dataflow operators to an object
    (sorry, but | has lower precedence than >> which is awkward) """
    def __div__(self, right):
        """ Concatenate dataflow or stages """
        return Dataflow(self, right)

    def __rdiv__(self, left):
        """ Concatenate to dataflow on the right """
        return Dataflow(left, self)

    def __rshift__(self, right):
        ( Dataflow(self) / Dataflow(right) ).call()

    def __rrshift__(self, left):
        ( Dataflow(left) / Dataflow(self) ).call()


class Cmd(DataflowOps):
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


def filt(filter, upstream):
    """ Apply a filter to an iterator """
    if hasattr(filter, '__filt__'):
        return filter.__filt__(upstream)     # apply filter to entire stream
    elif callable(filter):
        return (filter(x) for x in upstream) # apply individually to each item
    else:
        raise TypeError("filt: %r object is not a valid filter"
                % filter.__class__.__name__)


def feed(sink, source):
    """ Feed source iterator into a data sink """
    if hasattr(sink, '__feed__'):
        return sink.__feed__(source)
    elif type(sink) in feed_registry:
        feed_registry[type(sink)](sink, source)
    elif callable(sink):
        # feed items individually
        for x in source:
            sink(x)
    elif hasattr(sink, 'write'):
        # feed items individually, converting to string first
        for x in source:
            if not isinstance(x, str):
                x = '%s\n' % x
            sink.write(x)
    else:
        raise TypeError("sink: %r object is not a valid data sink"
                % sink.__class__.__name__)


def feed_list(l, source):
    """ Replace list contents with iterable source """
    l[:] = source

def feed_set(s, source):
    """ Replace set contents with iterable source"""
    s.clear()
    s.update(source)

def feed_null(s, source):
    """ Feed iterable source to the bit bucket """
    for x in source:
        pass

feed_registry = {
    list : feed_list,
    set : feed_set,
    type(None): feed_null,
}


class Dataflow(DataflowOps):
    """ Object representing recipe for a data flow

    A dataflow is iterable:
      iter(Dataflow(src)) => iter(src)
      iter(Dataflow(src, filter)) => filter(src)
      iter(Dataflow(src, filter1, filter2)) => filter2(filter1(src))

    A dataflow may be used as a filter:
      filt(Dataflow(filter1, filter2), src) => filter2(filter1(src))

    A dataflow may be used as a sink:
      feed(sink, Dataflow(src, filter2))

    A dataflow may represent a complete dataflow:
    Dataflow(src, filter1, filter2, ..., sink).call()
      Executed with for its side effects.

    Equivalent dataflow operators:

    a / b >> c == feed(c, Dataflow(a,b))

    Dataflow is generic and independent from Subprocess and its
    subclasses. Dataflow normally uses the iterator protocol but
    note that Subprocess and its subclasses will use file handles
    for any object that has a fileno() method so many dataflows
    may run entirely outside the python interpreter.
    """

    def __init__(self, *stages):
        flat = []
        for stage in stages:
            if stage.__class__ is Dataflow:
                flat.extend(stage.stages)
            else:
                flat.append(stage)
        self.stages = tuple(flat)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__,
                ', '.join(repr(s) for s in self.stages))

    def __iter__(self):
        """ Iterate first stage, applying the following stages as filters """
        if len(self.stages) == 0:
            return iter(())
        else:
            first, rest = self.stages[0], self.stages[1:]
            return filt(Dataflow(*rest), iter(first))

    def __filt__(self, upstream):
        """ Apply dataflow as a filter to specified upstream source """
        if len(self.stages) == 0:
            return upstream
        else:
            laststage = self.stages[-1]
            # construct dataflow consisting of the upstream source
            # followed by all filter stages but the last:
            upstream2 = Dataflow(*(upstream,) + self.stages[:-1])
            # And apply last stage as filter to this upstream source
            return filt(laststage, upstream2)

    def call(self):
        """ Run dataflow. Last stage must be a valid sink. """
        sink = self.stages[-1]
        feed(sink, Dataflow(*self.stages[:-1]) )

class File(DataflowOps):
    """ Class representing a file on the disk """
    def __init__(self, filename):
        self.filename = filename

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.filename)

    def __iter__(self):
        return open(self.filename)

    def __feed__(self, source):
        feed(open(self.filename, 'w'), source)

__all__ = [
    'PIPE', 'STDOUT',
    'Subprocess', 'Producer', 'Consumer',
    'Cmd', 'Dataflow', 'File',
    'filt', 'feed',
]
