"""
Extended interface to subprocess creation.
"""

import subprocess, io, os
from subprocess import PIPE, STDOUT

class Subprocess(subprocess.Popen):
    """ Similar to subprocess.Popen with the following enhancements:
        * Recurses into iterable arguments
        * Recusion strips newlines, to enable the output of subprocess
          to be used as arguments to another (like shell backticks)
        * stdin may be another subprocess (producer) or any python iterable
    """
    def __init__(self, args, **kw):
        args = self._processargs(args)
        if 'stdin' in kw:
            kw['stdin'] = self._asfiledesc(kw['stdin'])
        subprocess.Popen.__init__(self, args, **kw)

    @staticmethod
    def _asfiledesc(arg):
        """ Convert argument to something that may be used as a file descriptor """
        if hasattr(arg, 'fileno') or isinstance(arg, int):
            return arg
        if isinstance(arg, basestring):
            return FeederThread(arg)      # special case strings - do not iterate char by char
        iterator = iter(arg)
        if hasattr(iterator, 'fileno'):     # e.g. a python file object is an iterator with a fileno()
            return iterator
        else:
            return FeederThread(iterator)  # wrap in feeder thread

    @staticmethod
    def _processargs(args, maxdepth=3):

        def recurse(args, depth=1):
            """ recurse nested iterables except strings, stripping newlines except top level """
            for arg in args:
                if isinstance(arg, basestring):
                    if depth > 1:
                        arg = arg.rstrip('\n')
                    yield arg
                    continue
                if depth > maxdepth:
                    yield str(arg)
                    continue
                try:
                    iterator = iter(arg)
                except TypeError:
                    yield str(arg)
                else:
                    for x in recurse(iterator, depth + 1):
                        yield x

        return list(recurse(args))

class Producer(Subprocess, io.BufferedReader):
    """ Exposes a readable file-like interface to the standard output of a subprocess """
    def __init__(self, args, **kw):
        if 'stdout' in kw:
            raise ValueError("stdout already overridden on Producer")
        kw['stdout'] = subprocess.PIPE
        Subprocess.__init__(self, args, **kw)

        fd = os.dup(self.stdout.fileno())   # prevents next line from closing file
        self.stdout = None

        filemode = 'rU' if kw.get('universal_newlines', False) else 'r'
        io.BufferedReader.__init__(self, io.FileIO(fd, filemode))

class Consumer(Subprocess, io.BufferedWriter):
    """ Exposes a writable file-like interface to the standard input of a subprocess """
    def __init__(self, args, **kw):
        if 'stdin' in kw:
            raise ValueError("stdin already overridden on Consumer")
        kw['stdin'] = subprocess.PIPE
        Subprocess.__init__(self, args, **kw)

        fd = os.dup(self.stdin.fileno())    # prevents next line from closing file
        self.stdin = None
        io.BufferedWriter.__init__(self, io.FileIO(fd), 'w')

class _RawIterIO(io.BufferedIOBase):
    """ Helper class for turning python iterators into a file-like object """
    def __init__(self, iterable):
        self.iterator = iter(iterable)
        self.remainder = None

    def readable(self):
        return True

    def read(self, n=None):
        """ Read no more than n bytes from source """
        if self.remainder:
            data = self.remainder
            self.remainder = None
        else:
            try:
                data = next(self.iterator)
            except StopIteration:
                data = ''
                self.iterator = iter(())
            if not isinstance(data, basestring):
                data = '%s\n' % data

        if n is not None and 0 < n < len(data):
            data, self.remainder = data[:n], data[n:]

        return data

    def close(self):
        io.BufferedIOBase.close(self)
        self.iterator = iter(())

import threading
class FeederThread(threading.Thread):
    """ Thread feeding output of python iterable object into a pipe """

    def __init__(self, iterable):
        # turn it into a file-like object:
        if isinstance(iterable, basestring):
            from cStringIO import StringIO
            self._source = StringIO(iterable)
        else:
            self._source = io.BufferedReader(_RawIterIO(iterable))

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
        try:
            from fcntl import FD_CLOEXEC
        except AttributeError:
            FD_CLOEXEC = 1
        flags = fcntl.fcntl(self._writefd, fcntl.F_GETFD)
        fcntl.fcntl(self._writefd, fcntl.F_SETFD, flags | FD_CLOEXEC)

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
    def __getattr__(cls, name):
        if name.startswith('_'):
            raise AttributeError
        return Cmd(name)

class PipelineOps(object):
    def __or__(self, right):
        """ Append pipeline or stages """
        return Pipeline(self, right)

    def __ror__(self, left):
        """ Prepend pipelines or stages """
        return Pipeline(left, self)

    def __rshift__(self, right):
        ( Pipeline(self) | Pipeline(right) ).call()

    def __rrshift__(self, left):
        ( Pipeline(left) | Pipeline(self) ).call()

class Cmd(PipelineOps):
    """ Object describing an executable command """
    # Implementation: object attrs are names of arguments to Subprocess (i.e. subprocess.Popen)
    __metaclass__ = CmdMeta

    def __init__(self, *args, **kw):
        self.args = list(args)
        vars(self).update(kw)

    def __repr__(self):
        """ Make eval(repr(command)) == command """
        argrepr = [repr(a) for a in self.args] 
        argrepr += ["%s=%r" % (k,v) for k,v in sorted(vars(self).items()) if k != 'args']
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
        """ Bind args, start subprocess, wait for completion and return process returncode """
        return self(*args, **kw).subprocess().wait()

    # implements the iterator protocol 
    __iter__ = producer

    # implements writer protocol
    __writer__ = consumer

    # implements filter protocol
    def __filt__(self, upstream):
        return iter(self(stdin=upstream))


def filt(filter, upstream):
    """ Apply a filter to a stream """
    if hasattr(filter, '__filt__'):
        return filter.__filt__(upstream)            # apply filter to entire stream
    elif callable(filter):
        return (filter(x) for x in upstream)        # apply individually to each item in stream
    else:
        raise TypeError("%r object is not a valid filter" % filter.__class__.__name__)

class WriterWrapper:
    def __init__(self, callable):
        self.write = callable

    def close(self):
        pass

# Registry of object types which may be adated to writer protocol
writer_adapters = {
    list : lambda x : WriterWrapper(x.append),
    set  : lambda x : WriterWrapper(x.add),
}

def writer(target):
    """ The logical inverse of iter(). Returns object with .write() method. """
    if hasattr(target, '__writer__'):
        return target.__writer__()
    if hasattr(target, 'write') and hasattr(target, 'close'):
        return target
    if type(target) in writer_adapters:
        return writer_adapters[type(target)](target)
    elif callable(target):
        if hasattr(target, '__filter__'):
            raise TypeError("object supports filter interface and should not be used as a writer")
        return WriterWrapper(target)


class Pipeline(PipelineOps):
    """ Object representing data flow recipe 

    Complete pipeline:
    Pipeline(source, filter1, filter2, ..., target)
        May be executed with .call() for its side effects.

    Partial pipelines:

    Pipeline(source, filter1, filter2, ...)
        May be used as in iterable:
        l = list(pipeline)
        for x in pipeline:
        etc.

    Pipeline(filter1, filter2, ...)
        May be used as a filter:
        filt(pipeline, upstream)

    """

    def __init__(self, *stages):
        flattened = []
        for stage in stages:
            if stage.__class__ is Pipeline:
                flattened.extend(stage.stages)
            else:
                flattened.append(stage)
        self.stages = tuple(flattened)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, ', '.join(repr(s) for s in self.stages))

    def __iter__(self):
        """ Iterate first stage, applying the following stages as filters """
        if len(self.stages) == 0:
            return iter(())
        else:
            first, rest = self.stages[0], self.stages[1:]
            return filt(Pipeline(*rest), iter(first))

    def __filt__(self, upstream):
        """ Apply pipeline as a filter to specified upstream source """
        if len(self.stages) == 0:
            return upstream
        else:
            return filt(self.stages[-1], Pipeline(*(upstream,) + self.stages[:-1]))

    def into(self, target):
        # TODO: special case targets that can consume file descriptor
        w = writer(target)
        for x in self:
            w.write(x)
        w.close()

    def call(self):
        """ Run a pipeline for its side effects. Last stage must be adaptable to a writer """
        target = self.stages[-1]
        Pipeline(*self.stages[:-1]).into(target)

class File:
    def __init__(self, filename):
        self.filename = filename

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.filename)

    def __iter__(self):
        return open(self.filename)

    def __writer__(self):
        return open(self.filename, 'w')
