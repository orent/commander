"""
Extensions to subprocess module

TODO:
exception propagation
serialized safe wrapper threads
broken pipe signal
exit status errorlevel checks
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
        self.source = make_readable(obj)
        self.exc_info = None
        threading.Thread.__init__(self)

    def fileno(self):
        """ File descriptor through which data may be read. """
        self._initthread()
        return self._readfd

    def close(self):
        if self.exc_info:
            type, value, traceback = self.exc_info
            raise type, value, traceback

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

        # Get pipe buffer size (not critical, but nice for performance)
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
                    buf = self.source.read(self._bufsize)
                except Exception:
                    self._pending_exception = sys.exc_info()
                    break
                if not buf:
                    break
                try:
                    os.write(self._writefd, buf)
                except OSError:
                    return
        finally:
            os.close(self._writefd)
            self.source.close()

__all__ = [
    'PIPE', 'STDOUT',
    'Subprocess', 'Producer', 'Consumer',
]
