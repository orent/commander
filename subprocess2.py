"""
Extensions to subprocess module

TODO:
serialized safe wrapper threads
broken pipe signal
"""

import subprocess, io, os, threading
from subprocess import PIPE, STDOUT

class Subprocess(subprocess.Popen):
    """ Similar to subprocess.Popen with the following enhancement:
        * stdin may be another subprocess (producer) or any python iterable
        * errorlevel argument may be set to set threshold for converting exit codes to exceptions 
    """
    def __init__(self, args, **kw):
        if 'stdin' in kw:
            kw['stdin'] = self._asfiledesc(kw['stdin'])
        self._errorlevel = kw.pop('errorlevel', None)
        self._cmd = subprocess.list2cmdline(args)[:200]
        subprocess.Popen.__init__(self, args=args, text=True, **kw)

    def _handle_exitstatus(self, sts):
        subprocess.Popen._handle_exitstatus(self, sts)
        if self._errorlevel is not None:
            if self.returncode >= self._errorlevel or self.returncode < 0:
                raise subprocess.CalledProcessError(self.returncode, self._cmd)

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
            return iterator                 # (e.g. file, urlopen, producer)
        else:
            return Iter2Pipe(iterator)      # no, wrap in bridge thread

class Producer(Subprocess, io.TextIOWrapper):
    """ Exposes a readable file-like interface to stdout of a subprocess """

    encoding = property(io.TextIOWrapper.encoding.__get__, lambda *args: None)
    errors   = property(io.TextIOWrapper.errors  .__get__, lambda *args: None)

    def __init__(self, args, **kw):
        if 'stdout' in kw:
            raise ValueError("Producer: stdout already overridden")
        Subprocess.__init__(self, args, stdout=subprocess.PIPE, **kw)
        self._orig_stdout = f = self.stdout
        self.stdout = None
        io.TextIOWrapper.__init__(
            self,
            buffer=f.buffer, 
            encoding=f.encoding, 
            errors=f.errors,
            newline=f.newlines,
            line_buffering=f.line_buffering,
            write_through=f.write_through
        )


class Consumer(Subprocess, io.TextIOWrapper):
    """ Exposes a writable file-like interface to stdin of a subprocess """
    def __init__(self, args, stdin=None, **kw):
        if stdin is not None:
            raise ValueError("Consumer: stdin already overridden")
        Subprocess.__init__(self, args, stdin=subprocess.PIPE, **kw)
        f = self.stdin
        self.stdin = None
        io.TextIOWrapper.__init__(
            self,
            buffer=f.buffer, 
            encoding=f.encoding, 
            errors=f.errors,
            newline=f.newline,
            line_buffering=f.line_buffering,
            write_through=f.write_through
        )


class _RawIterIO(io.TextIOWrapper):
    """ Helper class for turning python iterator to a file-like object """
    def __init__(self, iterable):
        self.iterator = self.sourcereader(iterable)
        self.remainder = None

    def sourcereader(self, iterable):
        for x in iterable:
            if not isinstance(x, str):
                x = '%s\n' % x
            yield x
        iterable = None     # let gc do its work
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
        io.TextIOWrapper.close(self)
        self.iterator = None


def make_readable(obj):
    """ Adapt object into something that has a .read() method """
    if hasattr(obj, 'read'):
        return obj
    elif isinstance(obj, str):
        from cStringIO import StringIO
        return StringIO(obj)
    else:
        # assume it's somehow iterable:
        return io.TextIOWrapper(_RawIterIO(obj))


class Iter2Pipe(threading.Thread):
    """ Bridge thread from python iterator to a pipe """

    def __init__(self, obj):
        self.source = make_readable(obj)
        self._pending_exception = None
        threading.Thread.__init__(self)

    def fileno(self):
        """ File descriptor through which iterator data may be read. """
        if not self.is_alive():
            self._initthread()
        return self._readfd

    def close(self):
        # Propagate iteration raised in thread:
        if self._pending_exception:
            etype, evalue, traceback = self._pending_exception
            self._pending_exception = None
            try:
                raise evalue.with_traceback(traceback)
            except AttributeError:
                exec('raise etype, evalue, traceback')

    __del__ = close

    ### internal methods:

    def _initthread(self):
        """ Create thread to read from iterable and write to pipe """
        self._readfd, self._writefd = os.pipe()

        # ensure write side of pipe is not inherited by child process
        import fcntl
        flags = fcntl.fcntl(self._writefd, fcntl.F_GETFD)
        fcntl.fcntl(self._writefd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)

        # Get pipe buffer size
        try:
            self._bufsize = os.fstat(self._writefd).st_blksize
        except (os.error, AttributeError):
            self._bufsize = 2048   # too small better than too big - may cause blocking
        self.start()

    def run(self):
        """ Thread main function """
        import sys
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
