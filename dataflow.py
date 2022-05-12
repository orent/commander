"""
The dataflow toolkit is based on the following three protocols:

1. iterable/iterator protocol
The standard Python protocol for iteration over the items of a container:
    assource(object)) -> Iterable       - if not already Iterable
    iter(Iterable) -> Iterator

2. filter protocol
Applies a filter to an upstream iterator, returning output iterator
    asfilter(object) -> Filter          - if not already a Filter
    filt(Filter, Iterator) -> Iterator

    A filter may be:
    * An object with a __filt__ method. Applied to entire stream.
    * A callable object. Applied individually to each item in the stream.

3. target/receiver protocol
Feeds a source iterator into a data target.
    asfeeder(object) -> Feeder          - if not already a Feeder
    feed(Feeder, Iterator)

    A target may be:
    * An object with a __feed__ method
    * A callable object. Will be invoked for each item in stream.
    * A file-like object with a .write() method.
    * Special cases for list, set, None

A Dataflow object is a recipe for combinations of the above
protocols. This means that a Dataflow is a passive object that
does not actually do anything to its arguments until stated.

A dataflow uses all three protocols and usually makes it

nnecessary to directly use the functions iter, filt or feed.
Starting a dataflow is done by iteration or using the >> operator.
The // operator concatenates dataflows and dataflow stages. Sorry,
but the operator precedence of | is too low which makes it awkward
to use and requires parentheses.

The following statements are equivalent:
    for x in filt(f2, filt(f1, iter(src))):
    for x in Dataflow(src, f1, f2):
    for x in Dataflow(src) // Dataflow(f1) // Dataflow(f2):
    for x in Dataflow(src) // f1 // f2:
    for x in src // f1 // f2:

    (last one works if either src or f1 are derived from DataflowOps)

The following statements are also equivalent:
    feed(target, filt(f2, filt(f1, iter(src))))
    feed(target, Dataflow(src, f1, f2))
    Dataflow(src, f1, f2, target)()
    Dataflow(src, f1, f2) >> target
    Dataflow(src) // f1 // f2 >> target
    src // f1 // f2 >> target

    (again, last one requires src or f1 to have DataflowOps)

Note that the dataflow direction is from left to right while chaining
of function calls generally works right to left.

TODO: Allow last and second-to-last dataflow stages to collude in bypassing
iterator protocol (e.g. target is writable file, prev stage is a subprocess)
"""

from functools import singledispatch
from collections.abc import *
import abc

class DataflowOps(object):
    """ Adds // and >> dataflow operators to an object """
    def __floordiv__(self, right):
        """ Concatenate dataflows or stages """
        return Dataflow(self, right)

    def __rfloordiv__(self, left):
        """ Concatenate to dataflow on the right """
        return Dataflow(left, self)

    def __rshift__(self, right):
        """ Concatenate and start dataflow """
        ( Dataflow(self) // Dataflow(right) )()

    def __rrshift__(self, left):
        ( Dataflow(left) // Dataflow(self) )()


def filt(filter, upstream):
    """ Apply a filter to an iterator """
    if hasattr(filter, '__filt__'):
        # apply filter to entire stream:
        return filter.__filt__(upstream)
    elif callable(filter):
        # apply filter individually to each item in stream
        return (filter(x) for x in upstream)
    else:
        raise TypeError("filt: %r object is not a valid filter"
                % filter.__class__.__name__)


def feed(target, source):
    """ Feed source iterator into a data target """
    if hasattr(target, '__feed__'):
        return target.__feed__(source)
    elif type(target) in feed_registry:
        feed_registry[type(target)](target, source)
    elif callable(target):
        # feed items individually
        for x in source:
            target(x)
    elif hasattr(target, 'write'):
        # feed items individually, converting to string first
        for x in source:
            if not isinstance(x, str):
                x = '%s\n' % x
            target.write(x)
    else:
        raise TypeError("target: %r object is not a valid data target"
                % target.__class__.__name__)

class Feedable(abc.ABC):
    @classmethod
    def __subclasshook__(cls, x):
        if hasattr(x, '__feeder__'):
            return True
        return NotImplemented

@singledispatch
def feeder(o):
    raise TypeError("target: %r object is not a valid data target"
            % o.__class__.__name__)

@feeder.register(Callable)
def feeder_callable(c):
    yield None
    while True:
        x = yield
        c(x)

@feeder.register(Feedable)
def feeder_feedable(f):
    return f.__feeder__()

@feeder.register(list)
def feeder_list(l):
    """ Replace list contents with iterable source """
    yield None
    l[:] = ()
    while True:
        x = yield
        l.append(x)

@feeder.register(set)
def feeder_set(s):
    """ Replace set contents with iterable source"""
    yield None
    s.clear()
    while True:
        x = yield
        s.add(x)

@feeder.register(type(None))
def feed_none(n):
    """ Feed iterable source to the bit bucket """
    while True:
        yield


class Dataflow(DataflowOps):
    """ Object representing recipe for a data flow """

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
        """ Iterate first stage, applying the next stages as filters """
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

    def __call__(self):
        """ Run dataflow. Last stage must be a valid target. """
        target = self.stages[-1]
        feed(target, Dataflow(*self.stages[:-1]) )


class File(DataflowOps):
    """ Class representing a file on the disk """
    def __init__(self, filename, append=False):
        self.filename = filename
        self.appendmode = append

    def __repr__(self):
        return "%s(%r%s)" % (self.__class__.__name__, self.filename, ', append=True' if self.appendmode else '')

    def __iter__(self):
        return open(self.filename)

    def __feed__(self, source):
        feed(open(self.filename, 'a' if self.appendmode else 'w'), source)

    @property
    def append(self):
        return type(self)(self.filename, append=True)

class URL(DataflowOps):
    """ Class representing a URL. Read only. """
    def __init__(self, url):
        self.url = url

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.url)

    def __iter__(self):
        import urllib2
        return urllib2.urlopen(self.url)

class Filter(DataflowOps):
    """ Decorator to make a callable object a whole-stream filter rather
    than a per-item filter """
    def __init__(self, callable):
        self.callable = callable

    def __repr__(self):
        return "Filter(%r)" % self.callable

    def __filt__(self, upstream):
        return self.callable(upstream)

@Filter
def uniq(upstream):
    """ Sample filter """
    last = object()
    for x in upstream:
        if x != last:
            yield x
        last = x

def nl(x):
    """ Convert to string and add a newline, if necessary """
    if type(x) is str and x.endswith('\n'):
        return x
    else:
        return '%s\n' % x

def stripnl(x):
    """ Remove trailing newline """
    return x.rstrip('\n')


__all__ = ['Dataflow', 'filt', 'feed', 'File', 'URL', 'uniq', 'nl', 'stripnl']
