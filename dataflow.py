"""
The dataflow toolkit is based on the following three protocols:

1. iterator protocol
The standard Python protocol for iteration over the items of a container:
    iter(container) -> iterator

2. filter protocol
Applies a filter to an upstream iterator, returning output iterator
    filt(filter, upstream) -> iterator

    A filter may be:
    * An object with a __filt__ method. Applied to entire stream.
    * A callable object. Applied individually to each item in the stream.

3. feed protocol
Feeds a source iterator into a data sink.

    feed(sink, source)

    A sink may be:
    * An object with a __feed__ method
    * A callable object. Will be invoked for each item in stream.
    * A file-like object with a .write() method.
    * Special cases for list, set, None

A Dataflow object is a recipe for combinations of the above
protocols. This means that a Dataflow is a passive object that
does not actually do anything to its arguments until stated.

A dataflow implements all three protocols and usually makes it
unnecessary to directly use the functions iter, filt and feed.
Starting a dataflow is done by iteration or using the >> operator.
The / operator concatenates dataflows and dataflow stages.

The following statements are equivalent:
    for x in filt(f2, filt(f1, iter(src))):
    for x in Dataflow(src, f1, f2):
    for x in Dataflow(src) / Dataflow(f1) / Dataflow(f2):
    for x in Dataflow(src) / f1 / f2:
    for x in src / f1 / f2:

    (last one works if either src or f1 are derived from DataflowOps)

The following statements are also equivalent:
    feed(target, filt(f2, filt(f1, iter(src))))
    feed(target, Dataflow(src, f1, f2))
    Dataflow(src, f1, f2, target).call()
    Dataflow(src, f1, f2) >> target
    Dataflow(src) / f1 / f2 >> target
    src / f1 / f2 >> target

    (again, last one requires src or f1 to have DataflowOps)

Note that the dataflow is from left to right while chaining of function
calls generally works right to left.

"""

class DataflowOps(object):
    """ Adds / and >> dataflow operators to an object
    Sorry, no | operator.
    It has lower precedence than >> which makes it awkward to use.
    """
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


__all__ = ['Dataflow', 'filt', 'feed', 'File', 'uniq', 'nl', 'stripnl']
