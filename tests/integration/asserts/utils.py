import functools


class PrintLastCompare(object):
    """
    Meta class which remembers the last comparison result to minimize differences in pytest's diff output.
    """

    def __init_subclass__(cls, **kwargs) -> None:
        super().__init_subclass__(**kwargs)

        def _wrap_eq(fn):
            @functools.wraps(fn)
            def wrapped(self, value, /):
                self._last_compare_value = value
                self._last_compare_is_eq = fn(self, value)
                return self._last_compare_is_eq

            return wrapped

        if hasattr(cls, "__eq__"):
            setattr(cls, "__eq__", _wrap_eq(cls.__eq__))

        def _wrap_ne(fn):
            @functools.wraps(fn)
            def wrapped(self, value, /):
                self._last_compare_value = value
                self._last_compare_is_eq = not fn(self, value)
                return not self._last_compare_is_eq

            return wrapped

        if hasattr(cls, "__ne__"):
            setattr(cls, "__ne__", _wrap_ne(cls.__ne__))

        # This depends on pytest internals, should this ever break, a simple alternative is to instead override
        # repr of `cls` to return `repr(self._last_compare_value)`. This will not work for anything where pytest
        # has custom formatters, but for all primitives this will work.
        def _dispatch_last_compare(
            printer, obj, stream, indent, allowance, context, level
        ):
            # Since cls.__repr__ points to this function and we want to delegate to the original
            # repr implementation, we'll have to use a wrapper such as `_NoopRepr` to break the recursion.
            p = _NoopRepr(obj)
            if getattr(obj, "_last_compare_is_eq", False):
                p = obj._last_compare_value
            printer._format(p, stream, indent, allowance, context, level)

        from _pytest._io.pprint import PrettyPrinter

        PrettyPrinter._dispatch[cls.__repr__] = _dispatch_last_compare


class _NoopRepr(object):
    def __init__(self, delegate):
        self.delegate = delegate

    def __repr__(self) -> str:
        return repr(self.delegate)
