from sentry_relay._compat import implements_to_string
from sentry_relay._lowlevel import lib


__all__ = ["RelayError"]
exceptions_by_code = {}


@implements_to_string
class RelayError(Exception):
    code = None

    def __init__(self, msg):
        Exception.__init__(self)
        self.message = msg
        self.rust_info = None

    def __str__(self):
        rv = self.message
        if self.rust_info is not None:
            return "%s\n\n%s" % (rv, self.rust_info)
        return rv


def _make_error(error_name, base=RelayError, code=None):
    class Exc(base):
        pass

    Exc.__name__ = error_name
    Exc.__qualname__ = error_name
    if code is not None:
        Exc.code = code
    globals()[Exc.__name__] = Exc
    __all__.append(Exc.__name__)
    return Exc


def _get_error_base(error_name):
    pieces = error_name.split("Error", 1)
    if len(pieces) == 2 and pieces[0] and pieces[1]:
        base_error_name = pieces[0] + "Error"
        base_class = globals().get(base_error_name)
        if base_class is None:
            base_class = _make_error(base_error_name)
        return base_class
    return RelayError


def _make_exceptions():
    prefix = "RELAY_ERROR_CODE_"
    for attr in dir(lib):
        if not attr.startswith(prefix):
            continue

        error_name = attr[len(prefix) :].title().replace("_", "")
        base = _get_error_base(error_name)
        exc = _make_error(error_name, base=base, code=getattr(lib, attr))
        exceptions_by_code[exc.code] = exc


_make_exceptions()
