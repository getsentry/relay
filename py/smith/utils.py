import io
import uuid
import ntpath
import weakref
import posixpath
from smith._lowlevel import ffi, lib
from smith._compat import text_type
from smith.exceptions import exceptions_by_code, SmithError


attached_refs = weakref.WeakKeyDictionary()


class RustObject(object):
    __dealloc_func__ = None
    _objptr = None
    _shared = False

    def __init__(self):
        raise TypeError('Cannot instanciate %r objects' %
                        self.__class__.__name__)

    @classmethod
    def _from_objptr(cls, ptr, shared=False):
        rv = object.__new__(cls)
        rv._objptr = ptr
        rv._shared = shared
        return rv

    def _methodcall(self, func, *args):
        return rustcall(func, self._get_objptr(), *args)

    def _get_objptr(self):
        if not self._objptr:
            raise RuntimeError('Object is closed')
        return self._objptr

    def __del__(self):
        if self._objptr is None or self._shared:
            return
        f = self.__class__.__dealloc_func__
        if f is not None:
            rustcall(f, self._objptr)
            self._objptr = None


def rustcall(func, *args):
    """Calls rust method and does some error handling."""
    lib.smith_err_clear()
    rv = func(*args)
    err = lib.smith_err_get_last_code()
    if not err:
        return rv
    msg = lib.smith_err_get_last_message()
    cls = exceptions_by_code.get(err, SmithError)
    exc = cls(decode_str(msg))
    backtrace = decode_str(lib.smith_err_get_backtrace())
    if backtrace:
        exc.rust_info = backtrace
    raise exc


def decode_str(s, free=False):
    """Decodes a SymbolicStr"""
    try:
        if s.len == 0:
            return u''
        return ffi.unpack(s.data, s.len).decode('utf-8', 'replace')
    finally:
        if free:
            lib.smith_str_free(ffi.addressof(s))


def encode_str(s):
    """Encodes a SmithStr"""
    rv = ffi.new('SmithStr *')
    if isinstance(s, text_type):
        s = s.encode('utf-8')
    rv.data = ffi.from_buffer(s)
    rv.len = len(s)
    # we have to hold a weak reference here to ensure our string does not
    # get collected before the string is used.
    attached_refs[rv] = s
    return rv
