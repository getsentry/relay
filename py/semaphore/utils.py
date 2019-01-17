import uuid
import weakref
from semaphore._lowlevel import ffi, lib
from semaphore._compat import text_type, with_metaclass
from semaphore.exceptions import exceptions_by_code, SemaphoreError


attached_refs = weakref.WeakKeyDictionary()


class _NoDict(type):
    def __new__(cls, name, bases, d):
        d.setdefault("__slots__", ())
        return type.__new__(cls, name, bases, d)


def rustcall(func, *args):
    """Calls rust method and does some error handling."""
    lib.semaphore_err_clear()
    rv = func(*args)
    err = lib.semaphore_err_get_last_code()
    if not err:
        return rv
    msg = lib.semaphore_err_get_last_message()
    cls = exceptions_by_code.get(err, SemaphoreError)
    exc = cls(decode_str(msg))
    backtrace = decode_str(lib.semaphore_err_get_backtrace())
    if backtrace:
        exc.rust_info = backtrace
    raise exc


class RustObject(with_metaclass(_NoDict)):
    __slots__ = ["_objptr", "_shared"]
    __dealloc_func__ = None

    def __init__(self):
        raise TypeError("Cannot instanciate %r objects" % self.__class__.__name__)

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
            raise RuntimeError("Object is closed")
        return self._objptr

    def __del__(self):
        if rustcall is None:
            # Interpreter is shutting down and our memory management utils are
            # gone. Just give up, the process is going away anyway.
            return

        if self._objptr is None or self._shared:
            return
        f = self.__class__.__dealloc_func__
        if f is not None:
            rustcall(f, self._objptr)
            self._objptr = None


def decode_str(s, free=False):
    """Decodes a SymbolicStr"""
    try:
        if s.len == 0:
            return u""
        return ffi.unpack(s.data, s.len).decode("utf-8", "replace")
    finally:
        if free:
            lib.semaphore_str_free(ffi.addressof(s))


def encode_str(s, mutable=False):
    """Encodes a SemaphoreStr"""
    rv = ffi.new("SemaphoreStr *")
    if isinstance(s, text_type):
        s = s.encode("utf-8")
    if mutable:
        s = bytearray(s)
    rv.data = ffi.from_buffer(s)
    rv.len = len(s)
    # we have to hold a weak reference here to ensure our string does not
    # get collected before the string is used.
    attached_refs[rv] = s
    return rv


def make_buf(value):
    buf = memoryview(bytes(value))
    rv = ffi.new("SemaphoreBuf *")
    rv.data = ffi.from_buffer(buf)
    rv.len = len(buf)
    attached_refs[rv] = buf
    return rv


def decode_uuid(value):
    """Decodes the given uuid value."""
    return uuid.UUID(bytes=bytes(bytearray(ffi.unpack(value.data, 16))))
