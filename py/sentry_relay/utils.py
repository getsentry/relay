from __future__ import annotations

import os
import uuid
import weakref
from sentry_relay._lowlevel import ffi, lib
from sentry_relay.exceptions import exceptions_by_code, RelayError


attached_refs: weakref.WeakKeyDictionary[object, bytes]
attached_refs = weakref.WeakKeyDictionary()


lib.relay_init()
os.environ["RUST_BACKTRACE"] = "1"


class _NoDict(type):
    def __new__(cls, name, bases, d):
        d.setdefault("__slots__", ())
        return type.__new__(cls, name, bases, d)


def rustcall(func, *args):
    """Calls rust method and does some error handling."""
    lib.relay_err_clear()
    rv = func(*args)
    err = lib.relay_err_get_last_code()
    if not err:
        return rv
    msg = lib.relay_err_get_last_message()
    cls = exceptions_by_code.get(err, RelayError)
    exc = cls(decode_str(msg, free=True))
    backtrace = decode_str(lib.relay_err_get_backtrace(), free=True)
    if backtrace:
        exc.rust_info = backtrace
    raise exc


class RustObject(metaclass=_NoDict):
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
    """Decodes a RelayStr"""
    try:
        if s.len == 0:
            return ""
        return ffi.unpack(s.data, s.len).decode("utf-8", "replace")
    finally:
        if free and s.owned:
            lib.relay_str_free(ffi.addressof(s))


def encode_str(s, mutable=False):
    """Encodes a RelayStr"""
    rv = ffi.new("RelayStr *")
    if isinstance(s, str):
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
    rv = ffi.new("RelayBuf *")
    rv.data = ffi.from_buffer(buf)
    rv.len = len(buf)
    attached_refs[rv] = buf
    return rv


def decode_uuid(value):
    """Decodes the given uuid value."""
    return uuid.UUID(bytes=bytes(bytearray(ffi.unpack(value.data, 16))))
