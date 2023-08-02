# auto-generated file
__all__ = ['lib', 'ffi']

import os
from sentry_relay._lowlevel__ffi import ffi

lib = ffi.dlopen(os.path.join(os.path.dirname(__file__), '_lowlevel__lib.so'), 130)
del os
