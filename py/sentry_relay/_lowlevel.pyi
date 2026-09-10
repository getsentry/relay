# Type stub for the cffi-generated native module `sentry_relay._lowlevel`.
#
# At runtime `_lowlevel.py` does `lib = ffi.dlopen(...)`, so `lib`/`ffi` are
# populated dynamically from the Rust; their attributes cannot be known statically.
# Without this stub, mypy types `lib` as cffi's generic `_cffi_backend.Lib`,
# which declares none of the `relay_*` functions and so generates errors.
# Here, we type them as Any to avoid these errors.
from typing import Any

ffi: Any
lib: Any
