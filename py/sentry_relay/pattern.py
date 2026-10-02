from __future__ import annotations

from sentry_relay._lowlevel import lib
from sentry_relay.utils import RustObject, decode_str, encode_str, rustcall

__all__ = ["Pattern"]


class Pattern(RustObject):
    """A Relay pattern."""

    __dealloc_func__ = lib.relay_pattern_free
    __init__ = object.__init__

    def __new__(
        cls,
        pattern: str,
        *,
        case_insensitive: bool = False,
        max_complexity: int | None = None,
    ) -> Pattern:
        if max_complexity is None:
            max_complexity = (1 << 64) - 1
        return cls._from_objptr(
            rustcall(
                lib.relay_pattern_new,
                encode_str(pattern),
                case_insensitive,
                max_complexity,
            )
        )

    def is_match(self, haystack: str) -> bool:
        """Returns whether the pattern matches the passed string."""
        return self._methodcall(lib.relay_pattern_is_match, encode_str(haystack))

    def __str__(self) -> str:
        return decode_str(self._methodcall(lib.relay_pattern_to_string), free=True)
