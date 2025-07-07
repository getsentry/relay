from typing import Callable
from .time import time_after, time_within, time_within_delta

__all__ = ["time_after", "time_within", "time_within_delta", "matches"]


class _Matches(object):
    def __init__(self, compare_with: Callable[[object], bool]):
        self._compare_with = compare_with

    def __eq__(self, value: object) -> bool:
        return self._compare_with(value)

    def __ne__(self, value: object) -> bool:
        return not self.__eq__(value)

    def __repr__(self):
        return f"matches<{self._compare_with}>"


def matches(f: Callable[[object], bool]):
    """
    Helper to dynamically assert values with a function.

    Example::

        assert foo == {
            "value": matches(lambda x: 3 <= x <= 5)
        }
    """
    return _Matches(f)
