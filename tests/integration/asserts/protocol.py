from typing import Any, List


def _item_matcher(item):
    if isinstance(item, str):
        return _ItemType(item)

    # Assume it's a different matcher
    return item


class _AnyMatcher:
    def __init__(self, expected: List[Any]) -> None:
        self._expected = expected

    def __eq__(self, value: object, /) -> bool:
        return any(expected == value for expected in self._expected)

    def __str__(self) -> str:
        if len(self._expected) == 1:
            return str(self._expected[0])

        a = ", ".join(str(m) for m in self._expected)
        return f"Any({a})"

    def __repr__(self) -> str:
        return str(self)


class _ItemType:
    def __init__(self, expected: str) -> None:
        self._expected = expected

    def __eq__(self, value) -> bool:
        print(value)
        return value.type == self._expected

    def __str__(self) -> str:
        return f"item.type:{self._expected}"

    def __repr__(self) -> str:
        return str(self)


class _Envelope:
    def __init__(self, *, items) -> None:
        self._items = items

    def __eq__(self, other):
        if other is None:
            return False

        return all(self._items == item for item in other.items)

    def __str__(self) -> str:
        return f"Envelope(items=all({self._items}))"

    def __repr__(self) -> str:
        return str(self)


def only_items(*args):
    """
    Helper for asserting envelopes.

    Envelope must only contain items which match the passed matchers.
    """
    return _Envelope(items=_AnyMatcher([_item_matcher(arg) for arg in args]))
