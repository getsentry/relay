from datetime import timedelta, datetime, timezone


def _to_datetime(v):
    if isinstance(v, datetime):
        return v
    elif isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, timezone.utc)
    elif isinstance(v, str):
        return datetime.fromisoformat(v)
    else:
        assert False, f"cannot convert {v} to datetime"


class _WithinBounds:
    def __init__(self, lower_bound, upper_bound):
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def __eq__(self, other):
        other = _to_datetime(other)
        return self._lower_bound <= other <= self._upper_bound

    def __str__(self):
        return f"{self._lower_bound} <= x <= {self._upper_bound}"

    def __repr__(self) -> str:
        return str(self)


def time_after(lower_bound):
    upper_bound = datetime.now(tz=timezone.utc)
    return time_within(lower_bound, upper_bound)


def time_within(lower_bound, upper_bound):
    lower_bound = _to_datetime(lower_bound)
    upper_bound = _to_datetime(upper_bound)
    assert lower_bound <= upper_bound
    return _WithinBounds(lower_bound, upper_bound)


def time_within_delta(time=None, delta=timedelta(seconds=30)):
    time = _to_datetime(time) if time is not None else datetime.now(tz=timezone.utc)
    return _WithinBounds(time - delta, time + delta)
