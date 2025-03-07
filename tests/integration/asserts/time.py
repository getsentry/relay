from datetime import timedelta, datetime, timezone
from enum import StrEnum


class Resolution(StrEnum):
    Seconds = "s"
    MilliSeconds = "ms"
    MicroSeconds = "us"
    NanoSeconds = "ns"


def _to_datetime(v, resolution=Resolution.Seconds):
    if isinstance(v, datetime):
        return v
    elif isinstance(v, (int, float)):
        resolution = Resolution(resolution)
        to_secs_d = {
            Resolution.Seconds: 1,
            Resolution.MilliSeconds: 1_000,
            Resolution.MicroSeconds: 1_000_000,
            Resolution.NanoSeconds: 1_000_000_000,
        }[resolution]
        return datetime.fromtimestamp(v / to_secs_d, timezone.utc)
    elif isinstance(v, str):
        return datetime.fromisoformat(v)
    else:
        assert False, f"cannot convert {v} to datetime"


class _WithinBounds:
    def __init__(self, lower_bound, upper_bound, expect_resolution=Resolution.Seconds):
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        self._expect_resolution = expect_resolution

    def __eq__(self, other):
        other = _to_datetime(other, resolution=self._expect_resolution)
        return self._lower_bound <= other <= self._upper_bound

    def __str__(self):
        return f"{self._lower_bound} <= x <= {self._upper_bound}"

    def __repr__(self) -> str:
        return str(self)


def time_after(lower_bound, **kwargs):
    upper_bound = datetime.now(tz=timezone.utc)
    return time_within(lower_bound, upper_bound, **kwargs)


def time_within(lower_bound, upper_bound, **kwargs):
    lower_bound = _to_datetime(lower_bound)
    upper_bound = _to_datetime(upper_bound)
    assert lower_bound <= upper_bound
    return _WithinBounds(lower_bound, upper_bound, **kwargs)


def time_within_delta(time=None, delta=timedelta(seconds=30), **kwargs):
    time = _to_datetime(time) if time is not None else datetime.now(tz=timezone.utc)
    return _WithinBounds(time - delta, time + delta, **kwargs)
