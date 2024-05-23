from datetime import timedelta, datetime, timezone


class _WithinBounds:

    def __init__(self, lower_bound, upper_bound):
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound

    def __eq__(self, other):
        assert isinstance(other, int)
        return self._lower_bound <= other <= self._upper_bound

    def __str__(self):
        return f"{self._lower_bound} <= x <= {self._upper_bound}"


def time_after(lower_bound):
    upper_bound = int(datetime.now(tz=timezone.utc).timestamp())
    return time_within(lower_bound, upper_bound)


def time_within(lower_bound, upper_bound):
    assert lower_bound <= upper_bound
    return _WithinBounds(lower_bound, upper_bound)


def time_within_delta(timestamp, delta=None):
    if delta is None:
        delta = timedelta(seconds=5)

    lower_bound = (datetime.fromtimestamp(timestamp) - delta).timestamp()
    upper_bound = (datetime.fromtimestamp(timestamp) + delta).timestamp()

    return _WithinBounds(lower_bound, upper_bound)
