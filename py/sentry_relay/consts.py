import sys
from enum import IntEnum

from sentry_relay._lowlevel import lib
from sentry_relay.utils import decode_str, encode_str

__all__ = ["DataCategory", "SPAN_STATUS_CODE_TO_NAME", "SPAN_STATUS_NAME_TO_CODE"]


class DataCategory(IntEnum):
    # begin generated
    DEFAULT = 0
    ERROR = 1
    TRANSACTION = 2
    SECURITY = 3
    ATTACHMENT = 4
    SESSION = 5
    PROFILE = 6
    REPLAY = 7
    TRANSACTION_PROCESSED = 8
    TRANSACTION_INDEXED = 9
    MONITOR = 10
    PROFILE_INDEXED = 11
    SPAN = 12
    MONITOR_SEAT = 13
    USER_REPORT_V2 = 14
    METRIC_BUCKET = 15
    SPAN_INDEXED = 16
    PROFILE_DURATION = 17
    PROFILE_CHUNK = 18
    METRIC_SECOND = 19
    REPLAY_VIDEO = 20
    UNKNOWN = -1
    # end generated

    @classmethod
    def parse(cls, name):
        """
        Parses a `DataCategory` from its API name.
        """
        category = DataCategory(lib.relay_data_category_parse(encode_str(name or "")))
        if category == DataCategory.UNKNOWN:
            return None  # Unknown is a Rust-only value, replace with None
        return category

    @classmethod
    def from_event_type(cls, event_type):
        """
        Parses a `DataCategory` from an event type.
        """
        s = encode_str(event_type or "")
        return DataCategory(lib.relay_data_category_from_event_type(s))

    @classmethod
    def event_categories(cls):
        """
        Returns categories that count as events, including transactions.
        """
        return [
            DataCategory.DEFAULT,
            DataCategory.ERROR,
            DataCategory.TRANSACTION,
            DataCategory.SECURITY,
            DataCategory.USER_REPORT_V2,
        ]

    @classmethod
    def error_categories(cls):
        """
        Returns categories that count as traditional error tracking events.
        """
        return [DataCategory.DEFAULT, DataCategory.ERROR, DataCategory.SECURITY]

    def api_name(self):
        """
        Returns the API name of the given `DataCategory`.
        """
        return decode_str(lib.relay_data_category_name(self.value), free=True)


def _check_generated():
    prefix = "RELAY_DATA_CATEGORY_"

    attrs = {}
    for attr in dir(lib):
        if attr.startswith(prefix):
            category_name = attr[len(prefix) :]
            attrs[category_name] = getattr(lib, attr)

    if attrs != DataCategory.__members__:
        values = sorted(
            attrs.items(), key=lambda kv: sys.maxsize if kv[1] == -1 else kv[1]
        )
        generated = "".join(f"    {k} = {v}\n" for k, v in values)
        raise AssertionError(
            f"DataCategory enum does not match source!\n\n"
            f"Paste this into `class DataCategory` in py/sentry_relay/consts.py:\n\n"
            f"{generated}"
        )


_check_generated()

SPAN_STATUS_CODE_TO_NAME = {}
SPAN_STATUS_NAME_TO_CODE = {}


def _make_span_statuses():
    prefix = "RELAY_SPAN_STATUS_"

    for attr in dir(lib):
        if not attr.startswith(prefix):
            continue

        status_name = attr[len(prefix) :].lower()
        status_code = getattr(lib, attr)

        SPAN_STATUS_CODE_TO_NAME[status_code] = status_name
        SPAN_STATUS_NAME_TO_CODE[status_name] = status_code

    # Legacy alias
    SPAN_STATUS_NAME_TO_CODE["unknown_error"] = SPAN_STATUS_NAME_TO_CODE["unknown"]


_make_span_statuses()
