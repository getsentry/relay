from enum import IntEnum

from sentry_relay._lowlevel import lib
from sentry_relay.utils import decode_str, encode_str


__all__ = ["DataCategory", "SPAN_STATUS_CODE_TO_NAME", "SPAN_STATUS_NAME_TO_CODE"]


def _make_data_categories(ns):
    prefix = "RELAY_DATA_CATEGORY_"

    for attr in dir(lib):
        if attr.startswith(prefix):
            category_name = attr[len(prefix) :]
            ns[category_name] = getattr(lib, attr)


class DataCategory(IntEnum):
    _make_data_categories(locals())

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
