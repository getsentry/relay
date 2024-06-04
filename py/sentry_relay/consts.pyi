from enum import IntEnum

SPAN_STATUS_CODE_TO_NAME: dict[int, str]
SPAN_STATUS_NAME_TO_CODE: dict[str, int]

class DataCategory(IntEnum):
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
    METRIC_HOUR = 19
    UNKNOWN = -1

    @staticmethod
    def parse(name: str | None = None) -> DataCategory | None:
        """
        Parses a `DataCategory` from its API name.
        """
        ...

    @staticmethod
    def from_event_type(event_type: str | None = None) -> DataCategory:
        """
        Parses a `DataCategory` from an event type.
        """
        ...

    @staticmethod
    def event_categories() -> list[DataCategory]:
        """
        Returns categories that count as events, including transactions.
        """
        ...

    @staticmethod
    def error_categories() -> list[DataCategory]:
        """
        Returns categories that count as traditional error tracking events.
        """
        ...

    def api_name(self) -> str:
        """
        Returns the API name of the given `DataCategory`.
        """
        ...
