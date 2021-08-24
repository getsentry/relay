from sentry_relay._lowlevel import lib
from sentry_relay.utils import encode_str, rustcall


def to_metrics_symbol(value: str) -> int:
    """Convert a metrics name, tag key or tag value to an integer"""
    return rustcall(lib.relay_to_metrics_symbol, encode_str(value))
