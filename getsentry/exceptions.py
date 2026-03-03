"""
Shared exception classes for getsentry modules.
"""


class SubscriptionIntegrityError(Exception):
    """Raised when subscription update fails due to concurrent modification."""
    pass
