import pytest

from sentry_relay._lowlevel import lib
from sentry_relay.utils import rustcall
from sentry_relay.exceptions import Panic


def test_panic():
    with pytest.raises(Panic):
        rustcall(lib.relay_test_panic)
