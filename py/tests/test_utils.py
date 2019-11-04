# coding: utf-8
import pytest

from semaphore._lowlevel import lib
from semaphore.utils import rustcall
from semaphore.exceptions import Panic


def test_panic():
    with pytest.raises(Panic):
        rustcall(lib.semaphore_test_panic)
