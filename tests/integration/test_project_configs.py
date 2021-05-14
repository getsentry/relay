"""
Tests the project_configs endpoint (/api/0/relays/project_conigs/)
"""

import uuid
import pytest
from collections import namedtuple

from sentry_relay import PublicKey, SecretKey, generate_key_pair

