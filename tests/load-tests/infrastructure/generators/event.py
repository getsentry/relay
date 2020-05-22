import time
import uuid
import random

from infrastructure.generators.util import (
    schema_generator, version_generator, string_databag_generator,
    sentence_generator,
)
from infrastructure.generators.user import user_interface_generator
from infrastructure.generators.contexts import os_context_generator, device_context_generator, app_context_generator
from infrastructure.generators.breadcrumbs import breadcrumb_generator


def base_event_generator(
    with_event_id=True,
    with_level=True,
    randomized_fingerprints=False,
    max_message_length=10000,
    max_users=None,
    min_breadcrumbs=None,
    max_breadcrumbs=None,
    breadcrumb_categories=None,
    breadcrumb_levels=None,
    breadcrumb_types=None,
    breadcrumb_messages=None,
):
    return schema_generator(
        event_id=(lambda: uuid.uuid4().hex) if with_event_id else None,
        level=["error", "debug"] if with_level else None,
        fingerprint=(lambda: uuid.uuid4().hex) if randomized_fingerprints else None,
        transaction=[None, lambda: f"mytransaction{random.randrange(100)}"],
        logentry={"formatted":  sentence_generator()},
        logger=["foo.bar.baz", "bam.baz.bad", None],
        timestamp=time.time,
        environment=["production", "development", "staging"],
        user=user_interface_generator(max_users=max_users),
        contexts={
            "os": [None, os_context_generator()],
            "device": [None, device_context_generator()],
            "app": [None, app_context_generator()]
        },
        breadcrumbs=breadcrumb_generator(
            min=min_breadcrumbs,
            max=max_breadcrumbs,
            categories=breadcrumb_categories,
            levels=breadcrumb_levels,
            types=breadcrumb_types,
            messages=breadcrumb_messages,
        )
    )
