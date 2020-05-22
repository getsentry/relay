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
from infrastructure.generators.native import native_data_generator


def base_event_generator(
    with_event_id=True,
    with_level=True,
    num_event_groups=1,
    max_message_length=10000,
    max_users=None,
    min_breadcrumbs=None,
    max_breadcrumbs=None,
    breadcrumb_categories=None,
    breadcrumb_levels=None,
    breadcrumb_types=None,
    breadcrumb_messages=None,
    with_native_stacktrace=False,
    num_releases=10,
):
    event_generator = schema_generator(
        event_id=(lambda: uuid.uuid4().hex) if with_event_id else None,
        level=["error", "debug"] if with_level else None,
        fingerprint=lambda: [f"fingerprint{random.randrange(num_event_groups)}"],
        release=lambda: f"release{random.randrange(num_releases)}",
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

    if with_native_stacktrace:
        native_gen = native_data_generator()
        exc_gen = schema_generator(value=sentence_generator())

        def event_generator(base_gen=event_generator):
            event = base_gen()
            frames, images = native_gen()

            event['platform'] = 'cocoa'
            exc = exc_gen()
            event['exception'] = {'values': [exc]}

            exc['stacktrace'] = {'frames': frames}
            event['debug_meta'] = {'images': images}
            return event

    return event_generator
