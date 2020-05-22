import random
import time

from infrastructure.generators.util import schema_generator

# some canned messages to populate the breadcrumbs
_breadcrumb_messages = [
    "sending message via: UDP(10.8.0.10:53)",
    "GET http://localhost/xx/xxxx/xxxxxxxxxxxxxx [200]",
    "Authenticating the user_name"
    "IOError: [Errno 2] No such file or directory: '/tmp/someFile/'"
]


def breadcrumb_generator(min=None, max=None, categories=None, levels=None, types=None, messages=None):
    min = min if min is not None else 0
    max = max if max is not None else 50
    categories = categories if categories is not None and len(categories) > 0 else ["auth", "web-request", "query"]
    levels = levels if levels is not None else ["fatal", "error", "warning", "info", "debug"]
    types = types if types is not None else ["default", "http", "error"]
    messages = messages if messages is not None else _breadcrumb_messages
    get_num_crumbs = lambda: random.randrange(min, max + 1)

    generator = schema_generator(
        category=categories,
        timestamp=lambda: time.time(),
        level=levels,
        type=types,
        message=lambda: random.choice(messages)
    )

    def inner():
        num_crumbs = get_num_crumbs()
        result = [None] * num_crumbs
        for idx in range(num_crumbs):
            result[idx] = generator()
        return result

    return inner
