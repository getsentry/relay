import random

from infrastructure.generators.util import schema_generator, version_generator

def user_interface_generator(max_users=None):
    if not max_users:
        return schema_generator(ip_address=version_generator(4, 255))

    return schema_generator(
        ip_address=version_generator(4, 255),
        username=f"Hobgoblin {random.random()}",
        id=lambda: str(random.randrange(max_users))
    )
