import random

from infrastructure.contexts import schema_generator, version_generator

def user_interface_generator():
    return schema_generator(
        ip_address=version_generator(4),
        username=lambda: f"Foouser {random.random()}",
        id=lambda: f"userid {random.random()}",
    )
