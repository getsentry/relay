import random

def schema_generator(**fields):
    """
    Generate a dictionary instance according to the schema outlined by the
    provided kwargs. Supports:

    * string
    * number
    * callable
    * range object
    * list of any of the above (random item will be selected)
    """
    def inner():
        rv = {}
        for k, sub_generator in fields.items():
            if isinstance(sub_generator, (list, tuple, range)):
                sub_generator = random.choice(sub_generator)

            if isinstance(sub_generator, dict):
                sub_generator = schema_generator(**sub_generator)

            if callable(sub_generator):
                sub_generator = sub_generator()

            if sub_generator is not None:
                rv[k] = sub_generator

        return rv

    return inner

def version_generator(num_segments=3, max_version_segment=10):
    def inner():
        return ".".join(str(random.randrange(max_version_segment)) for _ in range(num_segments))

    return inner


def string_databag_generator(max_length=10000):
    """
    Generate a really random string of potentially ludicrous length.
    """

    def inner():
        rv = []
        for _ in range(random.randrange(max_length)):
            rv.append(chr(random.randrange(0, 256)))

        return "".join(rv)

    return inner
