import random

from infrastructure.generators.util import version_generator

def tags_generator(min=0, max=100, tag_values_per_tag=100):
    """
    Generate tags dictionary, at least `min` tags and at most `max`.

    Tag names are very predictable such that you can directly control tag *key*
    cardinality by increasing/decreasing the `max` parameter.

    Tag value cardinality can be controlled with `tag_values_per_tag`.
    """
    def inner():
        tags = {}

        for i in range(random.randrange(min, max)):
            tags[f"mytag{i}"] = f"Tag value {random.randrange(0, tag_values_per_tag)}"

        return tags

    return inner
