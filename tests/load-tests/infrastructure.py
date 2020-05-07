import os
import yaml
from importlib import import_module
from random import randrange
import functools

from locust import TaskSet, task, constant, Locust


def memoize(f):
    memo = {}

    @functools.wraps(f)
    def wrapper(*args):
        key_pattern = "{}_" * len(args)
        key = key_pattern.format(*args)
        if key not in memo:
            memo[key] = f(*args)
        return memo[key]

    return wrapper


def full_path_from_relative_path(module_name, relative_file_name):
    dir_path = os.path.dirname(os.path.realpath(module_name))
    return os.path.join(dir_path, relative_file_name)


class FakeSet(TaskSet):
    """
    A task set that is only used so that the ConfigurableLocust is recognized as a Locust class
    """

    @task
    def fake_task(self):
        pass  # a task never used


class ConfigParams(object):
    """
    Class used by the ConfigurableLocust to store its configuration.
    """
    def __init__(self, params):
        self.params = params

    def __getattr__(self, item):
        return self.params.get(item)

    def get_child_task_set_name(self):
        task_set_config = self.params.get("task_set",{})
        return task_set_config.get("class_name", None)

    @property
    def wait_time(self):
        return self.params.get("wait_time", constant(0))

    def get_child_params(self):
        return ConfigParams(self.params.get("task_set"))



class ConfigurableTaskSet(TaskSet):
    """
    A TaskSet that
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if hasattr(self.parent, "get_params"):
            self.__params = self.parent.get_params().get_child_params()
        else:
            raise ValueError(__name__, "A TaskSet derived from ConfigurableTasSet "
                                       "must only be used by a ConfigurableLocust object")

    def get_child_params(self):
        return TaskSetParams(self.__params.get("task_set"))




class ConfigurableLocust(Locust):
    """
    Root class for a configurable Locust.

    """

    def __init__(self, file_name):
        super().__init__()
        params = get_locust_params(file_name)
        self.__params = params
        task_set_class_name = params.get_child_task_set_name()
        if task_set_class_name is not None:
            self.task_set = _load_class(task_set_class_name)
        else:
            raise ValueError("Invalid locust config, no class name")
        self.__wait_time = params.wait_time
        self.weight = params.weight

    def wait_time(self):
        return self.__wait_time(self)

    def get_params(self):
        return self.__params


@memoize
def _load_class(class_name: str):
    """
    Loads a class from its name.

    Note: Relative names will be resolved relative to this module (and it is not recommended practice).
    For reliable results specify the full class name i.e. `package.module.class_name`
    """
    class_offset = class_name.rfind('.')
    if class_offset == -1:
        task_set = locals().get(class_name)
    else:
        module = import_module(class_name[:class_offset])
        task_set = getattr(module, class_name[class_offset + 1:])
    return task_set


@memoize
def _load_locust_config(file_name):
    config = getattr(_load_locust_config, 'config', None)
    if config is not None:
        return config
    with open(file_name, 'r') as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    users = config.get('users')
    total_weight = 0

    for idx, user in enumerate(users):
        weight = user.get("weight", 1)
        user['weight'] = weight
        total_weight += weight
        task_set = user.get("task_set")
        if task_set is None:
            raise ValueError("Unspecified task set for user at offset {}".format(idx))
    config['users'] = [ConfigParams(user) for user in users]
    config['total_weight'] = total_weight
    _load_locust_config.config = config
    return config


def get_locust_params(file_name):
    config = _load_locust_config(file_name)

    locust_lottery = randrange(0, config.get('total_weight', 1))
    current_weight = 0

    for user in config.get('users'):
        current_weight += user.weight
        if current_weight > locust_lottery:
            return user

    raise Exception("Bug: wrong math, we should never be here")
