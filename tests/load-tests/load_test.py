import yaml
import os

from locust import Locust, TaskSet, task, between, constant
from importlib import import_module
from random import randrange


def get_config_file_name():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_path, "load_test.yml")


def load_task_set_class(task_set_name: str):
    class_offset = task_set_name.rfind('.')
    if class_offset == -1:
        task_set = locals().get(task_set_name)
    else:
        module = import_module(task_set_name[:class_offset])
        task_set = getattr(module, task_set_name[class_offset + 1:])
    return task_set


def load_locust_config():
    config = getattr(load_locust_config, 'config', None)
    if config is not None:
        return config
    with open(get_config_file_name(), 'r') as f:
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
        class_name = task_set.get("class_name")
        if class_name is None:
            raise ValueError("Unspecified TaskSet class name for user at offset {}".format(idx))
        task_set['class'] = load_task_set_class(class_name)
    config['users'] = [LocustParams(user) for user in users]
    config['total_weight'] = total_weight
    load_locust_config.config = config
    return config


def get_locust_params():
    config = load_locust_config()

    locust_lottery = randrange(0, config.get('total_weight', 1))
    current_weight = 0

    for user in config.get('users'):
        current_weight += user.weight
        if current_weight > locust_lottery:
            return user

    raise Exception("Bug: wrong math, we should never be here")


class TaskSet1(TaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @task
    def my_task(self):
        print("111111")

    def wait_time(self):
        return 4


def my_task1(task_set):
    print("22222-1")


def my_task2(task_set):
    print("22222-2")


class TaskSet2(TaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = [my_task1, my_task2]

    def wait_time(self):
        return 5.5


class LocustParams(object):
    def __init__(self, params):
        self.params = params

    @property
    def task_set(self):
        task_set_config = self.params.get("task_set")
        return task_set_config.get("class", FakeSet)

    @property
    def wait_time(self):
        return self.params.get("wait_time", constant(0))

    def get_task_set_params(self):
        return TaskSetParams(self.params.get("task_set"))

    def __getattr__(self, item):
        return self.params.get(item)


class TaskSetParams(object):
    def __init__(self, params):
        self.params = params

    def __getattr__(self, item):
        return self.params.get(item)


class FakeSet(TaskSet):
    """ A task set that is only used so that the ConfigurableLocust is recognized as a Locust class"""

    @task
    def fake_task(self):
        pass  # a task never used


class ConfigurableLocust(Locust):
    task_set = FakeSet  # this is set only so that

    def __init__(self):
        super().__init__()
        params = get_locust_params()
        self.params = params
        self.task_set = params.task_set
        self.__wait_time = params.wait_time
        self.weight = params.weight
        print( "starting locust with task_set:{}".format(self.task_set.__name__))

    def wait_time(self):
        return self.__wait_time(self)


class ConfigurableTaskSet(TaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        parent_params = getattr(self.parent, "params")
        if parent_params is not None:
            self.params = self.parent.params.get_task_set_params()
        else:
            raise ValueError(__name__, "A TaskSet derived from ConfigurableTasSet "
                                       "must only be used by a ConfigurableLocust object")
