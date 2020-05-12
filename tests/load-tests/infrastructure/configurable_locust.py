import os
from threading import RLock
from importlib import import_module
from random import randrange
from yaml import load

from .util import memoize, full_path_from_module_relative_path

try:
    from yaml import CLoader as Loader, CDumper as Dumper, CFullLoader as FullLoader
except ImportError:
    from yaml import Loader, Dumper, FullLoader

from locust import TaskSet, task, constant, HttpLocust, Locust


class FakeSet(TaskSet):
    """
    A task set that is only used so that the ConfigurableLocust is recognized as a Locust class
    """

    @task
    def fake_task(self):
        pass  # a task never used

def _default_custom():
    """
    Default custom parameters
    """
    return {
        "num_projects": 1
    }

def _default_task_set_params():
    """
    Default taks_set parameters
    """
    return {}

class ConfigParams(object):
    """
    Class used by the ConfigurableLocust to store its configuration.
    """
    def __init__(self, params, default_params=None):
        default_params = default_params or {}
        self.params = {**default_params, **params}

    def __getattr__(self, item):
        return self.params.get(item)

    def get_child_task_set_name(self):
        task_set_config = self.params.get("task_set",{})
        return task_set_config.get("class_name", None)

    @property
    def wait_time(self):
        return self.params.get("wait_time", constant(0))

    def get_child_params(self):
        return ConfigParams(self.params.get("task_set"), _default_task_set_params())

    @property
    def custom(self):
        return ConfigParams(self.params.get("custom", _default_custom()))


class ConfigurableTaskSet(TaskSet):
    """
    Root class for a TaskSet that can be configured via child parameters carried by the parent.

    Note: Since this class looks for configuration parameters through its parent it can only be
    used as a child of a ConfigurableLocust or a child of another ConfigurableTaskSet
    """
    def __init__(self, parent):
        super().__init__(parent)
        if hasattr(self.parent, "get_params"):
            self.__params = self.parent.get_params().get_child_params()
        else:
            raise ValueError(__name__, "A TaskSet derived from ConfigurableTaskSet "
                                       "must only be used by a ConfigurableLocust object or another ConfigurableTaskSet")
        params = self.__params
        task_set_class_name = params.get_child_task_set_name()
        if task_set_class_name is not None:
            self.task_set = _load_class(task_set_class_name)

    @property
    def params(self):
        return self.__params

class ConfigurableLocust(HttpLocust):
    """
    Root class for a configurable Locust.

    """
    host = "OVERRIDE_ME"

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
def _load_locust_config(file_name):
    config = getattr(_load_locust_config, 'config', None)
    if config is not None:
        return config
    with open(file_name, 'r') as f:
        config = load(f, Loader=FullLoader)

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


class EventsCache(object):
    events_guard = RLock()
    event_list = None
    event_dict = None
    events_loaded = False

    @classmethod
    def are_events_loaded(cls):
        return cls.events_loaded

    @classmethod
    def _check_events_loaded(cls):
        if not cls.are_events_loaded():
            raise ValueError("Accessing events before loading")

    @classmethod
    def load_events(cls):
        if cls.are_events_loaded():
            return
        with cls.events_guard:
            if cls.are_events_loaded():
                return
            event_dict = {}
            event_list = []
            event_dir = cls._get_event_directory()
            events_file = os.path.join(event_dir, "event_index.yml")
            try:
                with open(events_file, "r") as file:
                    file_names = load(file, Loader=FullLoader)

            except Exception as err:
                raise ValueError(
                    "Invalid event index file, event_index.yml", events_file
                )

            for file_name in file_names:
                file_path = os.path.join(event_dir, file_name + ".json")
                with open(file_path, "r") as file:
                    content = file.read()
                    event_list.append(content)
                    event_dict[file_name] = content
            cls.event_list = event_list
            cls.event_dict = event_dict
            cls.events_loaded = True

    @classmethod
    def get_num_events(cls):
        EventsCache._check_events_loaded()
        return len(cls.event_list)

    @classmethod
    def get_event_by_index(cls, event_idx: int):
        EventsCache._check_events_loaded()
        if len(cls.event_list) > event_idx:
            return cls.event_list[event_idx]
        raise ValueError("Invalid event index")

    @classmethod
    def get_event_by_name(cls, event_name: str):
        EventsCache._check_events_loaded()
        event = cls.event_dict.get(event_name)
        if event is None:
            raise ValueError("Invalid event name", event_name)
        return event

    @classmethod
    def _get_event_directory(cls):
        return full_path_from_module_relative_path(__file__, "../events")




