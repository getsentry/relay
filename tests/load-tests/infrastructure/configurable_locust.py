from collections import abc

from yaml import load

from .config import relay_address
from .util import memoize, load_object

try:
    from yaml import CLoader as Loader, CDumper as Dumper, CFullLoader as FullLoader
except ImportError:
    from yaml import Loader, Dumper, FullLoader

from locust import TaskSet, HttpLocust, constant, between, constant_pacing, Locust


def create_task_set(user_name, config):
    task_set_config = config.get("task_set", {})

    tasks_info = task_set_config.get("tasks")
    if isinstance(tasks_info, abc.Sequence):
        # we have a list of tasks with no params just load them
        _tasks = [load_object(task_name) for task_name in tasks_info]
    elif isinstance(tasks_info, abc.Mapping):
        # we have tasks with attributes
        _tasks = {}
        for task_func_name, task_info in tasks_info.items():
            if "weight" in task_info:
                weight = task_info["weight"]
                del task_info["weight"]
            else:
                weight = 1
            if weight == 0:
                continue  # task disabled

            if len(task_info) > 0:
                # we have other attributes besides frequency, the tasks are actually task factory functions
                task_factory = load_object(task_func_name)
                task = task_factory(task_info)
                _tasks[task] = weight
            else:
                task = load_object(task_func_name)
                _tasks[task] = weight
    else:
        raise ValueError("Could not find a tasks dictionary attribute for user_name", user_name)

    if len(_tasks) == 0:
        raise ("User with 0 tasks enabled", user_name)

    class ConfigurableTaskSet(TaskSet):
        tasks = _tasks
        params = tasks_info

        def get_params(self):
            return self.params

        def get_locust_params(self):
            parent = self.parent
            while parent is not None:
                if isinstance(parent, Locust):
                    return parent.get_params()
                parent = parent.parent

    return ConfigurableTaskSet


def _get_wait_time(locust_info):
    """
    Evaluates a wait expression, the result should be a Callable[[None], float]

    in the locust file we expect something like:
    wait_time: between(12, 23)

    the following functions are recognized  between, constant, constant_pacing
    (all imported from the `locust` module)

    """
    wait_expr = locust_info.get("wait_time")

    if wait_expr is None:
        return constant(0)

    env_locals = {
        # add recognized functions (no attempt to recognize anything beyond what is here)
        "between": between,
        "constant": constant,
        "constant_pacing": constant_pacing
    }
    return eval(wait_expr, globals(), env_locals)


def create_locust_class(name, config_file_name, host=None, base_classes=None):
    if base_classes is None:
        base_classes = (HttpLocust,)

    config = _load_locust_config(config_file_name)
    locust_info = config.get(name)

    if locust_info is None:
        return None

    _weight = locust_info.get("weight", 1)

    if _weight == 0:
        return None  # locust is disabled don't bother loading it

    _task_set = create_task_set(name, locust_info)
    _wait_time = _get_wait_time(locust_info)
    if host is None:
        _host = relay_address()
    else:
        _host = host

    class ConfigurableLocust(*base_classes):
        """
        Root class for a configurable Locust.
        """
        task_set = _task_set
        wait_time = _wait_time
        weight = _weight
        params = locust_info
        host = _host

        def get_params(self):
            return self.params

    ConfigurableLocust.__name__ = name

    return ConfigurableLocust


@memoize
def _load_locust_config(file_name):
    config = getattr(_load_locust_config, "config", None)
    if config is not None:
        return config
    with open(file_name, "r") as f:
        config = load(f, Loader=FullLoader)

    users = config.get("users")
    return users
