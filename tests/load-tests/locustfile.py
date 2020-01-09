import os

from locust import HttpLocust, TaskSet
from threading import RLock
from math import floor
from random import random


from yaml import load

try:
    from yaml import CLoader as Loader, CDumper as Dumper, CFullLoader as FullLoader
except ImportError:
    from yaml import Loader, Dumper, FullLoader


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
        return os.path.realpath(os.path.join(__file__, "../events"))


def small_event_task(task_set: TaskSet):
    msg_body = EventsCache.get_event_by_name("small_event")
    send_message(task_set, msg_body)


def medium_event_task(task_set: TaskSet):
    msg_body = EventsCache.get_event_by_name("medium_event")
    send_message(task_set, msg_body)


def large_event_task(task_set: TaskSet):
    msg_body = EventsCache.get_event_by_name("large_event")
    send_message(task_set, msg_body)


def bad_event_task(task_set: TaskSet):
    msg_body = EventsCache.get_event_by_name("bad_event")
    send_message(task_set, msg_body)


def send_message(task_set, msg_body):
    client = task_set.client
    num_projects = task_set.num_projects
    config = task_set.locust.config

    use_fake_projects = config["use_fake_projects"]

    if not use_fake_projects:
        projects = config["projects"]
        num_available_projects = len(projects)
        if num_projects > num_available_projects:
            num_projects = num_available_projects
    project_idx = 1
    if num_projects > 1:
        project_idx = floor(random() * num_projects)
    if use_fake_projects:
        project_id = project_idx
        project_key = config["fake_projects"]["key"]
    else:
        project_cfg = config["projects"][project_idx]
        project_id = project_cfg["id"]
        project_key = project_cfg["key"]

    url = "/api/{}/store/".format(project_id)
    headers = {
        "X-Sentry-Auth": "Sentry sentry_key={},sentry_version=7".format(project_key),
        "Content-Type": "application/json; charset=UTF-8",
    }
    return client.post(url, headers=headers, data=msg_body)


def _get_relay_address(config):
    relay_settings = config.get("relay", {})
    host = relay_settings.get("host")
    port = relay_settings.get("port")

    if host is None:
        raise "Missing relay.host settings from config file:{}".format(
            _get_config_file_path()
        )
    if port is None:
        raise "Missing relay.port settings from config file:{}".format(
            _get_config_file_path()
        )

    return "{}:{}".format(host, port)


def _get_config():
    """
    Returns the program settings located in the main directory (just above this file's directory)
    with the name config.yml
    """
    file_name = _get_config_file_path()
    try:
        with open(file_name, "r") as file:
            return load(file, Loader=FullLoader)
    except Exception as err:
        print(
            "Error while getting the configuration file:{}\n {}".format(file_name, err)
        )
        raise ValueError("Invalid configuration")


def _get_config_file_path():
    return os.path.realpath(os.path.join(__file__, "..", "locust_config.yml"))


class ProjectApp(TaskSet):
    num_projects = 1
    tasks = {
        small_event_task: 10,
        medium_event_task: 10,
        large_event_task: 10,
        # bad_event_task: 1,
    }


class StandardClient(HttpLocust):
    min_wait = 100
    max_wait = 200
    task_set = ProjectApp
    config = _get_config()
    host = _get_relay_address(config)

    def setup(self):
        EventsCache.load_events()
