from collections import namedtuple
from math import floor
from random import random

from yaml import load

try:
    from yaml import CLoader as Loader, CDumper as Dumper, CFullLoader as FullLoader
except ImportError:
    from yaml import Loader, Dumper, FullLoader

from .util import full_path_from_module_relative_path, memoize


def relay_address():
    config = locust_config()
    relay_settings = config.get("relay", {})
    host = relay_settings.get("host")
    port = relay_settings.get("port")

    if host is None:
        raise "Missing relay.host settings from config file:{}".format(
            _config_file_path()
        )
    if port is None:
        raise "Missing relay.port settings from config file:{}".format(
            _config_file_path()
        )

    return "{}:{}".format(host, port)


def kafka_config():
    config = locust_config()
    return config.get("kafka", {})


@memoize
def locust_config():
    """
    Returns the program settings located in the main directory (just above this file's directory)
    with the name config.yml
    """
    file_name = _config_file_path()
    try:
        with open(file_name, "r") as file:
            return load(file, Loader=FullLoader)
    except Exception as err:
        print(
            "Error while getting the configuration file:{}\n {}".format(file_name, err)
        )
        raise ValueError("Invalid configuration")


ProjectInfo = namedtuple("ProjectInfo", "id, key")


def generate_project_info(num_projects) -> ProjectInfo:
    config = locust_config()

    use_fake_projects = config["use_fake_projects"]

    if not use_fake_projects:
        projects = config["projects"]
        num_available_projects = len(projects)
        if num_projects > num_available_projects:
            num_projects = num_available_projects

    project_idx = 0
    if num_projects > 1:
        project_idx = floor(random() * num_projects)

    if use_fake_projects:
        project_id = project_idx + 1
        project_key = config["fake_projects"]["key"]
    else:
        project_cfg = config["projects"][project_idx]
        project_id = project_cfg["id"]
        project_key = project_cfg["key"]

    return ProjectInfo(id=project_id, key=project_key)


def _config_file_path():
    return full_path_from_module_relative_path(
        __file__, "..", "config", "locust_config.yml"
    )
