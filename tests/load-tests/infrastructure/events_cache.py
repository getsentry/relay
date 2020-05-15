import os
from threading import RLock
from yaml import load

try:
    from yaml import CLoader as Loader, CDumper as Dumper, CFullLoader as FullLoader
except ImportError:
    from yaml import Loader, Dumper, FullLoader

from infrastructure import full_path_from_module_relative_path


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
            cls.load_events()

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
