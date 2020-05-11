from locust import HttpLocust, TaskSet, between
from infrastructure import (
    EventsCache, send_message, relay_address, get_project_info, ConfigurableLocust, FakeSet,
    full_path_from_module_relative_path, ConfigurableTaskSet,
)


def small_event_task(task_set: TaskSet):
    _process_event(task_set.client, "small_event", task_set.num_projects)


def medium_event_task(task_set: TaskSet):
    _process_event(task_set.client, "medium_event", task_set.num_projects)


def large_event_task(task_set: TaskSet):
    _process_event(task_set.client, "large_event", task_set.num_projects)


def bad_event_task(task_set: TaskSet):
    _process_event(task_set.client, "bad_event", task_set.num_projects)


class SimpleTaskSet(ConfigurableTaskSet):
    def __init__(self, parent):
        super().__init__(parent)
        custom = self.params.custom
        self.num_projects = custom.num_projects
        self.tasks2 = {
            small_event_task: 1,
        }
        frequencies = custom.request_frequency
        # Note if one passes a dictionary as tasks Locust converts it into
        # an array where each task is repeated by its frequency number
        # that (e.g. {t_a: 2, t_B: 3} is converted to [t_a,t_a,t_B,t_B,t_B]
        # we do the same thing below
        self.tasks = ([small_event_task] * frequencies.get("small", 1) +
                      [medium_event_task] * frequencies.get("medium", 1) +
                      [large_event_task] * frequencies.get("large", 1) +
                      [bad_event_task] * frequencies.get("bad", 0))


class SimpleLoadTest(ConfigurableLocust):
    task_set = FakeSet
    wait_time = between(0.1, 0.2)
    host = relay_address()

    def __init__(self):
        config_file_name = full_path_from_module_relative_path(__file__, "config/SimpleLoadTest.yml")
        super().__init__(config_file_name)

    def setup(self):
        EventsCache.load_events()


def _process_event(client, msg_name: str, num_projects: int):
    msg_body = EventsCache.get_event_by_name(msg_name)
    project_info = get_project_info(num_projects)
    return send_message(client, project_info.id, project_info.key, msg_body)
