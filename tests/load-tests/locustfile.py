
from locust import HttpLocust, TaskSet, between
from infrastructure import EventsCache, send_message, relay_address, get_project_info


def small_event_task(task_set: TaskSet):
    _process_event(task_set.client, "small_event", task_set.num_projects)


def medium_event_task(task_set: TaskSet):
    _process_event(task_set.client, "medium_event", task_set.num_projects)


def large_event_task(task_set: TaskSet):
    _process_event(task_set.client, "large_event", task_set.num_projects)


def bad_event_task(task_set: TaskSet):
    _process_event(task_set.client, "bad_event", task_set.num_projects)


class ProjectApp(TaskSet):
    num_projects = 50
    tasks = {
        small_event_task: 10,
        medium_event_task: 10,
        large_event_task: 10,
        # bad_event_task: 1,
    }


class StandardClient(HttpLocust):
    wait_time = between(0.1,0.2)
    task_set = ProjectApp
    host = relay_address()

    def setup(self):
        EventsCache.load_events()


def _process_event(client, msg_name: str, num_projects: int):
    msg_body = EventsCache.get_event_by_name(msg_name)
    project_info = get_project_info(num_projects)
    return send_message(client, project_info.id, project_info.key, msg_body)


