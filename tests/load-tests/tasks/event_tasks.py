"""
Contains tasks that generate various types of events
"""
from locust import TaskSet
from sentry_sdk.envelope import Envelope

from infrastructure import EventsCache, get_project_info, send_message, send_envelope


def canned_event_task(event_name: str):
    def inner(task_set: TaskSet):
        """
        Sends a canned event from the event cache, the event is retrieved
        from
        """
        locust_params = task_set.get_locust_params()
        num_projects = locust_params.get('num_projects', 1)

        msg_body = EventsCache.get_event_by_name(event_name)
        project_info = get_project_info(num_projects)
        return send_message(task_set.client, project_info.id, project_info.key, msg_body)

    return inner


def canned_envelope_event_task(event_name: str):
    def inner(task_set: TaskSet):
        locust_params = task_set.get_locust_params()
        num_projects = locust_params.get('num_projects', 1)

        body = EventsCache.get_event_by_name(event_name)
        project_info = get_project_info(num_projects)
        envelope = Envelope()
        envelope.add_event(body)
        return send_envelope(task_set.client, project_info.id, project_info.key, envelope)

    return inner
