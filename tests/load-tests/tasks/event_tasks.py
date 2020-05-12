"""
Contains tasks that generate various types of events
"""
from locust import TaskSet
from sentry_sdk.envelope import Envelope

from infrastructure import ConfigurableTaskSet, EventsCache, get_project_info, send_message, send_envelope


def canned_event_task(event_name: str):
    def inner(task_set: ConfigurableTaskSet):
        """
        Sends a canned event from the event cache, the event is retrieved
        from
        """
        custom = task_set.params.custom
        msg_body = EventsCache.get_event_by_name(event_name)
        project_info = get_project_info(custom.num_projects)
        return send_message(task_set.client, project_info.id, project_info.key, msg_body)

    return inner


def canned_envelope_event_task(event_name: str):
    def inner(task_set: TaskSet):
        custom = task_set.params.custom
        body = EventsCache.get_event_by_name(event_name)
        project_info = get_project_info(custom.num_projects)
        envelope = Envelope()
        envelope.add_event(body)
        return send_envelope(task_set.client, project_info.id, project_info.key, envelope)

    return inner
