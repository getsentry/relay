"""
Contains tasks that generate various types of events
"""
from locust import TaskSet
from sentry_sdk.envelope import Envelope

from infrastructure import EventsCache, generate_project_info, send_message, send_envelope
from infrastructure.configurable_locust import get_project_info
from infrastructure.generators.event import base_event_generator


def canned_event_task(event_name: str):
    def inner(task_set: TaskSet):
        """
        Sends a canned event from the event cache, the event is retrieved
        from
        """
        project_info = get_project_info(task_set)

        msg_body = EventsCache.get_event_by_name(event_name)
        return send_message(task_set.client, project_info.id, project_info.key, msg_body)

    return inner


def canned_envelope_event_task(event_name: str):
    def inner(task_set: TaskSet):
        project_info = get_project_info(task_set)

        body = EventsCache.get_event_by_name(event_name)
        envelope = Envelope()
        envelope.add_event(body)
        return send_envelope(task_set.client, project_info.id, project_info.key, envelope)

    return inner


def _get_task_config_args(task_params):
    return {k: v for k, v in task_params.items() if k != 'weight'}


def random_event_task_factory(task_params=None):
    if task_params is not None:
        kwargs = _get_task_config_args(task_params)
    else:
        kwargs = {}

    event_generator = base_event_generator(**kwargs)

    def inner(task_set: TaskSet):
        event = event_generator()
        project_info = get_project_info(task_set)

        return send_message(task_set.client, project_info.id, project_info.key, event)

    return inner


random_event_task = random_event_task_factory()


def random_envelope_event_task_factory(task_params=None):
    if task_params is not None:
        kwargs = _get_task_config_args(task_params)
    else:
        kwargs = {}

    event_generator = base_event_generator(**kwargs)

    def inner(task_set: TaskSet):
        event = event_generator()
        project_info = get_project_info(task_set)
        envelope = Envelope()
        envelope.add_event(event)
        return send_envelope(task_set.client, project_info.id, project_info.key, envelope)

    return inner


random_envelope_event_task = random_envelope_event_task_factory()
