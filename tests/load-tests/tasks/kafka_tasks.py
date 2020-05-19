"""
Tasks and task helpers to be used to generate kafka events and outcomes
"""
import json
import time

from locust import TaskSet

from infrastructure import EventsCache
from infrastructure.configurable_locust import get_project_info
from infrastructure.kafka import Outcome, kafka_send_outcome, kafka_send_event
import random

from infrastructure.util import get_uuid


def kafka_outcome_task(outcome: Outcome):
    """
    A task generator that creates outcomes of a single (specified) type
    """

    def task(task_set):
        _kafka_send_outcome(task_set, outcome)

    return task


_id_to_outcome = {outcome.value: outcome for outcome in Outcome}


def kafka_random_outcome_task(task_set):
    """
    A task that creates random outcomes
    """
    outcome = _id_to_outcome[random.randint(0, 4)]
    _kafka_send_outcome(task_set, outcome)


def kafka_configurable_outcome_task_factory(task_params):
    """
    A task factory that can be configured from the locust yaml file.

    IMPORTANT NOTE: in order to function as intended the yaml task definition using this
    factory needs too have at least one parameter (that is not the optional weight parameter)

    Example:

    task_set:
        tasks:
            do_stuff_task:  # a simple task with no parameters
                weight: 1
            kafka_configurable_outcome_task:
                accepted: 1
                filtered: 1

    """
    outcome_names = {outcome.name.lower(): outcome for outcome in Outcome}
    frequencies = []
    total_freq = 0
    for name, val in task_params.items():
        if name in outcome_names and val != 0:
            total_freq += val
            frequencies.append([outcome_names[name], total_freq])
    if total_freq == 0:
        ValueError("kafka_configurable_outcome_task has no configured outcomes")

    def task(task_set):
        outcome_idx = random.randint(1, total_freq)
        for outcome, acc_freq in frequencies:
            if acc_freq >= outcome_idx:
                break
        else:
            raise ValueError("kafka_configurable_outcome_task bug, invalid math, we should never get here")
        _kafka_send_outcome(task_set, outcome)

    return task


def canned_kafka_event_task(event_name: str, send_outcome: bool):
    def inner(task_set: TaskSet):
        event_body = EventsCache.get_event_by_name(event_name)
        event = json.loads(event_body)
        _kafka_send_event(task_set, event, send_outcome)

    return inner


def _kafka_send_outcome(task_set: TaskSet, outcome: Outcome):
    project_info = get_project_info(task_set)
    event_id = get_uuid()
    kafka_send_outcome(task_set, project_info.id, outcome, event_id, reason=outcome.reason())


def _kafka_send_event(task_set, event, send_outcome=True):
    project_info = get_project_info(task_set)
    event_id = get_uuid()
    event['event_id'] = event_id

    # set required attributes for processing in a central place
    event["project"] = project_info.id
    event['platform']= 'python'
    event['timestamp'] = time.time()

    kafka_send_event(task_set, event, project_info.id)

    if send_outcome:
        kafka_send_outcome(task_set, project_info.id, Outcome.ACCEPTED, event_id)
