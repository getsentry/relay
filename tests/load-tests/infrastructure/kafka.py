import json
from enum import IntEnum, Enum
from datetime import datetime

from infrastructure.config import kafka_config
from confluent_kafka import Producer

from infrastructure.util import get_uuid


class Outcome(IntEnum):
    ACCEPTED = 0
    FILTERED = 1
    RATE_LIMITED = 2
    INVALID = 3
    ABUSE = 4


class Topic(Enum):
    Events = {"config_name": "events", "default": "ingest-events"}
    Attachments = {"config_name": "attachments", "default": "ingest-attachments"}
    Transactions = {"config_name": "transactions", "default": "ingest-transactions"}
    Outcomes = {"config_name": "outcomes", "default": "outcomes"}
    Sessions = {"config_name": "sessions", "default": "ingest-sessions"}


class KafkaProducerMixin:
    """
    A mixin to be used by Locusts that need to send kafka messages
    """

    def __init__(self):
        self.config = kafka_config()
        broker_config = self.config.get("broker", {})
        self.producer = Producer(broker_config)

    def topic_name(self, topic: Topic):
        topics = self.config.get("topics", {})
        val = topic.value
        return topics.get(val['config_name'], val['default'])

    def get_producer(self):
        return self.producer


def kafka_flush(task_set):
    kafka_mixin = _get_producer_mixin(task_set)
    producer = kafka_mixin.producer
    producer.flush()


def kafka_send_outcome(task_set, project_id, outcome: Outcome, event_id=None, org_id=None, reason=None,
                       key_id=None, remote_addr=None):
    message = {
        "project_id": project_id,
        "timestamp": datetime.utcnow().isoformat(),
        "outcome": outcome,
        "event_id": event_id
    }

    if event_id is not None:
        message["event_id"] = event_id

    if org_id is not None:
        message["org_id"] = org_id

    if reason is not None:
        message["reason"] = reason

    if key_id is not None:
        message["key_id"] = key_id

    if remote_addr is not None:
        message["remote_addr"] = remote_addr

    kafka_mixin = _get_producer_mixin(task_set)
    kafka_mixin.producer.produce(kafka_mixin.topic_name(Topic.Outcomes), json.dumps(message))


def kafka_send_event(task_set, event):
    kafka_mixin = _get_producer_mixin(task_set)
    kafka_mixin.producer.produce(kafka_mixin.topic_name(Topic.Events), json.dumps(event))


def _get_producer_mixin(task_set):
    """
    Tries to find a kafka Producer by waking up the chain of TaskSet up to Locust until it finds a KafkaProducerMixin
    If no KafkaProducerMixin is found it raises a ValueError
    """
    current = task_set
    while current is not None:
        if isinstance(current, KafkaProducerMixin):
            return current
        current = getattr(current, "parent", None)

    raise ValueError("Could not find KafkaProducerMixin in TaskSet tree",
                     "Derive your TaskSet or Locust class from KafkaProducerMixin")
