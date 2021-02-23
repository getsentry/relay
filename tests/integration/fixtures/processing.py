import json
import msgpack
import uuid

import pytest
import os
import confluent_kafka as kafka
from copy import deepcopy
import json


@pytest.fixture
def get_topic_name():
    """
    Generate a unique topic name for each test
    """
    random = uuid.uuid4().hex
    return lambda topic: f"relay-test-{topic}-{random}"


@pytest.fixture
def processing_config(get_topic_name):
    """
    Returns a minimal configuration for setting up a relay capable of processing
    :param options: initial options to be merged
    :return: the altered options
    """

    def inner(options=None):
        # The CI script sets the kafka bootstrap server into system environment variable.
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "127.0.0.1:9092")

        options = deepcopy(options)  # avoid lateral effects

        if options is None:
            options = {}
        if options.get("processing") is None:
            options["processing"] = {}
        processing = options["processing"]
        processing["enabled"] = True
        if processing.get("kafka_config") is None:
            processing["kafka_config"] = [
                {"name": "bootstrap.servers", "value": bootstrap_servers},
                # {'name': 'batch.size', 'value': '0'}  # do not batch messages
            ]
        if processing.get("topics") is None:
            processing["topics"] = {
                "events": get_topic_name("events"),
                "attachments": get_topic_name("attachments"),
                "transactions": get_topic_name("transactions"),
                "outcomes": get_topic_name("outcomes"),
                "sessions": get_topic_name("sessions"),
            }

        if not processing.get("redis"):
            processing["redis"] = "redis://127.0.0.1"

        processing[
            "projectconfig_cache_prefix"
        ] = f"relay-test-relayconfig-{uuid.uuid4()}"

        return options

    return inner


@pytest.fixture
def relay_with_processing(relay, mini_sentry, processing_config):
    """
    Creates a fixture that configures a relay with processing enabled and that forwards
    requests to the test ingestion topics
    """

    def inner(options=None):
        options = processing_config(options)

        kafka_config = {}
        for elm in options["processing"]["kafka_config"]:
            kafka_config[elm["name"]] = elm["value"]

        return relay(mini_sentry, options=options)

    return inner


def kafka_producer(options):
    # look for the servers (it is the only config we are interested in)
    servers = [
        elm["value"]
        for elm in options["processing"]["kafka_config"]
        if elm["name"] == "bootstrap.servers"
    ]

    if not servers:
        raise ValueError(
            "Bad kafka_config, could not find 'bootstrap.servers'.\n"
            "The configuration should have an entry of the format \n"
            "{name:'bootstrap.servers', value:'127.0.0.1'} at path 'processing.kafka_config'"
        )

    return kafka.Producer({"bootstrap.servers": servers[0]})


@pytest.fixture
def kafka_consumer(request, get_topic_name, processing_config):
    """
    Creates a fixture that, when called, returns an already subscribed kafka consumer.
    """

    def inner(topic: str, options=None):
        topic_name = get_topic_name(topic)
        topics = [topic_name]
        options = processing_config(options)
        # look for the servers (it is the only config we are interested in)
        servers = [
            elm["value"]
            for elm in options["processing"]["kafka_config"]
            if elm["name"] == "bootstrap.servers"
        ]
        if len(servers) < 1:
            raise ValueError(
                "Bad kafka_config, could not find 'bootstrap.servers'.\n"
                "The configuration should have an entry of the format \n"
                "{name:'bootstrap.servers', value:'127.0.0.1'} at path 'processing.kafka_config'"
            )

        servers = servers[0]

        settings = {
            "bootstrap.servers": servers,
            "group.id": "test-consumer-%s" % uuid.uuid4().hex,
            "enable.auto.commit": True,
            "auto.offset.reset": "earliest",
        }

        consumer = kafka.Consumer(settings)
        consumer.assign([kafka.TopicPartition(t, 0) for t in topics])

        def die():
            consumer.close()

        request.addfinalizer(die)
        return consumer, options, topic_name

    return inner


class ConsumerBase(object):
    def __init__(self, consumer, options, topic_name, timeout=None):
        self.consumer = consumer
        self.test_producer = kafka_producer(options)
        self.topic_name = topic_name
        self.timeout = timeout or 1

        # Connect to the topic and poll a first test message.
        # First poll takes forever, the next ones are fast.
        self.assert_empty(timeout=5)

    def poll(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        return self.consumer.poll(timeout=timeout)

    def assert_empty(self, timeout=None):
        """
        An associated producer, that can send message on the same topic as the
        consumer used for tests when we don't expect anything to come back we
        can send a test message at the end and verify that it is the first and
        only message on the queue (care must be taken to make sure that the
        test message ends up in the same partition as the message we are checking).
        """
        # First, give Relay a bit of time to process
        assert self.poll(timeout=0.2) is None

        # Then, send a custom message to ensure we're not just timing out
        message = json.dumps({"__test__": uuid.uuid4().hex}).encode("utf8")
        self.test_producer.produce(self.topic_name, message)
        self.test_producer.flush()

        rv = self.poll(timeout=timeout)
        assert rv.error() is None
        assert rv.value() == message, rv.value()


@pytest.fixture
def outcomes_consumer(kafka_consumer):
    return lambda timeout=None: OutcomesConsumer(
        timeout=timeout, *kafka_consumer("outcomes")
    )


class OutcomesConsumer(ConsumerBase):
    def get_outcome(self):
        outcome = self.poll()
        assert outcome is not None
        assert outcome.error() is None
        return json.loads(outcome.value())

    def assert_rate_limited(self, reason, key_id=None, category=None):
        outcome = self.get_outcome()
        assert outcome["outcome"] == 2, outcome
        assert outcome["reason"] == reason
        if key_id is not None:
            assert outcome["key_id"] == key_id
        if category is not None:
            assert outcome["category"] == category, outcome["category"]
        else:
            assert outcome["category"] is not None

    def assert_dropped_internal(self):
        outcome = self.get_outcome()
        assert outcome["outcome"] == 3
        assert outcome["reason"] == "internal"

    def assert_dropped_unknown_project(self):
        outcome = self.get_outcome()
        assert outcome["outcome"] == 3
        assert outcome["reason"] == "project_id"


@pytest.fixture
def events_consumer(kafka_consumer):
    return lambda timeout=None: EventsConsumer(
        timeout=timeout, *kafka_consumer("events")
    )


@pytest.fixture
def transactions_consumer(kafka_consumer):
    return lambda: EventsConsumer(*kafka_consumer("transactions"))


@pytest.fixture
def attachments_consumer(kafka_consumer):
    return lambda: AttachmentsConsumer(*kafka_consumer("attachments"))


@pytest.fixture
def sessions_consumer(kafka_consumer):
    return lambda: SessionsConsumer(*kafka_consumer("sessions"))


class SessionsConsumer(ConsumerBase):
    def get_session(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        return json.loads(message.value())


class EventsConsumer(ConsumerBase):
    def get_event(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        event = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert event["type"] == "event"
        return json.loads(event["payload"].decode("utf8")), event

    def get_message(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        return message, msgpack.unpackb(message.value(), raw=False, use_list=False)


class AttachmentsConsumer(EventsConsumer):
    def get_attachment_chunk(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        v = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert v["type"] == "attachment_chunk", v["type"]
        return v["payload"], v

    def get_individual_attachment(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        v = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert v["type"] == "attachment", v["type"]
        return v
