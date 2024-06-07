import json
import msgpack
import uuid

import pytest
import os
import confluent_kafka as kafka
from copy import deepcopy


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
            metrics_topic = get_topic_name("metrics")
            outcomes_topic = get_topic_name("outcomes")
            processing["topics"] = {
                "events": get_topic_name("events"),
                "attachments": get_topic_name("attachments"),
                "transactions": get_topic_name("transactions"),
                "outcomes": outcomes_topic,
                "outcomes_billing": outcomes_topic,
                "metrics_sessions": metrics_topic,
                "metrics_generic": metrics_topic,
                "replay_events": get_topic_name("replay_events"),
                "replay_recordings": get_topic_name("replay_recordings"),
                "monitors": get_topic_name("monitors"),
                "spans": get_topic_name("spans"),
                "profiles": get_topic_name("profiles"),
                "metrics_summaries": get_topic_name("metrics_summaries"),
                "cogs": get_topic_name("cogs"),
                "feedback": get_topic_name("feedback"),
            }

        if not processing.get("redis"):
            processing["redis"] = "redis://127.0.0.1"

        processing["projectconfig_cache_prefix"] = (
            f"relay-test-relayconfig-{uuid.uuid4()}"
        )

        return options

    return inner


@pytest.fixture
def relay_with_processing(relay, mini_sentry, processing_config):
    """
    Creates a fixture that configures a relay with processing enabled and that forwards
    requests to the test ingestion topics
    """

    def inner(options=None, **kwargs):
        options = processing_config(options)
        return relay(mini_sentry, options=options, **kwargs)

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


class ConsumerBase:
    def __init__(self, consumer, options, topic_name, timeout=None):
        self.consumer = consumer
        self.test_producer = kafka_producer(options)
        self.topic_name = topic_name
        self.timeout = timeout or 5

        # Connect to the topic and poll a first test message.
        # First poll takes forever, the next ones are fast.
        self.assert_empty(timeout=5)

    def poll(self, timeout=None):
        if timeout is None:
            timeout = self.timeout
        return self.consumer.poll(timeout=timeout)

    def poll_many(self, timeout=None, n=None):
        if timeout is None:
            timeout = self.timeout

        if n == 0:
            self.assert_empty()
            return

        messages = 0

        # Wait for the first outcome to show up for the full timeout duration
        message = self.poll(timeout)
        while message is not None:
            yield message
            messages += 1

            if messages == n:
                self.assert_empty()
                break

            # Wait the full timeout duration if we're polling for an exact number of
            # of messages, otherwise use a shorter timeout to keep tests faster.
            # The rational being that once an item arrives on the topic the others
            # are quick to follow.
            message = self.poll(min(2, timeout) if n is None else timeout)

        if n is not None:
            assert (
                n == messages
            ), f"{self.__class__.__name__}: Expected {n} messages, only got {messages}"

    def assert_empty(self, timeout=None):
        """
        An associated producer, that can send message on the same topic as the
        consumer used for tests when we don't expect anything to come back we
        can send a test message at the end and verify that it is the first and
        only message on the queue (care must be taken to make sure that the
        test message ends up in the same partition as the message we are checking).
        """
        # First, give Relay a bit of time to process
        rv = self.poll(timeout=0.2)
        assert rv is None, f"{self.__class__.__name__} not empty: {rv.value()}"

        # Then, send a custom message to ensure we're not just timing out
        message = json.dumps({"__test__": uuid.uuid4().hex}).encode("utf8")
        self.test_producer.produce(self.topic_name, message)
        self.test_producer.flush(timeout=5)

        rv = self.poll(timeout=timeout)
        assert rv.error() is None
        assert rv.value() == message, rv.value()


def category_value(category):
    if category == "default":
        return 0
    if category == "error":
        return 1
    if category == "transaction":
        return 2
    if category == "security":
        return 3
    if category == "attachment":
        return 4
    if category == "session":
        return 5
    if category == "transaction_processed":
        return 8
    if category == "transaction_indexed":
        return 9
    if category == "user_report_v2":
        return 14
    if category == "metric_bucket":
        return 15
    assert False, "invalid category"


class OutcomesConsumer(ConsumerBase):
    def get_outcomes(self, timeout=None, n=None):
        outcomes = list(self.poll_many(timeout=timeout, n=n))
        for outcome in outcomes:
            assert outcome.error() is None
        return [json.loads(outcome.value()) for outcome in outcomes]

    def get_outcome(self, timeout=None):
        outcomes = self.get_outcomes(timeout)
        assert len(outcomes) > 0, "No outcomes were consumed"
        assert len(outcomes) == 1, "More than one outcome was consumed"
        return outcomes[0]

    def assert_rate_limited(
        self, reason, key_id=None, categories=None, quantity=None, timeout=1
    ):
        if categories is None:
            outcome = self.get_outcome(timeout=timeout)
            assert isinstance(outcome["category"], int)
            outcomes = [outcome]
        else:
            outcomes = self.get_outcomes(timeout=timeout)
            expected = {category_value(category) for category in categories}
            actual = {outcome["category"] for outcome in outcomes}
            assert actual == expected, (actual, expected)

        for outcome in outcomes:
            assert outcome["outcome"] == 2, outcome
            assert outcome["reason"] == reason, outcome["reason"]
            if key_id is not None:
                assert outcome["key_id"] == key_id

        if quantity is not None:
            count = sum(outcome["quantity"] for outcome in outcomes)
            assert count == quantity


@pytest.fixture
def consumer_fixture(kafka_consumer):
    def consumer_fixture(cls, default_topic):
        consumer = None

        def inner(timeout=None, topic=None):
            nonlocal consumer
            consumer = cls(timeout=timeout, *kafka_consumer(topic or default_topic))
            return consumer

        yield inner

        if consumer is not None:
            consumer.assert_empty()

    return consumer_fixture


@pytest.fixture
def outcomes_consumer(consumer_fixture):
    yield from consumer_fixture(OutcomesConsumer, "outcomes")


@pytest.fixture
def events_consumer(consumer_fixture):
    yield from consumer_fixture(EventsConsumer, "events")


@pytest.fixture
def transactions_consumer(consumer_fixture):
    yield from consumer_fixture(EventsConsumer, "transactions")


@pytest.fixture
def attachments_consumer(consumer_fixture):
    yield from consumer_fixture(AttachmentsConsumer, "attachments")


@pytest.fixture
def sessions_consumer(consumer_fixture):
    yield from consumer_fixture(SessionsConsumer, "sessions")


@pytest.fixture
def metrics_consumer(consumer_fixture):
    yield from consumer_fixture(MetricsConsumer, "metrics")


@pytest.fixture
def replay_recordings_consumer(consumer_fixture):
    yield from consumer_fixture(ReplayRecordingsConsumer, "replay_recordings")


@pytest.fixture
def replay_events_consumer(consumer_fixture):
    yield from consumer_fixture(ReplayEventsConsumer, "replay_events")


@pytest.fixture
def feedback_consumer(consumer_fixture):
    yield from consumer_fixture(FeedbackConsumer, "feedback")


@pytest.fixture
def monitors_consumer(consumer_fixture):
    yield from consumer_fixture(MonitorsConsumer, "monitors")


@pytest.fixture
def spans_consumer(consumer_fixture):
    yield from consumer_fixture(SpansConsumer, "spans")


@pytest.fixture
def profiles_consumer(consumer_fixture):
    yield from consumer_fixture(ProfileConsumer, "profiles")


@pytest.fixture
def metrics_summaries_consumer(consumer_fixture):
    yield from consumer_fixture(MetricsSummariesConsumer, "metrics_summaries")


@pytest.fixture
def cogs_consumer(consumer_fixture):
    yield from consumer_fixture(CogsConsumer, "cogs")


class MetricsConsumer(ConsumerBase):
    def get_metric(self, timeout=None):
        message = self.poll(timeout=timeout)
        assert message is not None
        assert message.error() is None

        return json.loads(message.value()), message.headers()

    def get_metrics(self, timeout=None, n=None):
        metrics = []

        for message in self.poll_many(timeout=timeout, n=n):
            assert message.error() is None
            metrics.append((json.loads(message.value()), message.headers()))

        return metrics


class SessionsConsumer(ConsumerBase):
    def get_session(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        return json.loads(message.value())


class EventsConsumer(ConsumerBase):
    def get_event(self, timeout=None):
        message = self.poll(timeout)
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

    def get_user_report(self, timeout=None):
        message = self.poll(timeout)
        assert message is not None
        assert message.error() is None

        v = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert v["type"] == "user_report", v["type"]
        return v


class ReplayRecordingsConsumer(EventsConsumer):
    def get_chunked_replay_chunk(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        v = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert v["type"] == "replay_recording_chunk", v["type"]
        return v["payload"], v

    def get_chunked_replay(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        v = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert v["type"] == "replay_recording", v["type"]
        return v

    def get_not_chunked_replay(self, timeout=None):
        message = self.poll(timeout=timeout)
        assert message is not None
        assert message.error() is None

        v = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert v["type"] == "replay_recording_not_chunked", v["type"]
        return v


class ReplayEventsConsumer(ConsumerBase):
    def get_replay_event(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        event = json.loads(message.value())
        payload = json.loads(bytes(event["payload"]))

        assert payload["type"] == "replay_event"
        return payload, event


class FeedbackConsumer(ConsumerBase):
    def get_event(self, timeout=None):
        message = self.poll(timeout)
        assert message is not None
        assert message.error() is None

        message_dict = msgpack.unpackb(message.value(), raw=False, use_list=False)
        return json.loads(message_dict["payload"].decode("utf8")), message_dict


class MonitorsConsumer(ConsumerBase):
    def get_check_in(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        wrapper = msgpack.unpackb(message.value(), raw=False, use_list=False)
        assert wrapper["type"] == "check_in"
        return json.loads(wrapper["payload"].decode("utf8")), wrapper


class SpansConsumer(ConsumerBase):
    def get_span(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        return json.loads(message.value())

    def get_spans(self, timeout=None, n=None):
        spans = []

        for message in self.poll_many(timeout=timeout, n=n):
            assert message.error() is None
            spans.append(json.loads(message.value()))

        return spans


class ProfileConsumer(ConsumerBase):
    def get_profile(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        return msgpack.loads(message.value()), message.headers()


class MetricsSummariesConsumer(ConsumerBase):
    def get_metrics_summary(self):
        message = self.poll()
        assert message is not None
        assert message.error() is None

        return json.loads(message.value())

    def get_metrics_summaries(self, timeout=None, n=None):
        metrics_summaries = []

        for message in self.poll_many(timeout=timeout, n=n):
            assert message.error() is None
            metrics_summaries.append(json.loads(message.value()))

        return metrics_summaries


class CogsConsumer(ConsumerBase):
    def get_measurement(self, timeout=None):
        message = self.poll(timeout=timeout)
        assert message is not None
        assert message.error() is None

        return json.loads(message.value())
