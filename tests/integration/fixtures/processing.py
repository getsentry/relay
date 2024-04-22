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
        self.timeout = timeout or 2

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
        rv = self.poll(timeout=0.2)
        assert rv is None, f"not empty: {rv.value()}"

        # Then, send a custom message to ensure we're not just timing out
        message = json.dumps({"__test__": uuid.uuid4().hex}).encode("utf8")
        self.test_producer.produce(self.topic_name, message)
        self.test_producer.flush(timeout=5)

        rv = self.poll(timeout=timeout)
        assert rv.error() is None
        assert rv.value() == message, rv.value()


@pytest.fixture
def outcomes_consumer(kafka_consumer):
    return lambda timeout=None, topic=None: OutcomesConsumer(
        timeout=timeout, *kafka_consumer(topic or "outcomes")
    )


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
    assert False, "invalid category"


class OutcomesConsumer(ConsumerBase):
    def _poll_all(self, timeout):
        while True:
            outcome = self.poll(timeout)
            if outcome is None:
                return
            else:
                yield outcome

    def get_outcomes(self, timeout=None):
        outcomes = list(self._poll_all(timeout))
        for outcome in outcomes:
            assert outcome.error() is None
        return [json.loads(outcome.value()) for outcome in outcomes]

    def get_outcome(self, timeout=None):
        outcomes = self.get_outcomes(timeout)
        assert len(outcomes) > 0, "No outcomes were consumed"
        assert len(outcomes) == 1, "More than one outcome was consumed"
        return outcomes[0]

    def assert_rate_limited(self, reason, key_id=None, categories=None, quantity=None):
        if categories is None:
            outcome = self.get_outcome()
            assert isinstance(outcome["category"], int)
            outcomes = [outcome]
        else:
            outcomes = self.get_outcomes()
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
def events_consumer(kafka_consumer):
    return lambda timeout=None: EventsConsumer(
        timeout=timeout, *kafka_consumer("events")
    )


@pytest.fixture
def transactions_consumer(kafka_consumer):
    return lambda timeout=None: EventsConsumer(
        timeout=timeout, *kafka_consumer("transactions")
    )


@pytest.fixture
def attachments_consumer(kafka_consumer):
    return lambda: AttachmentsConsumer(*kafka_consumer("attachments"))


@pytest.fixture
def sessions_consumer(kafka_consumer):
    return lambda: SessionsConsumer(*kafka_consumer("sessions"))


@pytest.fixture
def metrics_consumer(kafka_consumer):
    # The default timeout of 3 seconds compensates for delays and jitter
    return lambda timeout=3, topic=None: MetricsConsumer(
        timeout=timeout, *kafka_consumer(topic or "metrics")
    )


@pytest.fixture
def replay_recordings_consumer(kafka_consumer):
    return lambda: ReplayRecordingsConsumer(*kafka_consumer("replay_recordings"))


@pytest.fixture
def replay_events_consumer(kafka_consumer):
    return lambda timeout=None: ReplayEventsConsumer(
        timeout=timeout, *kafka_consumer("replay_events")
    )


@pytest.fixture
def feedback_consumer(kafka_consumer):
    return lambda timeout=None: FeedbackConsumer(
        timeout=timeout,
        *kafka_consumer(
            "feedback"
        ),  # Corresponds to key in processing_config["processing"]["topics"]
    )


@pytest.fixture
def monitors_consumer(kafka_consumer):
    return lambda timeout=None: MonitorsConsumer(
        timeout=timeout, *kafka_consumer("monitors")
    )


@pytest.fixture
def spans_consumer(kafka_consumer):
    return lambda timeout=None: SpansConsumer(timeout=timeout, *kafka_consumer("spans"))


@pytest.fixture
def profiles_consumer(kafka_consumer):
    return lambda: ProfileConsumer(*kafka_consumer("profiles"))


@pytest.fixture
def metrics_summaries_consumer(kafka_consumer):
    return lambda timeout=None: MetricsSummariesConsumer(
        timeout=timeout, *kafka_consumer("metrics_summaries")
    )


@pytest.fixture
def cogs_consumer(kafka_consumer):
    return lambda timeout=None: CogsConsumer(timeout=timeout, *kafka_consumer("cogs"))


class MetricsConsumer(ConsumerBase):
    def get_metric(self, timeout=None):
        message = self.poll(timeout=timeout)
        assert message is not None
        assert message.error() is None

        return json.loads(message.value()), message.headers()

    def get_metrics(self, timeout=None, max_attempts=100):
        for _ in range(max_attempts):
            message = self.poll(timeout=timeout)

            if message is None:
                return
            else:
                assert message.error() is None
                yield json.loads(message.value()), message.headers()


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

    def get_spans(self, timeout=None, max_attempts=100):
        for _ in range(max_attempts):
            message = self.poll(timeout=timeout)

            if message is None:
                return
            else:
                assert message.error() is None
                yield json.loads(message.value())


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

    def get_metrics_summaries(self, timeout=None, max_attempts=100):
        for _ in range(max_attempts):
            message = self.poll(timeout=timeout)

            if message is None:
                return
            else:
                assert message.error() is None
                yield json.loads(message.value())


class CogsConsumer(ConsumerBase):
    def get_measurement(self, timeout=None):
        message = self.poll(timeout=timeout)
        assert message is not None
        assert message.error() is None

        return json.loads(message.value())
