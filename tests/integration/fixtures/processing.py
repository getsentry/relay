import pytest
import os
import confluent_kafka as kafka
from copy import deepcopy
from typing import Optional

from confluent_kafka.admin import AdminClient

topic_names = {
    "events": "test-ingest-events",
    "attachments": "test-ingest-attachments",
    "transactions": "test-ingest-transactions",
    "outcomes": "test-event-outcomes",
}


def _get_topic_name(topic: str, test_name: Optional[str]):
    topic_name = topic_names.get(topic)
    if topic_name is None:
        raise ValueError("Invalid topic_name specified, check topic_names dict for accepted topic names.", topic)
    if test_name is None:
        return topic_name
    else:
        return "{}--{}".format(topic_name, test_name)


def _processing_config(test_name: Optional[str], options=None):
    """
    Returns a minimal configuration for setting up a relay capable of processing
    :param options: initial options to be merged
    :return: the altered options
    """
    # The Travis script sets the kafka bootstrap server into system environment variable.
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVER', '127.0.0.1:9092')

    options = deepcopy(options)  # avoid lateral effects

    if options is None:
        options = {}
    if options.get('processing') is None:
        options['processing'] = {}
    processing = options['processing']
    processing['enabled'] = True
    if processing.get('kafka_config') is None:
        processing['kafka_config'] = [
            {'name': 'bootstrap.servers', 'value': bootstrap_servers},
            # {'name': 'batch.size', 'value': '0'}  # do not batch messages
        ]
    if processing.get('topics') is None:
        processing['topics'] = {
            'events': _get_topic_name("events", test_name),
            'attachments': _get_topic_name("attachments", test_name),
            'transactions': _get_topic_name("transactions", test_name),
            'outcomes': _get_topic_name("outcomes", test_name),
        }

    if not processing.get('redis'):
        processing['redis'] = 'redis://127.0.0.1'
    return options


@pytest.fixture
def relay_with_processing(relay, mini_sentry, request):
    """
    Creates a fixture that configures a relay with processing enabled and that forwards
    requests to the test ingestion topics
    """

    def inner(options=None):
        test_name = request.node.name
        options = _processing_config(test_name, options)
        admin = _KafkaAdminWrapper(request, options)
        admin.delete_events_topic()

        return relay(mini_sentry, options=options)

    return inner


class _KafkaAdminWrapper:
    def __init__(self, request, options):
        self.test_name = request.node.name
        self.options = options

        kafka_config = {}
        for elm in options['processing']['kafka_config']:
            kafka_config[elm['name']] = elm['value']

        self.admin_client = AdminClient(kafka_config)

    def delete_events_topic(self):
        self._delete_topic("events")

    def delete_outcomes_topic(self):
        self._delete_topic("outcomes")

    def _delete_topic(self, topic):
        topic_name = _get_topic_name(topic, self.test_name)
        try:
            futures_dict = self.admin_client.delete_topics([topic_name])
            self._sync_wait_on_result(futures_dict)
        except Exception:  # noqa
            pass  # noqa nothing to do (probably there was no topic to start with)

    def _sync_wait_on_result(self, futures_dict):
        """
        Synchronously waits on all futures returned by the admin_client api.
        :param futures_dict: the api returns a dict of futures that can be awaited
        """
        # just wait on all futures returned by the async operations of the admin_client
        for f in futures_dict.values():
            f.result(5)  # wait up to 5 seconds for the admin operation to finish



@pytest.fixture
def kafka_consumer(request):
    """
    Creates a fixture that, when called, returns an already subscribed kafka consumer.
    """

    def inner(topic: str, options=None):
        test_name = request.node.name
        topics = [_get_topic_name(topic, test_name)]
        options = _processing_config(test_name, options)
        # look for the servers (it is the only config we are interested in)
        servers = [elm['value'] for elm in options['processing']['kafka_config'] if elm['name'] == 'bootstrap.servers']
        if len(servers) < 1:
            raise ValueError("Bad kafka_config, could not find 'bootstrap.servers'.\n"
                             "The configuration should have an entry of the format \n"
                             "{name:'bootstrap.servers', value:'127.0.0.1'} at path 'processing.kafka_config'")

        servers = servers[0]

        settings = {
            'bootstrap.servers': servers,
            'group.id': 'test.consumer',
            'enable.auto.commit': True,
            'auto.offset.reset': 'earliest',
        }

        consumer = kafka.Consumer(settings)
        consumer.subscribe(topics)

        return consumer

    return inner
