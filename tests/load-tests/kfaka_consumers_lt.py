"""
Load test for kafka consumer
"""
from locust import between

from infrastructure import (
    ConfigurableLocust, FakeSet, relay_address, full_path_from_module_relative_path,
    ConfigurableTaskSet,
)
from infrastructure.kafka import KafkaProducerMixin


class KafkaEventsTaskSet(ConfigurableTaskSet):
    pass


class KafkaOutcomesTaskSet(ConfigurableTaskSet):
    pass



class KafkaConsumersLoadTest(ConfigurableLocust, KafkaProducerMixin):
    task_set = FakeSet
    wait_time = between(0.1, 0.2)
    host = relay_address()

    def __init__(self):
        config_file_name = full_path_from_module_relative_path(__file__, "config/KafkaConsumersLoadTest.yml")
        super().__init__(config_file_name)
