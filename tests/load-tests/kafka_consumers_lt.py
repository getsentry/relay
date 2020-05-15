"""
Load test for kafka consumer
"""
from locust import Locust

from infrastructure import (
    full_path_from_module_relative_path, create_locust_class,
)
from infrastructure.kafka import KafkaProducerMixin


def task1(task_set):
    print("In task1")


def task2(task_set):
    print("In task2")


def task3(task_set):
    print("In task3")


def task4(task_set):
    print("In task4")


def task_factory1(task_set_params):
    def internal(task_set):
        print(f"In task_set_params with {task_set_params}")

    return internal


_config_path = full_path_from_module_relative_path(__file__, "config/kafka_consumers_load_test.yml")
First = create_locust_class("First", _config_path, base_classes=(Locust, KafkaProducerMixin))
Second = create_locust_class("Second", _config_path, base_classes=(Locust, KafkaProducerMixin))
