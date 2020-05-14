from locust import between
from infrastructure import (
    EventsCache, relay_address, ConfigurableLocust, FakeSet,
    full_path_from_module_relative_path, ConfigurableTaskSet,
)
from tasks.event_tasks import canned_event_task, canned_envelope_event_task

small_event_task = canned_event_task("small_event")
medium_event_task = canned_event_task("medium_event")
large_event_task = canned_event_task("large_event")
bad_event_task = canned_event_task("bad_event")
medium_event_envelope_task = canned_envelope_event_task("medium_event")


class SimpleTaskSet(ConfigurableTaskSet):
    def __init__(self, parent):
        super().__init__(parent)
        custom = self.params.custom

        frequencies = custom.request_frequency
        self.set_tasks_frequencies({
            small_event_task: frequencies.get("small", 1),
            medium_event_task: frequencies.get("medium", 1),
            large_event_task: frequencies.get("large", 1),
            bad_event_task: frequencies.get("bad", 0),
            medium_event_envelope_task: frequencies.get("medium_envelope", 0),
        })


class SimpleLoadTest(ConfigurableLocust):
    task_set = FakeSet
    wait_time = between(0.1, 0.2)
    host = relay_address()

    def __init__(self):
        config_file_name = full_path_from_module_relative_path(__file__, "config/simple_load_test.yml")
        super().__init__(config_file_name)

    def setup(self):
        EventsCache.load_events()
