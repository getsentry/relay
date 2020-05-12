from locust import HttpLocust, TaskSet, between
from infrastructure import (
    EventsCache, send_message, relay_address, get_project_info, ConfigurableLocust, FakeSet,
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
        # Note if one passes a dictionary as tasks Locust converts it into
        # an array where each task is repeated by its frequency number
        # that (e.g. {t_a: 2, t_B: 3} is converted to [t_a,t_a,t_B,t_B,t_B]
        # we do the same thing below
        self.tasks = ([small_event_task] * frequencies.get("small", 1) +
                      [medium_event_task] * frequencies.get("medium", 1) +
                      [large_event_task] * frequencies.get("large", 1) +
                      [bad_event_task] * frequencies.get("bad", 0) +
                      [medium_event_envelope_task] * frequencies.get("medium_envelope", 0)
                      )


class SimpleLoadTest(ConfigurableLocust):
    task_set = FakeSet
    wait_time = between(0.1, 0.2)
    host = relay_address()

    def __init__(self):
        config_file_name = full_path_from_module_relative_path(__file__, "config/SimpleLoadTest.yml")
        super().__init__(config_file_name)

    def setup(self):
        EventsCache.load_events()
