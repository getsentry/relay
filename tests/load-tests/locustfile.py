from infrastructure import full_path_from_module_relative_path, create_locust_class
from tasks.event_tasks import canned_event_task, canned_envelope_event_task

small_event_task = canned_event_task("small_event")
medium_event_task = canned_event_task("medium_event")
large_event_task = canned_event_task("large_event")
bad_event_task = canned_event_task("bad_event")
medium_event_envelope_task = canned_envelope_event_task("medium_event")

_config_path = full_path_from_module_relative_path(__file__, "config/simple_load_test.yml")
SimpleLoadTest = create_locust_class("SimpleLoadTest", _config_path)
