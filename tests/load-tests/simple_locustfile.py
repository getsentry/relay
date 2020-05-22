from infrastructure import full_path_from_module_relative_path, create_locust_class
from tasks import event_tasks
from tasks.event_tasks import canned_event_task, canned_envelope_event_task

small_event_task = canned_event_task("small_event")
medium_event_task = canned_event_task("medium_event")
large_event_task = canned_event_task("large_event")
bad_event_task = canned_event_task("bad_event")
medium_event_envelope_task = canned_envelope_event_task("medium_event")

# do NOT just import the functions in the module (you will get a warning that the function is not used,
# you will remove it and then will get a runtime error)
random_event_task_factory = event_tasks.random_event_task_factory
random_event_task = event_tasks.random_event_task
random_envelope_event_task_factory = event_tasks.random_envelope_event_task_factory
random_envelope_event_task = event_tasks.random_envelope_event_task

_config_path = full_path_from_module_relative_path(__file__, "config/simple.test.yml")
SimpleLoadTest = create_locust_class("SimpleLoadTest", _config_path)
RandomEvents = create_locust_class("RandomEvents", _config_path)
