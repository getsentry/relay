from locust import task

# do not remove it needs to be here in order for the file to be recognized as a locust file
from infrastructure import ConfigurableLocust, FakeSet, full_path_from_module_relative_path  # noqa
from infrastructure import ConfigurableTaskSet


class TaskSet1(ConfigurableTaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @task
    def my_task(self):
        pass

    def wait_time(self):
        return 4


def my_task1(task_set):
    # task_set_params = task_set.get_params()
    # custom = task_set_params.get_custom_params()
    # print("custom vals for 22222-1 is : {}".format(custom.__custom))
    print("22222-1")


def my_task2(task_set):
    # task_set_params = task_set.get_params()
    # custom = task_set_params.get_custom_params()
    # print("custom vals for 22222-2 is : {}".format(custom))
    print("22222-2")


class TaskSet2(ConfigurableTaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tasks = [my_task1, my_task2]

    def wait_time(self):
        return 5.5

    @task
    def my_task(self):
        pass


class GenericLocust(ConfigurableLocust):
    """
    In order for locust to recognize a Locust class we need a valid task_set class property
    """
    task_set = FakeSet  # this is set only so that

    def __init__(self):
        config_file_name = full_path_from_module_relative_path(__file__, "config/load_test.yml")
        super().__init__(config_file_name)
