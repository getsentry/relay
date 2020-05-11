import functools
import os


def full_path_from_module_relative_path(module_name, *args):
    dir_path = os.path.dirname(os.path.realpath(module_name))
    return os.path.join(dir_path, *args)


def send_message(client, project_id, project_key, msg_body):
    url = "/api/{}/store/".format(project_id)
    headers = {
        "X-Sentry-Auth": "Sentry sentry_key={},sentry_version=7".format(project_key),
        "Content-Type": "application/json; charset=UTF-8",
    }
    return client.post(url, headers=headers, data=msg_body)


def memoize(f):
    memo = {}

    @functools.wraps(f)
    def wrapper(*args):
        key_pattern = "{}_" * len(args)
        key = key_pattern.format(*args)
        if key not in memo:
            memo[key] = f(*args)
        return memo[key]

    return wrapper
