import functools
import os


def full_path_from_module_relative_path(module_name, *args):
    dir_path = os.path.dirname(os.path.realpath(module_name))
    return os.path.join(dir_path, *args)


def send_message(client, project_id, project_key, msg_body, headers=None):
    url = "/api/{}/store/".format(project_id)
    headers = {
        "X-Sentry-Auth": _auth_header(project_key),
        "Content-Type": "application/json; charset=UTF-8",
        **(headers or {})
    }
    return client.post(url, headers=headers, data=msg_body)


def send_envelope(client, project_id, project_key, envelope, headers=None):
    url = "/api/{}/envelope/".format(project_id)

    headers = {
        "X-Sentry-Auth": _auth_header(project_key),
        "Content-Type": "application/x-sentry-envelope",
        **(headers or {})
    }

    data = envelope.serialize()
    return client.post(url, headers=headers, data=data)


def _auth_header(project_key):
    return "Sentry sentry_key={},sentry_version=7".format(project_key)


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
