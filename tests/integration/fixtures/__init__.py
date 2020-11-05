import datetime
import time

import requests
from sentry_sdk.envelope import Envelope

session = requests.session()


class SentryLike(object):
    _healthcheck_passed = False

    dsn_public_key = "31a5a894b4524f74a9a8d0e27e21ba91"

    def __init__(self, server_address, upstream=None, public_key=None):
        self.server_address = server_address
        self.upstream = upstream
        self.public_key = public_key
        self.public_keys = {}

    @property
    def url(self):
        return "http://{}:{}".format(*self.server_address)

    @property
    def dsn(self):
        """DSN for which you will find the events in self.captured_events"""
        # bogus, we never check the DSN
        return "http://{}@{}:{}/42".format(self.dsn_public_key, *self.server_address)

    @property
    def auth_header(self):
        return (
            "Sentry sentry_version=5, sentry_timestamp=1535376240291, "
            "sentry_client=raven-node/2.6.3, "
            "sentry_key={}".format(self.dsn_public_key)
        )

    def _wait(self, path):
        backoff = 0.1
        while True:
            try:
                self.get(path).raise_for_status()
                break
            except Exception:
                time.sleep(backoff)
                if backoff > 10:
                    raise
                backoff *= 2

    def wait_relay_healthcheck(self):
        if self._healthcheck_passed:
            return

        self._wait("/api/relay/healthcheck/ready/")
        self._healthcheck_passed = True

    def __repr__(self):
        return "<{}({})>".format(self.__class__.__name__, repr(self.upstream))

    def iter_public_keys(self):
        if self.public_key is not None:
            yield self.public_key

        if self.upstream is not None:
            if isinstance(self.upstream, tuple):
                for upstream in self.upstream:
                    yield from upstream.iter_public_keys()
            else:
                yield from self.upstream.iter_public_keys()

    def send_event(self, project_id, payload=None, headers=None, legacy=False):
        if payload is None:
            payload = {"message": "Hello, World!"}

        if isinstance(payload, dict):
            kwargs = {"json": payload}
        elif isinstance(payload, bytes):
            kwargs = {"data": payload}
        else:
            raise ValueError(f"Invalid type {type(payload)} for payload.")

        headers = {
            "Content-Type": "application/octet-stream",
            "X-Sentry-Auth": self.auth_header,
            **(headers or {}),
        }

        if legacy:
            url = "/api/store/"
        else:
            url = "/api/%s/store/" % project_id

        response = self.post(url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()

    def send_options(self, project_id, headers=None):
        headers = {
            "X-Sentry-Auth": self.auth_header,
            **(headers or {}),
        }
        url = f"/api/{project_id}/store/"
        response = self.req_options(url, headers=headers)
        return response

    def send_envelope(self, project_id, envelope, headers=None):
        url = "/api/%s/envelope/" % project_id
        headers = {
            "Content-Type": "application/x-sentry-envelope",
            "X-Sentry-Auth": self.auth_header,
            **(headers or {}),
        }

        response = self.post(url, headers=headers, data=envelope.serialize())
        response.raise_for_status()

    def send_session(self, project_id, payload):
        envelope = Envelope()
        envelope.add_session(payload)
        self.send_envelope(project_id, envelope)

    def send_security_report(
        self, project_id, content_type, payload, release, environment, origin=None
    ):
        headers = {
            "Content-Type": content_type,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
        }

        if origin is not None:
            headers["Origin"] = origin

        response = self.post(
            "/api/{}/security/?sentry_key={}&sentry_release={}&sentry_environment={}".format(
                project_id, self.dsn_public_key, release, environment
            ),
            headers=headers,
            json=payload,
        )
        response.raise_for_status()

    def send_minidump(self, project_id, params=None, files=None):
        """
        :param project_id: the project id
        :param params: a list of tuples (param_name, param_value)
        :param files: a list of triples (param_name, file_name, file_content)
        """

        if files is not None:
            all_files = {
                name: (file_name, file_content)
                for (name, file_name, file_content) in files
            }
        else:
            all_files = {}

        if params is not None:
            for param in params:
                all_files[param[0]] = (None, param[1])

        response = self.post(
            "/api/{}/minidump/?sentry_key={}".format(project_id, self.dsn_public_key),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            },
            files=all_files,
        )

        response.raise_for_status()
        return response

    def send_unreal_request(self, project_id, file_content):
        """
        Sends a requuest to the unreal endpoint
        :param project_id: the project id
        :param file_content: the unreal file content
        """
        response = self.post(
            "/api/{}/unreal/{}/".format(project_id, self.dsn_public_key),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            },
            data=file_content,
        )

        response.raise_for_status()
        return response

    def send_attachments(self, project_id, event_id, files):
        files = {
            name: (file_name, file_content) for (name, file_name, file_content) in files
        }
        response = self.post(
            "/api/{}/events/{}/attachments/".format(project_id, event_id),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
                "X-Sentry-Auth": (
                    "Sentry sentry_version=5, sentry_timestamp=1535376240291, "
                    "sentry_client=raven-node/2.6.3, "
                    "sentry_key={}".format(self.dsn_public_key)
                ),
            },
            files=files,
        )
        response.raise_for_status()
        return response

    def request(self, method, path, **kwargs):
        assert path.startswith("/")
        return session.request(method, self.url + path, **kwargs)

    def post(self, path, **kwargs):
        return self.request("post", path, **kwargs)

    def req_options(self, path, **kwargs):
        return self.request("options", path, **kwargs)

    def get(self, path, **kwargs):
        return self.request("get", path, **kwargs)
