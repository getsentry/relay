import datetime
import time

import requests
from sentry_sdk.envelope import Envelope, Item, PayloadRef

session = requests.session()


class SentryLike(object):
    _healthcheck_passed = False

    default_dsn_public_key = "31a5a894b4524f74a9a8d0e27e21ba91"

    def __init__(self, server_address, upstream=None, public_key=None):
        self.server_address = server_address
        self.upstream = upstream
        self.public_key = public_key

    def get_dsn_public_key_configs(self, project_id):
        """
        Returns the public keys configuration objects for a project.

        If the current SentryLike object does not have project_config object for the
        requested project_id but has an upstream configured fall back on the upstream

        If no upstream and no project config return None.

        """
        project_config = None
        if hasattr(self, "project_configs"):
            project_config = self.project_configs.get(project_id)

        if project_config is not None:
            return project_config["publicKeys"]

        if self.upstream is not None:
            return self.upstream.get_dsn_public_key_configs(project_id)

        return None

    def get_dsn_public_key(self, project_id, idx=0):
        """
        Returns a dsn key for a project.
        By default it returns the first configured dsn key, if idx is specified
        it tries to return the key at the specified index.
        If the index is beyond the number of  available dsn_keys for the project it raises

        If no project exists for the requested project_id it falls back on a default_dsn_public_key
        in order not to crash sloppily written tests (maybe we should crash and fix the tests).
        """
        public_keys = self.get_dsn_public_key_configs(project_id)

        if public_keys is None:
            return self.default_dsn_public_key

        if len(public_keys) <= idx:
            raise Exception(
                "Invalid public key index:{} requested for project_id:{}".format(
                    idx, project_id
                )
            )

        key_config = public_keys[idx]

        return key_config["publicKey"]

    @property
    def url(self):
        return "http://{}:{}".format(*self.server_address)

    def get_auth_header(self, project_id, dsn_key_idx=0, dsn_key=None):
        if dsn_key is None:
            dsn_key = self.get_dsn_public_key(project_id, dsn_key_idx)
        return (
            "Sentry sentry_version=5, sentry_timestamp=1535376240291, "
            "sentry_client=raven-node/2.6.3, "
            "sentry_key={}".format(dsn_key)
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

    def send_event(
        self,
        project_id,
        payload=None,
        headers=None,
        legacy=False,
        dsn_key_idx=0,
        dsn_key=None,
    ):
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
            "X-Sentry-Auth": self.get_auth_header(project_id, dsn_key_idx, dsn_key),
            **(headers or {}),
        }

        if legacy:
            url = "/api/store/"
        else:
            url = "/api/%s/store/" % project_id

        response = self.post(url, headers=headers, **kwargs)
        response.raise_for_status()
        return response.json()

    def send_options(self, project_id, headers=None, dsn_key_idx=0):
        headers = {
            "X-Sentry-Auth": self.get_auth_header(project_id, dsn_key_idx),
            **(headers or {}),
        }
        url = f"/api/{project_id}/store/"
        response = self.req_options(url, headers=headers)
        return response

    def send_envelope(self, project_id, envelope, headers=None, dsn_key_idx=0):
        url = "/api/%s/envelope/" % project_id
        headers = {
            "Content-Type": "application/x-sentry-envelope",
            "X-Sentry-Auth": self.get_auth_header(project_id, dsn_key_idx),
            **(headers or {}),
        }

        response = self.post(url, headers=headers, data=envelope.serialize())
        response.raise_for_status()

    def send_session(self, project_id, payload):
        envelope = Envelope()
        envelope.add_session(payload)
        self.send_envelope(project_id, envelope)

    def send_session_aggregates(self, project_id, payload):
        envelope = Envelope()
        envelope.add_item(Item(payload=PayloadRef(json=payload), type="sessions"))
        self.send_envelope(project_id, envelope)

    def send_security_report(
        self,
        project_id,
        content_type,
        payload,
        release,
        environment,
        origin=None,
        dsn_key_idx=0,
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
                project_id,
                self.get_dsn_public_key(project_id, dsn_key_idx),
                release,
                environment,
            ),
            headers=headers,
            json=payload,
        )
        response.raise_for_status()
        return response

    def send_minidump(self, project_id, params=None, files=None, dsn_key_idx=0):
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
            "/api/{}/minidump/?sentry_key={}".format(
                project_id, self.get_dsn_public_key(project_id, dsn_key_idx)
            ),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            },
            files=all_files,
        )

        response.raise_for_status()
        return response

    def send_unreal_request(self, project_id, file_content, dsn_key_idx=0):
        """
        Sends a request to the unreal endpoint
        :param project_id: the project id
        :param file_content: the unreal file content
        """
        response = self.post(
            "/api/{}/unreal/{}/".format(
                project_id, self.get_dsn_public_key(project_id, dsn_key_idx)
            ),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
            },
            data=file_content,
        )

        response.raise_for_status()
        return response

    def send_attachments(self, project_id, event_id, files, dsn_key_idx=0):
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
                    "sentry_key={}".format(
                        self.get_dsn_public_key(project_id, dsn_key_idx)
                    )
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
