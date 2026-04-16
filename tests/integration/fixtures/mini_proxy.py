from queue import Queue
import threading
import pytest
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from requests.structures import CaseInsensitiveDict

from . import SentryLike

HOP_BY_HOP = {
    "host",
    "content-length",
    "connection",
    "transfer-encoding",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "upgrade",
}


@dataclass
class CapturedRequest:
    method: str
    path: str
    headers: CaseInsensitiveDict


class Proxy:
    def __init__(self, server: ThreadingHTTPServer) -> None:
        self.server = server
        self.server_address = server.server_address
        self.timeout = 5

        self.captured_requests: Queue = Queue()

    @property
    def url(self):
        return "http://{}:{}".format(*self.server_address)

    def get_captured_request(self, *, timeout=None):
        return self.captured_requests.get(timeout=timeout or self.timeout)


@pytest.fixture
def mini_proxy(request):
    def inner(upstream: str | SentryLike):
        proxy = None

        upstream = getattr(upstream, "url", upstream)

        class Handler(BaseHTTPRequestHandler):
            def _forward(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""

                req = CapturedRequest(
                    method=self.command,
                    path=self.path,
                    headers=CaseInsensitiveDict(dict(self.headers)),
                )
                proxy.captured_requests.put(req)

                fwd_headers = {
                    k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP
                }
                req = Request(
                    upstream.rstrip("/") + self.path,
                    data=body or None,
                    headers=fwd_headers,
                    method=self.command,
                )
                try:
                    resp = urlopen(req)
                    status, resp_headers, resp_body = (
                        resp.status,
                        resp.getheaders(),
                        resp.read(),
                    )
                except HTTPError as e:
                    status, resp_headers, resp_body = (
                        e.code,
                        e.headers.items(),
                        e.read(),
                    )

                self.send_response(status)
                for k, v in resp_headers:
                    if k.lower() not in HOP_BY_HOP:
                        self.send_header(k, v)
                self.end_headers()
                self.wfile.write(resp_body)

            do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = _forward

            def log_message(self, *args, **kwargs):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        request.addfinalizer(server.shutdown)

        proxy = Proxy(server)
        return proxy

    return inner
