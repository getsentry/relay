import io
import sys
import json
import shutil


PY2 = sys.version_info[0] == 2
if PY2:
    text_type = unicode  # NOQA
else:
    text_type = str


class Envelope(object):
    def __init__(self, headers=None, items=None):
        if headers is not None:
            headers = dict(headers)
        self.headers = headers or {}
        if items is None:
            items = []
        else:
            items = list(items)
        self.items = items

    def add_item(self, item):
        self.items.append(item)

    def get_event(self):
        for items in self.items:
            event = items.get_event()
            if event is not None:
                return event

    def __iter__(self):
        return iter(self.items)

    def serialize_into(self, f):
        f.write(json.dumps(self.headers).encode("utf-8"))
        f.write(b"\n")
        for item in self.items:
            item.serialize_into(f)

    def serialize(self):
        out = io.BytesIO()
        self.serialize_into(out)
        return out.getvalue()

    @classmethod
    def deserialize_from(cls, f):
        headers = json.loads(f.readline())
        items = []
        while 1:
            item = Item.deserialize_from(f)
            if item is None:
                break
            items.append(item)
        return cls(headers=headers, items=items)

    @classmethod
    def deserialize(cls, bytes):
        return cls.deserialize_from(io.BytesIO(bytes))

    def __repr__(self):
        return "<Envelope headers=%r items=%r>" % (self.headers, self.items,)


class PayloadRef(object):
    def __init__(self, bytes=None, path=None, event=None):
        self.bytes = bytes
        self.path = path
        self.event = event

    def get_bytes(self):
        if self.bytes is None:
            if self.path is not None:
                with open(self.path, "rb") as f:
                    self.bytes = f.read()
            elif self.event is not None:
                self.bytes = json.dumps(self.event).encode("utf-8")
            else:
                self.bytes = b""
        return self.bytes

    def prepare_serialize(self):
        if self.path is not None and self.bytes is None:
            f = open(self.path, "rb")
            f.seek(0, 2)
            length = f.tell()
            f.seek(0, 0)

            def writer(out):
                try:
                    shutil.copyfileobj(f, out)
                finally:
                    f.close()

            return length, writer

        bytes = self.get_bytes()
        return len(bytes), lambda f: f.write(bytes)

    @property
    def _type(self):
        if self.event is not None:
            return "event"
        elif self.bytes is not None:
            return "bytes"
        elif self.path is not None:
            return "path"
        return "empty"

    def __repr__(self):
        return "<Payload %r>" % (self._type,)


class Item(object):
    def __init__(self, payload, headers=None):
        if headers is not None:
            headers = dict(headers)
        elif headers is None:
            headers = {}
        self.headers = headers
        if isinstance(payload, bytes):
            payload = PayloadRef(bytes=payload)
        elif isinstance(payload, text_type):
            payload = PayloadRef(bytes=payload.encode("utf-8"))
        elif isinstance(payload, dict):
            payload = PayloadRef(event=payload)
        else:
            payload = payload

        if "content_type" not in headers:
            if payload.event is not None:
                headers["content_type"] = "application/json"
            else:
                headers["content_type"] = "application/octet-stream"
        if "type" not in headers:
            if payload.event is not None:
                headers["type"] = "event"
            else:
                headers["type"] = "attachment"
        self.payload = payload

    def __repr__(self):
        return "<Item headers=%r payload=%r>" % (self.headers, self.payload,)

    def get_bytes(self):
        return self.payload.get_bytes()

    def get_event(self):
        if self.payload.event is not None:
            return self.payload.event

    def serialize_into(self, f):
        headers = dict(self.headers)
        length, writer = self.payload.prepare_serialize()
        headers["length"] = length
        f.write(json.dumps(headers).encode("utf-8"))
        f.write(b"\n")
        writer(f)
        f.write(b"\n")

    def serialize(self):
        out = io.BytesIO()
        self.serialize_into(out)
        return out.getvalue()

    @classmethod
    def deserialize_from(cls, f):
        line = f.readline().rstrip()
        if not line:
            return
        headers = json.loads(line)
        length = headers["length"]
        payload = f.read(length)
        if headers.get("type") == "event":
            rv = cls(headers=headers, payload=PayloadRef(event=json.loads(payload)))
        else:
            rv = cls(headers=headers, payload=payload)
        f.readline()
        return rv

    @classmethod
    def deserialize(cls, bytes):
        return cls.deserialize_from(io.BytesIO(bytes))
