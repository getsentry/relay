import json
import uuid
from sentry_relay._lowlevel import lib
from sentry_relay._compat import PY2, text_type, implements_to_string
from sentry_relay.utils import (
    RustObject,
    encode_str,
    decode_str,
    decode_uuid,
    rustcall,
    make_buf,
)
from sentry_relay.exceptions import UnpackErrorBadSignature


__all__ = [
    "PublicKey",
    "SecretKey",
    "generate_key_pair",
    "create_register_challenge",
    "validate_register_response",
    "is_version_supported",
]


@implements_to_string
class PublicKey(RustObject):
    __dealloc_func__ = lib.relay_publickey_free

    @classmethod
    def parse(cls, string):
        s = encode_str(string)
        ptr = rustcall(lib.relay_publickey_parse, s)
        return cls._from_objptr(ptr)

    def verify(self, buf, sig, max_age=None):
        buf = make_buf(buf)
        sig = encode_str(sig)
        if max_age is None:
            return self._methodcall(lib.relay_publickey_verify, buf, sig)
        return self._methodcall(lib.relay_publickey_verify_timestamp, buf, sig, max_age)

    def unpack(self, buf, sig, max_age=None):
        if not self.verify(buf, sig, max_age):
            raise UnpackErrorBadSignature("invalid signature")
        return json.loads(buf)

    def __str__(self):
        return decode_str(self._methodcall(lib.relay_publickey_to_string), free=True)

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, text_type(self))


class SecretKey(RustObject):
    __dealloc_func__ = lib.relay_secretkey_free

    @classmethod
    def parse(cls, string):
        s = encode_str(string)
        ptr = rustcall(lib.relay_secretkey_parse, s)
        return cls._from_objptr(ptr)

    def sign(self, value):
        buf = make_buf(value)
        return decode_str(self._methodcall(lib.relay_secretkey_sign, buf), free=True)

    def pack(self, data):
        packed = json.dumps(data, separators=(",", ":"))
        if not PY2:
            packed = packed.encode("utf8")
        return packed, self.sign(packed)

    def __str__(self):
        return decode_str(self._methodcall(lib.relay_secretkey_to_string), free=True)

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, text_type(self))


def generate_key_pair():
    rv = rustcall(lib.relay_generate_key_pair)
    return (
        SecretKey._from_objptr(rv.secret_key),
        PublicKey._from_objptr(rv.public_key),
    )


def generate_relay_id():
    return decode_uuid(rustcall(lib.relay_generate_relay_id))


def create_register_challenge(data, signature, secret, max_age=60):
    challenge_json = rustcall(
        lib.relay_create_register_challenge,
        make_buf(data),
        encode_str(signature),
        encode_str(secret),
        max_age,
    )

    challenge = json.loads(decode_str(challenge_json, free=True))
    return {
        "relay_id": uuid.UUID(challenge["relay_id"]),
        "token": challenge["token"],
    }


def validate_register_response(data, signature, secret, max_age=60):
    response_json = rustcall(
        lib.relay_validate_register_response,
        make_buf(data),
        encode_str(signature),
        encode_str(secret),
        max_age,
    )

    response = json.loads(decode_str(response_json, free=True))
    return {
        "relay_id": uuid.UUID(response["relay_id"]),
        "token": response["token"],
        "public_key": response["public_key"],
        "version": response["version"],
    }


def is_version_supported(version):
    """
    Checks if the provided Relay version is still compatible with this library. The version can be
    ``None``, in which case a legacy Relay is assumed.
    """
    return rustcall(lib.relay_version_supported, encode_str(version or ""))
