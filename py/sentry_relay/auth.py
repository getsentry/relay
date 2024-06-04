from ._relay_pyo3 import (
    _generate_key_pair,
    generate_relay_id,
    _create_register_challenge,
    _validate_register_response,
    is_version_supported,
    RustPublicKey,
    RustSecretKey,
    UnpackErrorBadSignature,
)

import json
import uuid

class SecretKey:
    def __init__(self, inner):
        self.inner = inner
    
    @classmethod
    def parse(cls, string):
        return SecretKey(RustSecretKey.parse(string))

    def sign(self, value):
        return self.inner.sign(value)

    def pack(self, data):
        # TODO(@anonrig): Look into separators requirement
        packed = json.dumps(data, separators=(",", ":")).encode()
        return packed, self.sign(packed)

    def __str__(self):
        return self.inner.__str__()

    def __repr__(self):
        return self.inner.__repr__()


class PublicKey:
    def __init__(self, inner):
        self.inner = inner
    
    @classmethod
    def parse(cls, string):
        return SecretKey(RustPublicKey.parse(string))

    def verify(self, buf, sig, max_age=None):
        return self.inner.verify(buf, sig, max_age)

    def unpack(
        self,
        buf,
        sig,
        max_age=None,
    ):
        if not self.verify(buf, sig, max_age):
            raise UnpackErrorBadSignature()
        return json.loads(buf)

    def __str__(self):
        return self.inner.__str__()

    def __repr__(self):
        return self.inner.__repr__()

    


def generate_key_pair():
    rsk, rpk = _generate_key_pair()
    return (SecretKey(rsk), PublicKey(rpk))

def create_register_challenge(
    data,
    signature,
    secret,
    max_age=60,
    ):
    
    challenge = _create_register_challenge(
        data,
        signature,
        secret,
        max_age,
    )

    return {
        "relay_id": uuid.UUID(challenge["relay_id"]),
        "token": challenge["token"],
    }

def validate_register_response(
    data,
    signature,
    secret,
    max_age=60,
    ):
    response = _validate_register_response(
        data,
        signature,
        secret,
        max_age,
    )

    return {
        "relay_id": uuid.UUID(response["relay_id"]),
        "token": response["token"],
        "public_key": response["public_key"],
        "version": response["version"],
    }
