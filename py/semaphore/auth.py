import json
import uuid
from semaphore._lowlevel import lib, ffi
from semaphore._compat import text_type, implements_to_string
from semaphore.utils import RustObject, encode_str, decode_str, decode_uuid, \
    rustcall, make_buf
from semaphore.exceptions import UnpackErrorBadSignature


__all__ = ['PublicKey', 'SecretKey', 'generate_key_pair',
           'create_register_challenge', 'get_register_response_relay_id',
           'validate_register_response']


@implements_to_string
class PublicKey(RustObject):
    __dealloc_func__ = lib.semaphore_publickey_free

    @classmethod
    def parse(cls, string):
        s = encode_str(string)
        ptr = rustcall(lib.semaphore_publickey_parse, s)
        return cls._from_objptr(ptr)

    def verify(self, buf, sig, max_age=None):
        buf = make_buf(buf)
        sig = encode_str(sig)
        if max_age is None:
            return self._methodcall(lib.semaphore_publickey_verify, buf, sig)
        return self._methodcall(
            lib.semaphore_publickey_verify_timestamp, buf, sig, max_age)

    def unpack(self, buf, sig, max_age=None):
        if not self.verify(buf, sig, max_age):
            raise UnpackErrorBadSignature('invalid signature')
        return json.loads(buf)

    def __str__(self):
        return decode_str(self._methodcall(lib.semaphore_publickey_to_string), free=True)

    def __repr__(self):
        return '<%s %r>' % (
            self.__class__.__name__,
            text_type(self),
        )


class SecretKey(RustObject):
    __dealloc_func__ = lib.semaphore_secretkey_free

    @classmethod
    def parse(cls, string):
        s = encode_str(string)
        ptr = rustcall(lib.semaphore_secretkey_parse, s)
        return cls._from_objptr(ptr)

    def sign(self, value):
        buf = make_buf(value)
        return decode_str(self._methodcall(lib.semaphore_secretkey_sign, buf), free=True)

    def pack(self, data):
        packed = json.dumps(data, separators=(',', ':'))
        return packed, self.sign(packed)

    def __str__(self):
        return decode_str(self._methodcall(lib.semaphore_secretkey_to_string), free=True)

    def __repr__(self):
        return '<%s %r>' % (
            self.__class__.__name__,
            text_type(self),
        )


def generate_key_pair():
    rv = rustcall(lib.semaphore_generate_key_pair)
    return (
        SecretKey._from_objptr(rv.secret_key),
        PublicKey._from_objptr(rv.public_key),
    )


def generate_relay_id():
    return decode_uuid(rustcall(lib.semaphore_generate_relay_id))


def create_register_challenge(data, signature, max_age=60 * 15):
    rv = json.loads(decode_str(rustcall(
        lib.semaphore_create_register_challenge, make_buf(data),
        encode_str(signature), max_age)))
    return {
        'relay_id': uuid.UUID(rv['relay_id']),
        'public_key': PublicKey.parse(rv['public_key']),
        'token': rv['token'],
    }


def get_register_response_relay_id(data):
    return decode_uuid(rustcall(
        lib.semaphore_get_register_response_relay_id, make_buf(data)))


def validate_register_response(public_key, data, signature, max_age=60 * 15):
    rv = json.loads(decode_str(rustcall(
        lib.semaphore_validate_register_response,
        public_key._objptr,
        make_buf(data),
        encode_str(signature),
        max_age)))
    return {
        'relay_id': uuid.UUID(rv['relay_id']),
        'token': rv['token'],
    }
