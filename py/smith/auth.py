from smith._lowlevel import lib, ffi
from smith._compat import text_type, implements_to_string
from smith.utils import RustObject, encode_str, decode_str, rustcall, make_buf


__all__ = ['PublicKey', 'SecretKey', 'generate_key_pair']


@implements_to_string
class PublicKey(RustObject):
    __dealloc_func__ = lib.smith_publickey_free

    @classmethod
    def parse(cls, string):
        s = encode_str(string)
        ptr = rustcall(lib.smith_publickey_parse, s)
        return cls._from_objptr(ptr)

    def verify(self, buf, sig):
        buf = make_buf(buf)
        sig = encode_str(sig)
        return self._methodcall(lib.smith_publickey_verify, buf, sig)

    def __str__(self):
        return decode_str(self._methodcall(lib.smith_publickey_to_string), free=True)

    def __repr__(self):
        return '<%s %r>' % (
            self.__class__.__name__,
            text_type(self),
        )


class SecretKey(RustObject):
    __dealloc_func__ = lib.smith_secretkey_free

    @classmethod
    def parse(cls, string):
        s = encode_str(string)
        ptr = rustcall(lib.smith_secretkey_parse, s)
        return cls._from_objptr(ptr)

    def sign(self, value):
        buf = make_buf(value)
        return decode_str(self._methodcall(lib.smith_secretkey_sign, buf), free=True)

    def __str__(self):
        return decode_str(self._methodcall(lib.smith_secretkey_to_string), free=True)

    def __repr__(self):
        return '<%s %r>' % (
            self.__class__.__name__,
            text_type(self),
        )


def generate_key_pair():
    rv = rustcall(lib.smith_generate_key_pair)
    return (
        SecretKey._from_objptr(rv.secret_key),
        PublicKey._from_objptr(rv.public_key),
    )
