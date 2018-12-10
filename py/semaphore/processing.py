import json

from semaphore._compat import string_types, iteritems, text_type
from semaphore._lowlevel import lib, ffi
from semaphore.utils import encode_str, decode_str, rustcall, RustObject, attached_refs


__all__ = ["split_chunks", "meta_with_chunks", "StoreNormalizer", "GeoIpLookup"]


def split_chunks(string, remarks):
    return json.loads(
        decode_str(
            rustcall(
                lib.semaphore_split_chunks,
                encode_str(string),
                encode_str(json.dumps(remarks)),
            )
        )
    )


def meta_with_chunks(data, meta):
    if not isinstance(meta, dict):
        return meta

    result = {}
    for key, item in iteritems(meta):
        if key == "" and isinstance(item, dict):
            result[""] = item.copy()
            if item.get("rem") and isinstance(data, string_types):
                result[""]["chunks"] = split_chunks(data, item["rem"])
        elif isinstance(data, dict):
            result[key] = meta_with_chunks(data.get(key), item)
        elif isinstance(data, list):
            result[key] = meta_with_chunks(data[int(key)], item)
        else:
            result[key] = item

    return result


class GeoIpLookup(RustObject):
    __dealloc_func__ = lib.semaphore_geoip_lookup_free
    __slots__ = ("_path",)

    @classmethod
    def from_path(cls, path):
        if isinstance(path, text_type):
            path = path.encode("utf-8")
        rv = cls._from_objptr(rustcall(lib.semaphore_geoip_lookup_new, path))
        rv._path = path
        return rv

    def __repr__(self):
        return "<GeoIpLookup %r>" % (self.path,)


class StoreNormalizer(RustObject):
    __dealloc_func__ = lib.semaphore_store_normalizer_free
    __init__ = object.__init__
    __slots__ = ("__weakref__",)

    def __new__(cls, geoip_lookup=None, **config):
        config = json.dumps(config)
        geoptr = geoip_lookup._get_objptr() if geoip_lookup is not None else ffi.NULL
        rv = cls._from_objptr(
            rustcall(lib.semaphore_store_normalizer_new, encode_str(config), geoptr)
        )
        if geoip_lookup is not None:
            attached_refs[rv] = geoip_lookup
        return rv

    def normalize_event(self, event):
        event = encode_str(json.dumps(event))
        rv = self._methodcall(lib.semaphore_store_normalizer_normalize_event, event)
        return json.loads(decode_str(rv))
