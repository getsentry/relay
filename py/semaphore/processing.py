import json
import six

from semaphore._lowlevel import lib
from semaphore.utils import encode_str, decode_str, rustcall


__all__ = ['split_chunks', 'meta_with_chunks']

def split_chunks(string, remarks):
    return json.loads(decode_str(rustcall(
        lib.semaphore_split_chunks,
        encode_str(string),
        encode_str(json.dumps(remarks))
    )))

def meta_with_chunks(data, meta):
    if not isinstance(meta, dict):
        return meta

    result = {}
    for key, item in six.iteritems(meta):
        if key == '' and isinstance(item, dict):
            result[''] = item.copy()
            if item.get('rem') and isinstance(data, six.string_types):
                result['']['chunks'] = split_chunks(data, item['rem'])
        elif isinstance(data, dict):
            result[key] = meta_with_chunks(data.get(key), item)
        elif isinstance(data, list):
            result[key] = meta_with_chunks(data[int(key)], item)
        else:
            result[key] = item

    return result
