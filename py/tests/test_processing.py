import semaphore
import pytest

REMARKS = [['myrule', 's', 7, 17]]
META = {'': {'rem': REMARKS}}
TEXT = 'Hello, [redacted]!'
CHUNKS = [
    {'type': 'text', 'text': 'Hello, '},
    {'type': 'redaction', 'text': '[redacted]', 'rule_id': 'myrule', 'remark': 's'},
    {'type': 'text', 'text': '!'},
]
META_WITH_CHUNKS = {
    '': {
        'rem': REMARKS,
        'chunks': CHUNKS,
    }
}

def test_split_chunks():
    chunks = semaphore.split_chunks(TEXT, REMARKS)
    assert chunks == CHUNKS


def test_meta_with_chunks():
    meta = semaphore.meta_with_chunks(TEXT, META)
    assert meta == META_WITH_CHUNKS


def test_meta_with_chunks_none():
    meta = semaphore.meta_with_chunks(TEXT, None)
    assert meta == None


def test_meta_with_chunks_empty():
    meta = semaphore.meta_with_chunks(TEXT, {})
    assert meta == {}


def test_meta_with_chunks_empty_remarks():
    meta = semaphore.meta_with_chunks(TEXT, {'rem': []})
    assert meta == {'rem': []}


def test_meta_with_chunks_dict():
    meta = semaphore.meta_with_chunks({'test': TEXT, 'other': 1}, {'test': META})
    assert meta == {'test': META_WITH_CHUNKS}


def test_meta_with_chunks_list():
    meta = semaphore.meta_with_chunks(['other', TEXT], {'1': META})
    assert meta == {'1': META_WITH_CHUNKS}


def test_meta_with_chunks_missing_value():
    meta = semaphore.meta_with_chunks(None, META)
    assert meta == META


def test_meta_with_chunks_missing_non_string():
    meta = semaphore.meta_with_chunks(True, META)
    assert meta == META
