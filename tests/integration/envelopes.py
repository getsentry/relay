"""Transaction helpers for envelopes, which sentry-sdk 3.x no longer provides."""

import json

from sentry_sdk.envelope import Envelope, Item, PayloadRef


def add_transaction(envelope, transaction):
    envelope.add_item(Item(payload=PayloadRef(json=transaction), type="transaction"))


def get_transaction_event(envelope):
    for item in envelope.items:
        if item.type == "transaction" and item.payload.json is not None:
            return item.payload.json
    return None


def deserialize_envelope(data):
    """Deserialize an envelope and parse its transaction payloads as JSON."""
    envelope = Envelope.deserialize(data)
    for item in envelope.items:
        if item.type == "transaction":
            item.payload = PayloadRef(json=json.loads(item.payload.get_bytes()))
    return envelope
