"""Helpers for working with TUS uploads."""

from urllib.parse import parse_qs, urlencode, urlparse

# Dummy payloads
FIRST_CHUNK = b"first chunk of a resumable upload|"
SECOND_CHUNK = b"second chunk completing it"
RESUMABLE_DATA = FIRST_CHUNK + SECOND_CHUNK


def location_parts(location):
    """Splits a TUS location into its path and a flat dict of query params."""
    parsed = urlparse(location)
    return parsed.path, {k: v[0] for k, v in parse_qs(parsed.query).items()}


def rebuild_location(path, params):
    """Inverse of `location_parts`."""
    return f"{path}?{urlencode(params)}"


def patch_chunk(relay, location, project_key, chunk, offset, headers=None):
    """Sends one TUS PATCH with the standard headers.

    Entries in `headers` override the defaults; a value of `None` removes the
    header entirely.
    """
    all_headers = {
        "X-Decoded-Content-Length": str(len(chunk)),
        "Content-Type": "application/offset+octet-stream",
        "Tus-Resumable": "1.0.0",
        "Upload-Offset": str(offset),
    }
    all_headers.update(headers or {})
    all_headers = {k: v for k, v in all_headers.items() if v is not None}
    return relay.patch(
        f"{location}&sentry_key={project_key}", headers=all_headers, data=chunk
    )
