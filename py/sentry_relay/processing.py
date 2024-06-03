from __future__ import annotations
from ._relay_pyo3 import RustStoreNormalizer, validate_sampling_configuration, compare_versions, validate_pii_selector, validate_pii_config, validate_rule_condition, validate_sampling_condition, is_codeowners_path_match, split_chunks, _pii_strip_event, parse_release, normalize_global_config, _pii_selector_suggestions_from_event, convert_datascrubbing_config, is_glob_match, meta_with_chunks, normalize_cardinality_limit_config, normalize_project_config, VALID_PLATFORMS, GeoIpLookup

import json
from typing import Callable, Any

__all__ = [
    "split_chunks",
    "meta_with_chunks",
    "StoreNormalizer",
    "GeoIpLookup",
    "is_glob_match",
    "is_codeowners_path_match",
    "parse_release",
    "validate_pii_selector",
    "validate_pii_config",
    "convert_datascrubbing_config",
    "pii_strip_event",
    "pii_selector_suggestions_from_event",
    "VALID_PLATFORMS",
    "validate_rule_condition",
    "validate_sampling_condition",
    "validate_sampling_configuration",
    "normalize_project_config",
    "normalize_cardinality_limit_config",
    "normalize_global_config",
]

class StoreNormalizer:
    def __init__(
        self, **config
    ):
        self.inner = RustStoreNormalizer(**config)

    def normalize_event(
        self,
        event=None,
        raw_event=None,
    ):
        if raw_event is None:
            raw_event = json.dumps(event, ensure_ascii=False)
        if isinstance(raw_event, str):
            raw_event = raw_event.encode(errors="replace")

        rv = self.inner.normalize_event(raw_event)
        return json.loads(rv)

def pii_strip_event(
    config,
    event,
):
    event = json.dumps(event)
    rv = _pii_strip_event(config, event)
    return json.loads(rv)

def pii_selector_suggestions_from_event(event):
    event = json.dumps(event)
    rv = _pii_selector_suggestions_from_event(event)
    return json.loads(rv)
    
