from typing import Any

VALID_PLATFORMS: set[str]

class GeoIpLookup:
    @staticmethod
    def open(path: str) -> GeoIpLookup:
        """Opens a maxminddb file by path."""
        ...

    @staticmethod
    def from_path(path: str) -> GeoIpLookup:
        """Opens a maxminddb file by path."""
        ...

    def lookup(self, ip_address: str) -> Any | None:
        """Looks up an IP address"""
        ...

class StoreNormalizer:
    def __new__(cls, *args: object, **kwargs: object) -> StoreNormalizer: ...
    def normalize_event(
        self, event: dict | None = None, raw_event: str | bytes | None = None
    ) -> Any: ...

def validate_sampling_configuration(condition: bytes | str) -> None:
    """
    Validate the whole sampling configuration. Used in dynamic sampling serializer.
    The parameter is a string containing the rules configuration as JSON.
    """
    ...

def compare_versions(lhs: bytes | str, rhs: bytes | str) -> int:
    """Compares two versions with each other and returns 1/0/-1."""
    ...

def validate_pii_selector(selector: str) -> None:
    """
    Validate a PII selector spec. Used to validate data-scrubbing safe fields.
    """
    ...

def validate_pii_config(config: str) -> None:
    """
    Validate a PII config against the schema. Used in project options UI.

    The parameter is a JSON-encoded string. We should pass the config through
    as a string such that line numbers from the error message match with what
    the user typed in.
    """
    ...

def validate_rule_condition(condition: str) -> None:
    """
    Validate a dynamic rule condition. Used by dynamic sampling, metric extraction, and metric
    tagging.

    :param condition: A string containing the condition encoded as JSON.
    """
    ...

def validate_sampling_condition(condition: str) -> None:
    """
    Deprecated legacy alias. Please use ``validate_rule_condition`` instead.
    """
    ...

def is_codeowners_path_match(value: bytes, pattern: str) -> bool: ...
def split_chunks(input: str, remarks: list[Any]) -> Any: ...
def pii_strip_event(config: Any, event: str) -> Any:
    """
    Scrub an event using new PII stripping config.
    """
    ...

def parse_release(release: str) -> dict[str, Any]:
    """Parses a release string into a dictionary of its components."""
    ...

def normalize_global_config(config: object) -> Any:
    """Normalize the global config.

    Normalization consists of deserializing and serializing back the given
    global config. If deserializing fails, throw an exception. Note that even if
    the roundtrip doesn't produce errors, the given config may differ from
    normalized one.

    :param config: the global config to validate.
    """
    ...

def pii_selector_suggestions_from_event(event: object) -> Any:
    """
    Walk through the event and collect selectors that can be applied to it in a
    PII config. This function is used in the UI to provide auto-completion of
    selectors.
    """
    ...

def convert_datascrubbing_config(config: object) -> Any:
    """
    Convert an old datascrubbing config to the new PII config format.
    """
    ...

def init_valid_platforms() -> set[str]:
    """
    Initialize the set of valid platforms.
    """
    ...

def is_glob_match(
    value: str,
    pat: str,
    double_star: bool | None,
    case_insensitive: bool | None,
    path_normalize: bool | None,
    allow_newline: bool | None,
) -> bool:
    """
    Check if a value matches a pattern.
    """
    ...

def meta_with_chunks(data: str | object, meta: Any) -> Any:
    """
    Add the chunks to the meta data.
    """
    ...

def normalize_cardinality_limit_config(config: dict) -> dict:
    """Normalize the cardinality limit config.

    Normalization consists of deserializing and serializing back the given
    cardinality limit config. If deserializing fails, throw an exception. Note that even if
    the roundtrip doesn't produce errors, the given config may differ from
    normalized one.

    :param config: the cardinality limit config to validate.
    """
    ...

def normalize_project_config(config: dict) -> dict:
    """Normalize a project config.

    :param config: the project config to validate.
    :param json_dumps: a function that stringifies python objects
    :param json_loads: a function that parses and converts JSON strings
    """
    ...
