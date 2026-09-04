from enum import IntEnum


class Outcome(IntEnum):
    """
    The numerical identifier of the outcome category.

    Mirrors the definition in: relay-server/src/services/outcome/mod.rs
    """

    ACCEPTED = 0
    FILTERED = 1
    RATE_LIMITED = 2
    INVALID = 3
    ABUSE = 4
    CLIENT_DISCARD = 5
    CARDINALITY_LIMITED = 6


# Minimum supported version for generic metrics extraction by Relay.
METRICS_EXTRACTION_MIN_SUPPORTED_VERSION = 4

DUMMY_UPLOAD_PATH = "/api/42/upload/019cdc82ed6c7761ba21fd34b86481c2/"
DUMMY_UPLOAD_LOCATION = f"{DUMMY_UPLOAD_PATH}?upload_length=11&upload_id=my_upload&upload_signature=z_fUMhT0EZqJz6OQtwGHqTlOOLPpTVpvPa-rYTg18FVWZM1OGny-LeVJB5H-sSR_5e--I1xt-FlCmRG2bsmcAQ.eyJ0IjoiMjAyNi0wMy0xMVQxMDo0ODoxMy45NDM1ODNaIn0"

ZSTD_MAGIC_HEADER = b"\x28\xb5\x2f\xfd"
