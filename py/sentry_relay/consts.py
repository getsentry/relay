__all__ = ["SPAN_STATUS_CODE_TO_NAME", "SPAN_STATUS_NAME_TO_CODE"]


SPAN_STATUS_CODE_TO_NAME = {
    0: "ok",
    1: "cancelled",
    2: "unknown_error",
    3: "invalid_argument",
    4: "deadline_exceeded",
    5: "not_found",
    6: "already_exists",
    7: "permission_denied",
    8: "resource_exhausted",
    9: "failed_precondition",
    10: "aborted",
    11: "out_of_range",
    12: "unimplemented",
    13: "internal_error",
    14: "unavailable",
    15: "data_loss",
    16: "unauthenticated",
}

SPAN_STATUS_NAME_TO_CODE = dict((v, k) for k, v in SPAN_STATUS_CODE_TO_NAME.items())
SPAN_STATUS_NAME_TO_CODE["unknown"] = SPAN_STATUS_NAME_TO_CODE["unknown_error"]
