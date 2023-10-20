use std::collections::BTreeMap;

use once_cell::sync::Lazy;

pub static HTTP: Lazy<BTreeMap<i64, &str>> = Lazy::new(|| {
    BTreeMap::from([
        (400, "failed_precondition"),
        (401, "unauthenticated"),
        (403, "permission_denied"),
        (404, "not_found"),
        (409, "aborted"),
        (429, "resource_exhausted"),
        (499, "cancelled"),
        (500, "internal_error"),
        (501, "unimplemented"),
        (503, "unavailable"),
        (504, "deadline_exceeded"),
    ])
});

pub static GRPC: Lazy<BTreeMap<i64, &str>> = Lazy::new(|| {
    BTreeMap::from([
        (1, "cancelled"),
        (2, "unknown_error"),
        (3, "invalid_argument"),
        (4, "deadline_exceeded"),
        (5, "not_found"),
        (6, "already_exists"),
        (7, "permission_denied"),
        (8, "resource_exhausted"),
        (9, "failed_precondition"),
        (10, "aborted"),
        (11, "out_of_range"),
        (12, "unimplemented"),
        (13, "internal_error"),
        (14, "unavailable"),
        (15, "data_loss"),
        (16, "unauthenticated"),
    ])
});
