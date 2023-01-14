/* C bindings to the sentry relay library */

#ifndef RELAY_H_INCLUDED
#define RELAY_H_INCLUDED

/* Generated with cbindgen:0.24.3 */

/* Warning, this file is autogenerated. Do not modify this manually. */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Classifies the type of data that is being ingested.
 */
enum RelayDataCategory {
  /**
   * Reserved and unused.
   */
  RELAY_DATA_CATEGORY_DEFAULT = 0,
  /**
   * Error events and Events with an `event_type` not explicitly listed below.
   */
  RELAY_DATA_CATEGORY_ERROR = 1,
  /**
   * Transaction events.
   */
  RELAY_DATA_CATEGORY_TRANSACTION = 2,
  /**
   * Events with an event type of `csp`, `hpkp`, `expectct` and `expectstaple`.
   */
  RELAY_DATA_CATEGORY_SECURITY = 3,
  /**
   * An attachment. Quantity is the size of the attachment in bytes.
   */
  RELAY_DATA_CATEGORY_ATTACHMENT = 4,
  /**
   * Session updates. Quantity is the number of updates in the batch.
   */
  RELAY_DATA_CATEGORY_SESSION = 5,
  /**
   * A profile
   */
  RELAY_DATA_CATEGORY_PROFILE = 6,
  /**
   * Session Replays
   */
  RELAY_DATA_CATEGORY_REPLAY = 7,
  /**
   * DEPRECATED: A transaction for which metrics were extracted.
   *
   * This category is now obsolete because the `Transaction` variant will represent
   * processed transactions from now on.
   */
  RELAY_DATA_CATEGORY_TRANSACTION_PROCESSED = 8,
  /**
   * Indexed transaction events.
   *
   * This is the category for transaction payloads that were accepted and stored in full. In
   * contrast, `transaction` only guarantees that metrics have been accepted for the transaction.
   */
  RELAY_DATA_CATEGORY_TRANSACTION_INDEXED = 9,
  /**
   * Any other data category not known by this Relay.
   */
  RELAY_DATA_CATEGORY_UNKNOWN = -1,
};
typedef int8_t RelayDataCategory;

/**
 * Controls the globbing behaviors.
 */
enum GlobFlags {
  /**
   * When enabled `**` matches over path separators and `*` does not.
   */
  GLOB_FLAGS_DOUBLE_STAR = 1,
  /**
   * Enables case insensitive path matching.
   */
  GLOB_FLAGS_CASE_INSENSITIVE = 2,
  /**
   * Enables path normalization.
   */
  GLOB_FLAGS_PATH_NORMALIZE = 4,
  /**
   * Allows newlines.
   */
  GLOB_FLAGS_ALLOW_NEWLINE = 8,
};
typedef uint32_t GlobFlags;

/**
 * Represents all possible error codes.
 */
enum RelayErrorCode {
  RELAY_ERROR_CODE_NO_ERROR = 0,
  RELAY_ERROR_CODE_PANIC = 1,
  RELAY_ERROR_CODE_UNKNOWN = 2,
  RELAY_ERROR_CODE_INVALID_JSON_ERROR = 101,
  RELAY_ERROR_CODE_KEY_PARSE_ERROR_BAD_ENCODING = 1000,
  RELAY_ERROR_CODE_KEY_PARSE_ERROR_BAD_KEY = 1001,
  RELAY_ERROR_CODE_UNPACK_ERROR_BAD_SIGNATURE = 1003,
  RELAY_ERROR_CODE_UNPACK_ERROR_BAD_PAYLOAD = 1004,
  RELAY_ERROR_CODE_UNPACK_ERROR_SIGNATURE_EXPIRED = 1005,
  RELAY_ERROR_CODE_UNPACK_ERROR_BAD_ENCODING = 1006,
  RELAY_ERROR_CODE_PROCESSING_ERROR_INVALID_TRANSACTION = 2001,
  RELAY_ERROR_CODE_PROCESSING_ERROR_INVALID_GEO_IP = 2002,
  RELAY_ERROR_CODE_INVALID_RELEASE_ERROR_TOO_LONG = 3001,
  RELAY_ERROR_CODE_INVALID_RELEASE_ERROR_RESTRICTED_NAME = 3002,
  RELAY_ERROR_CODE_INVALID_RELEASE_ERROR_BAD_CHARACTERS = 3003,
};
typedef uint32_t RelayErrorCode;

/**
 * Trace status.
 *
 * Values from <https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/api-tracing.md#status>
 * Mapping to HTTP from <https://github.com/open-telemetry/opentelemetry-specification/blob/8fb6c14e4709e75a9aaa64b0dbbdf02a6067682a/specification/data-http.md#status>
 */
enum RelaySpanStatus {
  /**
   * The operation completed successfully.
   *
   * HTTP status 100..299 + successful redirects from the 3xx range.
   */
  RELAY_SPAN_STATUS_OK = 0,
  /**
   * The operation was cancelled (typically by the user).
   */
  RELAY_SPAN_STATUS_CANCELLED = 1,
  /**
   * Unknown. Any non-standard HTTP status code.
   *
   * "We do not know whether the transaction failed or succeeded"
   */
  RELAY_SPAN_STATUS_UNKNOWN = 2,
  /**
   * Client specified an invalid argument. 4xx.
   *
   * Note that this differs from FailedPrecondition. InvalidArgument indicates arguments that
   * are problematic regardless of the state of the system.
   */
  RELAY_SPAN_STATUS_INVALID_ARGUMENT = 3,
  /**
   * Deadline expired before operation could complete.
   *
   * For operations that change the state of the system, this error may be returned even if the
   * operation has been completed successfully.
   *
   * HTTP redirect loops and 504 Gateway Timeout
   */
  RELAY_SPAN_STATUS_DEADLINE_EXCEEDED = 4,
  /**
   * 404 Not Found. Some requested entity (file or directory) was not found.
   */
  RELAY_SPAN_STATUS_NOT_FOUND = 5,
  /**
   * Already exists (409)
   *
   * Some entity that we attempted to create already exists.
   */
  RELAY_SPAN_STATUS_ALREADY_EXISTS = 6,
  /**
   * 403 Forbidden
   *
   * The caller does not have permission to execute the specified operation.
   */
  RELAY_SPAN_STATUS_PERMISSION_DENIED = 7,
  /**
   * 429 Too Many Requests
   *
   * Some resource has been exhausted, perhaps a per-user quota or perhaps the entire file
   * system is out of space.
   */
  RELAY_SPAN_STATUS_RESOURCE_EXHAUSTED = 8,
  /**
   * Operation was rejected because the system is not in a state required for the operation's
   * execution
   */
  RELAY_SPAN_STATUS_FAILED_PRECONDITION = 9,
  /**
   * The operation was aborted, typically due to a concurrency issue.
   */
  RELAY_SPAN_STATUS_ABORTED = 10,
  /**
   * Operation was attempted past the valid range.
   */
  RELAY_SPAN_STATUS_OUT_OF_RANGE = 11,
  /**
   * 501 Not Implemented
   *
   * Operation is not implemented or not enabled.
   */
  RELAY_SPAN_STATUS_UNIMPLEMENTED = 12,
  /**
   * Other/generic 5xx.
   */
  RELAY_SPAN_STATUS_INTERNAL_ERROR = 13,
  /**
   * 503 Service Unavailable
   */
  RELAY_SPAN_STATUS_UNAVAILABLE = 14,
  /**
   * Unrecoverable data loss or corruption
   */
  RELAY_SPAN_STATUS_DATA_LOSS = 15,
  /**
   * 401 Unauthorized (actually does mean unauthenticated according to RFC 7235)
   *
   * Prefer PermissionDenied if a user is logged in.
   */
  RELAY_SPAN_STATUS_UNAUTHENTICATED = 16,
};
typedef uint8_t RelaySpanStatus;

/**
 * A geo ip lookup helper based on maxmind db files.
 */
typedef struct RelayGeoIpLookup RelayGeoIpLookup;

/**
 * Represents a public key in Relay.
 */
typedef struct RelayPublicKey RelayPublicKey;

/**
 * Represents a secret key in Relay.
 */
typedef struct RelaySecretKey RelaySecretKey;

/**
 * The processor that normalizes events for store.
 */
typedef struct RelayStoreNormalizer RelayStoreNormalizer;

/**
 * A length-prefixed UTF-8 string.
 *
 * As opposed to C strings, this string is not null-terminated. If the string is owned, indicated
 * by the `owned` flag, the owner must call the `free` function on this string. The convention is:
 *
 *  - When obtained as instance through return values, always free the string.
 *  - When obtained as pointer through field access, never free the string.
 */
typedef struct RelayStr {
  /**
   * Pointer to the UTF-8 encoded string data.
   */
  char *data;
  /**
   * The length of the string pointed to by `data`.
   */
  uintptr_t len;
  /**
   * Indicates that the string is owned and must be freed.
   */
  bool owned;
} RelayStr;

/**
 * A binary buffer of known length.
 *
 * If the buffer is owned, indicated by the `owned` flag, the owner must call the `free` function
 * on this buffer. The convention is:
 *
 *  - When obtained as instance through return values, always free the buffer.
 *  - When obtained as pointer through field access, never free the buffer.
 */
typedef struct RelayBuf {
  /**
   * Pointer to the raw data.
   */
  uint8_t *data;
  /**
   * The length of the buffer pointed to by `data`.
   */
  uintptr_t len;
  /**
   * Indicates that the buffer is owned and must be freed.
   */
  bool owned;
} RelayBuf;

/**
 * Represents a key pair from key generation.
 */
typedef struct RelayKeyPair {
  /**
   * The public key used for verifying Relay signatures.
   */
  struct RelayPublicKey *public_key;
  /**
   * The secret key used for signing Relay requests.
   */
  struct RelaySecretKey *secret_key;
} RelayKeyPair;

/**
 * A 16-byte UUID.
 */
typedef struct RelayUuid {
  /**
   * UUID bytes in network byte order (big endian).
   */
  uint8_t data[16];
} RelayUuid;

/**
 * Parses a public key from a string.
 */
struct RelayPublicKey *relay_publickey_parse(const struct RelayStr *s);

/**
 * Frees a public key.
 */
void relay_publickey_free(struct RelayPublicKey *spk);

/**
 * Converts a public key into a string.
 */
struct RelayStr relay_publickey_to_string(const struct RelayPublicKey *spk);

/**
 * Verifies a signature
 */
bool relay_publickey_verify(const struct RelayPublicKey *spk,
                            const struct RelayBuf *data,
                            const struct RelayStr *sig);

/**
 * Verifies a signature
 */
bool relay_publickey_verify_timestamp(const struct RelayPublicKey *spk,
                                      const struct RelayBuf *data,
                                      const struct RelayStr *sig,
                                      uint32_t max_age);

/**
 * Parses a secret key from a string.
 */
struct RelaySecretKey *relay_secretkey_parse(const struct RelayStr *s);

/**
 * Frees a secret key.
 */
void relay_secretkey_free(struct RelaySecretKey *spk);

/**
 * Converts a secret key into a string.
 */
struct RelayStr relay_secretkey_to_string(const struct RelaySecretKey *spk);

/**
 * Verifies a signature
 */
struct RelayStr relay_secretkey_sign(const struct RelaySecretKey *spk,
                                     const struct RelayBuf *data);

/**
 * Generates a secret, public key pair.
 */
struct RelayKeyPair relay_generate_key_pair(void);

/**
 * Randomly generates an relay id
 */
struct RelayUuid relay_generate_relay_id(void);

/**
 * Creates a challenge from a register request and returns JSON.
 */
struct RelayStr relay_create_register_challenge(const struct RelayBuf *data,
                                                const struct RelayStr *signature,
                                                const struct RelayStr *secret,
                                                uint32_t max_age);

/**
 * Validates a register response.
 */
struct RelayStr relay_validate_register_response(const struct RelayBuf *data,
                                                 const struct RelayStr *signature,
                                                 const struct RelayStr *secret,
                                                 uint32_t max_age);

/**
 * Returns true if the given version is supported by this library.
 */
bool relay_version_supported(const struct RelayStr *version);

/**
 * Returns the API name of the given `DataCategory`.
 */
struct RelayStr relay_data_category_name(RelayDataCategory category);

/**
 * Parses a `DataCategory` from its API name.
 */
RelayDataCategory relay_data_category_parse(const struct RelayStr *name);

/**
 * Parses a `DataCategory` from an event type.
 */
RelayDataCategory relay_data_category_from_event_type(const struct RelayStr *event_type);

/**
 * Creates a Relay string from a c string.
 */
struct RelayStr relay_str_from_cstr(const char *s);

/**
 * Frees a Relay str.
 *
 * If the string is marked as not owned then this function does not
 * do anything.
 */
void relay_str_free(struct RelayStr *s);

/**
 * Returns true if the uuid is nil.
 */
bool relay_uuid_is_nil(const struct RelayUuid *uuid);

/**
 * Formats the UUID into a string.
 *
 * The string is newly allocated and needs to be released with
 * `relay_cstr_free`.
 */
struct RelayStr relay_uuid_to_str(const struct RelayUuid *uuid);

/**
 * Frees a Relay buf.
 *
 * If the buffer is marked as not owned then this function does not
 * do anything.
 */
void relay_buf_free(struct RelayBuf *b);

/**
 * Initializes the library
 */
void relay_init(void);

/**
 * Returns the last error code.
 *
 * If there is no error, 0 is returned.
 */
RelayErrorCode relay_err_get_last_code(void);

/**
 * Returns the last error message.
 *
 * If there is no error an empty string is returned.  This allocates new memory
 * that needs to be freed with `relay_str_free`.
 */
struct RelayStr relay_err_get_last_message(void);

/**
 * Returns the panic information as string.
 */
struct RelayStr relay_err_get_backtrace(void);

/**
 * Clears the last error.
 */
void relay_err_clear(void);

/**
 * Chunks the given text based on remarks.
 */
struct RelayStr relay_split_chunks(const struct RelayStr *string,
                                   const struct RelayStr *remarks);

/**
 * Opens a maxminddb file by path.
 */
struct RelayGeoIpLookup *relay_geoip_lookup_new(const char *path);

/**
 * Frees a `RelayGeoIpLookup`.
 */
void relay_geoip_lookup_free(struct RelayGeoIpLookup *lookup);

/**
 * Returns a list of all valid platform identifiers.
 */
const struct RelayStr *relay_valid_platforms(uintptr_t *size_out);

/**
 * Creates a new normalization processor.
 */
struct RelayStoreNormalizer *relay_store_normalizer_new(const struct RelayStr *config,
                                                        const struct RelayGeoIpLookup *geoip_lookup);

/**
 * Frees a `RelayStoreNormalizer`.
 */
void relay_store_normalizer_free(struct RelayStoreNormalizer *normalizer);

/**
 * Normalizes the event given as JSON.
 */
struct RelayStr relay_store_normalizer_normalize_event(struct RelayStoreNormalizer *normalizer,
                                                       const struct RelayStr *event);

/**
 * Replaces invalid JSON generated by Python.
 */
bool relay_translate_legacy_python_json(struct RelayStr *event);

/**
 * Validate a PII config against the schema. Used in project options UI.
 */
struct RelayStr relay_validate_pii_config(const struct RelayStr *value);

/**
 * Convert an old datascrubbing config to the new PII config format.
 */
struct RelayStr relay_convert_datascrubbing_config(const struct RelayStr *config);

/**
 * Scrub an event using new PII stripping config.
 */
struct RelayStr relay_pii_strip_event(const struct RelayStr *config,
                                      const struct RelayStr *event);

/**
 * Walk through the event and collect selectors that can be applied to it in a PII config. This
 * function is used in the UI to provide auto-completion of selectors.
 */
struct RelayStr relay_pii_selector_suggestions_from_event(const struct RelayStr *event);

/**
 * A test function that always panics.
 */
void relay_test_panic(void);

/**
 * Performs a glob operation on bytes.
 *
 * Returns `true` if the glob matches, `false` otherwise.
 */
bool relay_is_glob_match(const struct RelayBuf *value,
                         const struct RelayStr *pat,
                         GlobFlags flags);

/**
 * Converts a codeowners path into a regex and searches for match against the provided value.
 *
 * Returns `true` if the regex matches, `false` otherwise.
 */
bool relay_is_codeowners_path_match(const struct RelayBuf *value,
                         const struct RelayStr *pat);

/**
 * Parse a sentry release structure from a string.
 */
struct RelayStr relay_parse_release(const struct RelayStr *value);

/**
 * Compares two versions.
 */
int32_t relay_compare_versions(const struct RelayStr *a,
                               const struct RelayStr *b);

/**
 * Validate a sampling rule condition.
 */
struct RelayStr relay_validate_sampling_condition(const struct RelayStr *value);

/**
 * Validate whole rule ( this will be also implemented in Sentry for better error messages)
 * The implementation in relay is just to make sure that the Sentry implementation doesn't
 * go out of sync.
 */
struct RelayStr relay_validate_sampling_configuration(const struct RelayStr *value);

#endif /* RELAY_H_INCLUDED */
