/* C bindings to the sentry relay library */

#ifndef RELAY_H_INCLUDED
#define RELAY_H_INCLUDED

/* Generated with cbindgen:0.12.2 */

/* Warning, this file is autogenerated. Do not modify this manually. */

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Controls the globbing behaviors.
 */
enum GlobFlags {
  GLOB_FLAGS_DOUBLE_STAR = 1,
  GLOB_FLAGS_CASE_INSENSITIVE = 2,
  GLOB_FLAGS_PATH_NORMALIZE = 4,
  GLOB_FLAGS_ALLOW_NEWLINE = 8,
};
typedef uint32_t GlobFlags;

/**
 * Represents all possible error codes
 */
enum RelayErrorCode {
  RELAY_ERROR_CODE_NO_ERROR = 0,
  RELAY_ERROR_CODE_PANIC = 1,
  RELAY_ERROR_CODE_UNKNOWN = 2,
  RELAY_ERROR_CODE_KEY_PARSE_ERROR_BAD_ENCODING = 1000,
  RELAY_ERROR_CODE_KEY_PARSE_ERROR_BAD_KEY = 1001,
  RELAY_ERROR_CODE_UNPACK_ERROR_BAD_SIGNATURE = 1003,
  RELAY_ERROR_CODE_UNPACK_ERROR_BAD_PAYLOAD = 1004,
  RELAY_ERROR_CODE_UNPACK_ERROR_SIGNATURE_EXPIRED = 1005,
  RELAY_ERROR_CODE_PROCESSING_ACTION_INVALID_TRANSACTION = 2000,
};
typedef uint32_t RelayErrorCode;

typedef struct RelayGeoIpLookup RelayGeoIpLookup;

/**
 * Represents a public key in relay.
 */
typedef struct RelayPublicKey RelayPublicKey;

/**
 * Represents a secret key in relay.
 */
typedef struct RelaySecretKey RelaySecretKey;

typedef struct RelayStoreNormalizer RelayStoreNormalizer;

/**
 * Represents a buffer.
 */
typedef struct {
  uint8_t *data;
  uintptr_t len;
  bool owned;
} RelayBuf;

/**
 * Represents a string.
 */
typedef struct {
  char *data;
  uintptr_t len;
  bool owned;
} RelayStr;

/**
 * Represents a key pair from key generation.
 */
typedef struct {
  RelayPublicKey *public_key;
  RelaySecretKey *secret_key;
} RelayKeyPair;

/**
 * Represents a uuid.
 */
typedef struct {
  uint8_t data[16];
} RelayUuid;

/**
 * Frees a Relay buf.
 *
 * If the buffer is marked as not owned then this function does not
 * do anything.
 */
void relay_buf_free(RelayBuf *b);

/**
 * Creates a challenge from a register request and returns JSON.
 */
RelayStr relay_create_register_challenge(const RelayBuf *data,
                                         const RelayStr *signature,
                                         uint32_t max_age);

/**
 * Clears the last error.
 */
void relay_err_clear(void);

/**
 * Returns the panic information as string.
 */
RelayStr relay_err_get_backtrace(void);

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
RelayStr relay_err_get_last_message(void);

/**
 * Generates a secret, public key pair.
 */
RelayKeyPair relay_generate_key_pair(void);

/**
 * Randomly generates an relay id
 */
RelayUuid relay_generate_relay_id(void);

void relay_geoip_lookup_free(RelayGeoIpLookup *lookup);

RelayGeoIpLookup *relay_geoip_lookup_new(const char *path);

/**
 * Given just the data from a register response returns the
 * conained relay id without validating the signature.
 */
RelayUuid relay_get_register_response_relay_id(const RelayBuf *data);

/**
 * Initializes the library
 */
void relay_init(void);

bool relay_is_glob_match(const RelayBuf *value,
                         const RelayStr *pat,
                         GlobFlags flags);

RelayStr relay_parse_release(const RelayStr *value);

/**
 * Frees a public key.
 */
void relay_publickey_free(RelayPublicKey *spk);

/**
 * Parses a public key from a string.
 */
RelayPublicKey *relay_publickey_parse(const RelayStr *s);

/**
 * Converts a public key into a string.
 */
RelayStr relay_publickey_to_string(const RelayPublicKey *spk);

/**
 * Verifies a signature
 */
bool relay_publickey_verify(const RelayPublicKey *spk,
                            const RelayBuf *data,
                            const RelayStr *sig);

/**
 * Verifies a signature
 */
bool relay_publickey_verify_timestamp(const RelayPublicKey *spk,
                                      const RelayBuf *data,
                                      const RelayStr *sig,
                                      uint32_t max_age);

RelayStr relay_scrub_event(const RelayStr *config, const RelayStr *event);

/**
 * Frees a secret key.
 */
void relay_secretkey_free(RelaySecretKey *spk);

/**
 * Parses a secret key from a string.
 */
RelaySecretKey *relay_secretkey_parse(const RelayStr *s);

/**
 * Verifies a signature
 */
RelayStr relay_secretkey_sign(const RelaySecretKey *spk, const RelayBuf *data);

/**
 * Converts a secret key into a string.
 */
RelayStr relay_secretkey_to_string(const RelaySecretKey *spk);

RelayStr relay_split_chunks(const RelayStr *string, const RelayStr *remarks);

void relay_store_normalizer_free(RelayStoreNormalizer *normalizer);

RelayStoreNormalizer *relay_store_normalizer_new(const RelayStr *config,
                                                 const RelayGeoIpLookup *geoip_lookup);

RelayStr relay_store_normalizer_normalize_event(RelayStoreNormalizer *normalizer,
                                                const RelayStr *event);

/**
 * Frees a Relay str.
 *
 * If the string is marked as not owned then this function does not
 * do anything.
 */
void relay_str_free(RelayStr *s);

/**
 * Creates a Relay str from a c string.
 *
 * This sets the string to owned.  In case it's not owned you either have
 * to make sure you are not freeing the memory or you need to set the
 * owned flag to false.
 */
RelayStr relay_str_from_cstr(const char *s);

void relay_test_panic(void);

bool relay_translate_legacy_python_json(RelayStr *event);

/**
 * Returns true if the uuid is nil
 */
bool relay_uuid_is_nil(const RelayUuid *uuid);

/**
 * Formats the UUID into a string.
 *
 * The string is newly allocated and needs to be released with
 * `relay_cstr_free`.
 */
RelayStr relay_uuid_to_str(const RelayUuid *uuid);

/**
 * Returns a list of all valid platform identifiers.
 */
const RelayStr *relay_valid_platforms(uintptr_t *size_out);

/**
 * Validates a register response.
 */
RelayStr relay_validate_register_response(const RelayPublicKey *pk,
                                          const RelayBuf *data,
                                          const RelayStr *signature,
                                          uint32_t max_age);

#endif /* RELAY_H_INCLUDED */
