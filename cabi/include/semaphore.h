/* c bindings to the sentry relay library */

#ifndef SEMAPHORE_H_INCLUDED
#define SEMAPHORE_H_INCLUDED

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Controls the globbing behaviors
 */
enum GlobFlags {
  GLOB_FLAGS_DOUBLE_STAR = 1,
  GLOB_FLAGS_CASE_INSENSITIVE = 2,
  GLOB_FLAGS_PATH_NORMALIZE = 4,
};
typedef uint32_t GlobFlags;

/**
 * Represents all possible error codes
 */
enum SemaphoreErrorCode {
  SEMAPHORE_ERROR_CODE_NO_ERROR = 0,
  SEMAPHORE_ERROR_CODE_PANIC = 1,
  SEMAPHORE_ERROR_CODE_UNKNOWN = 2,
  SEMAPHORE_ERROR_CODE_KEY_PARSE_ERROR_BAD_ENCODING = 1000,
  SEMAPHORE_ERROR_CODE_KEY_PARSE_ERROR_BAD_KEY = 1001,
  SEMAPHORE_ERROR_CODE_UNPACK_ERROR_BAD_SIGNATURE = 1003,
  SEMAPHORE_ERROR_CODE_UNPACK_ERROR_BAD_PAYLOAD = 1004,
  SEMAPHORE_ERROR_CODE_UNPACK_ERROR_SIGNATURE_EXPIRED = 1005,
};
typedef uint32_t SemaphoreErrorCode;

typedef struct SemaphoreGeoIpLookup SemaphoreGeoIpLookup;

/**
 * Represents a public key in semaphore.
 */
typedef struct SemaphorePublicKey SemaphorePublicKey;

/**
 * Represents a secret key in semaphore.
 */
typedef struct SemaphoreSecretKey SemaphoreSecretKey;

typedef struct SemaphoreStoreNormalizer SemaphoreStoreNormalizer;

/**
 * Represents a buffer.
 */
typedef struct {
  uint8_t *data;
  uintptr_t len;
  bool owned;
} SemaphoreBuf;

/**
 * Represents a string.
 */
typedef struct {
  char *data;
  uintptr_t len;
  bool owned;
} SemaphoreStr;

/**
 * Represents a key pair from key generation.
 */
typedef struct {
  SemaphorePublicKey *public_key;
  SemaphoreSecretKey *secret_key;
} SemaphoreKeyPair;

/**
 * Represents a uuid.
 */
typedef struct {
  uint8_t data[16];
} SemaphoreUuid;

/**
 * Frees a semaphore buf.
 *
 * If the buffer is marked as not owned then this function does not
 * do anything.
 */
void semaphore_buf_free(SemaphoreBuf *b);

/**
 * Creates a challenge from a register request and returns JSON.
 */
SemaphoreStr semaphore_create_register_challenge(const SemaphoreBuf *data,
                                                 const SemaphoreStr *signature,
                                                 uint32_t max_age);

/**
 * Clears the last error.
 */
void semaphore_err_clear(void);

/**
 * Returns the panic information as string.
 */
SemaphoreStr semaphore_err_get_backtrace(void);

/**
 * Returns the last error code.
 *
 * If there is no error, 0 is returned.
 */
SemaphoreErrorCode semaphore_err_get_last_code(void);

/**
 * Returns the last error message.
 *
 * If there is no error an empty string is returned.  This allocates new memory
 * that needs to be freed with `semaphore_str_free`.
 */
SemaphoreStr semaphore_err_get_last_message(void);

/**
 * Generates a secret, public key pair.
 */
SemaphoreKeyPair semaphore_generate_key_pair(void);

/**
 * Randomly generates an relay id
 */
SemaphoreUuid semaphore_generate_relay_id(void);

void semaphore_geoip_lookup_free(SemaphoreGeoIpLookup *lookup);

SemaphoreGeoIpLookup *semaphore_geoip_lookup_new(const char *path);

/**
 * Given just the data from a register response returns the
 * conained relay id without validating the signature.
 */
SemaphoreUuid semaphore_get_register_response_relay_id(const SemaphoreBuf *data);

/**
 * Initializes the library
 */
void semaphore_init(void);

bool semaphore_is_glob_match(const SemaphoreBuf *value, const SemaphoreStr *pat, GlobFlags flags);

/**
 * Frees a public key.
 */
void semaphore_publickey_free(SemaphorePublicKey *spk);

/**
 * Parses a public key from a string.
 */
SemaphorePublicKey *semaphore_publickey_parse(const SemaphoreStr *s);

/**
 * Converts a public key into a string.
 */
SemaphoreStr semaphore_publickey_to_string(const SemaphorePublicKey *spk);

/**
 * Verifies a signature
 */
bool semaphore_publickey_verify(const SemaphorePublicKey *spk,
                                const SemaphoreBuf *data,
                                const SemaphoreStr *sig);

/**
 * Verifies a signature
 */
bool semaphore_publickey_verify_timestamp(const SemaphorePublicKey *spk,
                                          const SemaphoreBuf *data,
                                          const SemaphoreStr *sig,
                                          uint32_t max_age);

SemaphoreStr semaphore_scrub_event(const SemaphoreStr *config, const SemaphoreStr *event);

/**
 * Frees a secret key.
 */
void semaphore_secretkey_free(SemaphoreSecretKey *spk);

/**
 * Parses a secret key from a string.
 */
SemaphoreSecretKey *semaphore_secretkey_parse(const SemaphoreStr *s);

/**
 * Verifies a signature
 */
SemaphoreStr semaphore_secretkey_sign(const SemaphoreSecretKey *spk, const SemaphoreBuf *data);

/**
 * Converts a secret key into a string.
 */
SemaphoreStr semaphore_secretkey_to_string(const SemaphoreSecretKey *spk);

SemaphoreStr semaphore_split_chunks(const SemaphoreStr *string, const SemaphoreStr *remarks);

void semaphore_store_normalizer_free(SemaphoreStoreNormalizer *normalizer);

SemaphoreStoreNormalizer *semaphore_store_normalizer_new(const SemaphoreStr *config,
                                                         const SemaphoreGeoIpLookup *geoip_lookup);

SemaphoreStr semaphore_store_normalizer_normalize_event(SemaphoreStoreNormalizer *normalizer,
                                                        const SemaphoreStr *event);

/**
 * Frees a semaphore str.
 *
 * If the string is marked as not owned then this function does not
 * do anything.
 */
void semaphore_str_free(SemaphoreStr *s);

/**
 * Creates a semaphore str from a c string.
 *
 * This sets the string to owned.  In case it's not owned you either have
 * to make sure you are not freeing the memory or you need to set the
 * owned flag to false.
 */
SemaphoreStr semaphore_str_from_cstr(const char *s);

void semaphore_test_panic(void);

bool semaphore_translate_legacy_python_json(SemaphoreStr *event);

/**
 * Returns true if the uuid is nil
 */
bool semaphore_uuid_is_nil(const SemaphoreUuid *uuid);

/**
 * Formats the UUID into a string.
 *
 * The string is newly allocated and needs to be released with
 * `semaphore_cstr_free`.
 */
SemaphoreStr semaphore_uuid_to_str(const SemaphoreUuid *uuid);

/**
 * Validates a register response.
 */
SemaphoreStr semaphore_validate_register_response(const SemaphorePublicKey *pk,
                                                  const SemaphoreBuf *data,
                                                  const SemaphoreStr *signature,
                                                  uint32_t max_age);

#endif /* SEMAPHORE_H_INCLUDED */
