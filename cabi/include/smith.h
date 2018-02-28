/* c bindings to the sentry relay library */

#ifndef SMITH_H_INCLUDED
#define SMITH_H_INCLUDED

#include <stdint.h>
#include <stdlib.h>
#include <stdbool.h>

/*
 * Represents all possible error codes
 */
enum SmithErrorCode {
  SMITH_ERROR_CODE_NO_ERROR = 0,
  SMITH_ERROR_CODE_PANIC = 1,
  SMITH_ERROR_CODE_UNKNOWN = 2,
  SMITH_ERROR_CODE_KEY_PARSE_ERROR_BAD_ENCODING = 1000,
  SMITH_ERROR_CODE_KEY_PARSE_ERROR_BAD_KEY = 1001,
  SMITH_ERROR_CODE_UNPACK_ERROR_BAD_SIGNATURE = 1003,
  SMITH_ERROR_CODE_UNPACK_ERROR_BAD_PAYLOAD = 1004,
  SMITH_ERROR_CODE_UNPACK_ERROR_SIGNATURE_EXPIRED = 1005,
};
typedef uint32_t SmithErrorCode;

/*
 * Represents a public key in smith.
 */
typedef struct SmithPublicKey SmithPublicKey;

/*
 * Represents a secret key in smith.
 */
typedef struct SmithSecretKey SmithSecretKey;

/*
 * Represents a buffer.
 */
typedef struct {
  uint8_t *data;
  size_t len;
  bool owned;
} SmithBuf;

/*
 * Represents a string.
 */
typedef struct {
  char *data;
  size_t len;
  bool owned;
} SmithStr;

/*
 * Represents a key pair from key generation.
 */
typedef struct {
  SmithPublicKey *public_key;
  SmithSecretKey *secret_key;
} SmithKeyPair;

/*
 * Represents a uuid.
 */
typedef struct {
  uint8_t data[16];
} SmithUuid;

/*
 * Frees a smith buf.
 *
 * If the buffer is marked as not owned then this function does not
 * do anything.
 */
void smith_buf_free(SmithBuf *b);

/*
 * Creates a challenge from a register request and returns JSON.
 */
SmithStr smith_create_register_challenge(const SmithBuf *data,
                                         const SmithStr *signature,
                                         uint32_t max_age);

/*
 * Clears the last error.
 */
void smith_err_clear(void);

/*
 * Returns the panic information as string.
 */
SmithStr smith_err_get_backtrace(void);

/*
 * Returns the last error code.
 *
 * If there is no error, 0 is returned.
 */
SmithErrorCode smith_err_get_last_code(void);

/*
 * Returns the last error message.
 *
 * If there is no error an empty string is returned.  This allocates new memory
 * that needs to be freed with `smith_str_free`.
 */
SmithStr smith_err_get_last_message(void);

/*
 * Generates a secret, public key pair.
 */
SmithKeyPair smith_generate_key_pair(void);

/*
 * Randomly generates an relay id
 */
SmithUuid smith_generate_relay_id(void);

/*
 * Initializes the library
 */
void smith_init(void);

/*
 * Frees a public key.
 */
void smith_publickey_free(SmithPublicKey *spk);

/*
 * Parses a public key from a string.
 */
SmithPublicKey *smith_publickey_parse(const SmithStr *s);

/*
 * Converts a public key into a string.
 */
SmithStr smith_publickey_to_string(const SmithPublicKey *spk);

/*
 * Verifies a signature
 */
bool smith_publickey_verify(const SmithPublicKey *spk, const SmithBuf *data, const SmithStr *sig);

/*
 * Frees a secret key.
 */
void smith_secretkey_free(SmithSecretKey *spk);

/*
 * Parses a secret key from a string.
 */
SmithSecretKey *smith_secretkey_parse(const SmithStr *s);

/*
 * Verifies a signature
 */
SmithStr smith_secretkey_sign(const SmithSecretKey *spk, const SmithBuf *data);

/*
 * Converts a secret key into a string.
 */
SmithStr smith_secretkey_to_string(const SmithSecretKey *spk);

/*
 * Frees a smith str.
 *
 * If the string is marked as not owned then this function does not
 * do anything.
 */
void smith_str_free(SmithStr *s);

/*
 * Creates a smith str from a c string.
 *
 * This sets the string to owned.  In case it's not owned you either have
 * to make sure you are not freeing the memory or you need to set the
 * owned flag to false.
 */
SmithStr smith_str_from_cstr(const char *s);

/*
 * Returns true if the uuid is nil
 */
bool smith_uuid_is_nil(const SmithUuid *uuid);

/*
 * Formats the UUID into a string.
 *
 * The string is newly allocated and needs to be released with
 * `smith_cstr_free`.
 */
SmithStr smith_uuid_to_str(const SmithUuid *uuid);

/*
 * Validates a register response.
 */
SmithStr smith_validate_register_response(const SmithPublicKey *pk,
                                          const SmithBuf *data,
                                          const SmithStr *signature,
                                          uint32_t max_age);

#endif /* SMITH_H_INCLUDED */
