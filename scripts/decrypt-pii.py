#!/usr/bin/env python3
"""Decrypt the `_encrypted_pii` payload Relay attaches to an event.

Relay only ever holds the public half of the keypair, so this script -- run wherever the private key
lives -- is the only thing that can read those values back.

Requires PyNaCl:

    pip install pynacl

Generate a keypair (the public half goes into the project's PII config under `vars.publicKey`, the
secret half stays with you):

    ./scripts/decrypt-pii.py keygen

Recover the originals from an event:

    ./scripts/decrypt-pii.py decrypt --key-file secret.key event.json
"""

import argparse
import base64
import json
import sys

from nacl.public import PrivateKey, SealedBox

ENCRYPTED_PII_KEY = "_encrypted_pii"


def keygen(args):
    secret = PrivateKey.generate()
    public_b64 = base64.b64encode(bytes(secret.public_key)).decode()
    secret_b64 = base64.b64encode(bytes(secret)).decode()

    if args.key_file:
        with open(args.key_file, "w") as f:
            f.write(secret_b64 + "\n")
        print(f"secret key written to {args.key_file}", file=sys.stderr)
    else:
        print(f"secret key (keep this private): {secret_b64}", file=sys.stderr)

    print("Add this to your project's PII config:", file=sys.stderr)
    print(json.dumps({"vars": {"publicKey": public_b64}}, indent=2))


def decrypt(args):
    with open(args.key_file) as f:
        secret = PrivateKey(base64.b64decode(f.read().strip()))

    with open(args.event) as f:
        event = json.load(f)

    sealed = event.get(ENCRYPTED_PII_KEY)
    if not sealed:
        print(f"event has no {ENCRYPTED_PII_KEY} payload", file=sys.stderr)
        return 1

    try:
        opened = SealedBox(secret).decrypt(base64.b64decode(sealed))
    except Exception as e:
        print(f"failed to decrypt: {e}", file=sys.stderr)
        print("(is this the private key matching the configured public key?)", file=sys.stderr)
        return 1

    values = json.loads(opened)

    if args.json:
        print(json.dumps(values, indent=2))
    else:
        width = max(len(path) for path in values) if values else 0
        for path, value in values.items():
            print(f"{path.ljust(width)}  {value}")

    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_keygen = sub.add_parser("keygen", help="generate an X25519 keypair")
    p_keygen.add_argument("--key-file", help="write the secret key here instead of stdout")
    p_keygen.set_defaults(func=keygen)

    p_decrypt = sub.add_parser("decrypt", help="recover encrypted values from an event")
    p_decrypt.add_argument("event", help="path to the event JSON")
    p_decrypt.add_argument("--key-file", required=True, help="file holding the base64 secret key")
    p_decrypt.add_argument("--json", action="store_true", help="output JSON instead of a table")
    p_decrypt.set_defaults(func=decrypt)

    args = parser.parse_args()
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
