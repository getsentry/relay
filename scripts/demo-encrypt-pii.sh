#!/usr/bin/env bash
#
# Local end-to-end demo of `encrypt` PII rules.
#
# Runs a real Relay in `static` mode -- project config comes from the filesystem, so no Sentry is
# needed -- behind a throwaway upstream that prints whatever Relay forwards to it. Sends an event
# over HTTP, shows the scrubbed result, then decrypts it with the private key.
#
# Usage:
#   ./scripts/demo-encrypt-pii.sh
#
# Requires: cargo, python3 with `zstandard` and `pynacl` (see PYTHON below).
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMO="${DEMO_DIR:-/tmp/relay-encrypt-demo}"
RELAY_PORT="${RELAY_PORT:-3000}"
SINK_PORT="${SINK_PORT:-8999}"
PROJECT_ID=42
SENTRY_KEY="e12d836b15bb49d7bbf99e64295d995b"
info() { printf '\n\033[1;34m==> %s\033[0m\n' "$1"; }

# Find an interpreter that has what the sink and the decrypt script need. `make setup-venv` produces
# a .venv with both (zstandard and PyNaCl are in requirements-dev.txt), so prefer it.
has_deps() { [ -x "$1" ] && "$1" -c 'import zstandard, nacl' 2>/dev/null; }

if [ -n "${PYTHON:-}" ]; then
  if ! has_deps "$PYTHON"; then
    echo "error: \$PYTHON ($PYTHON) cannot import zstandard and nacl" >&2
    exit 1
  fi
else
  for candidate in "$REPO/.venv/bin/python" "$REPO/.venv-demo/bin/python" python3; do
    resolved="$(command -v "$candidate" 2>/dev/null || echo "$candidate")"
    if has_deps "$resolved"; then
      PYTHON="$resolved"
      break
    fi
  done
fi

if [ -z "${PYTHON:-}" ]; then
  cat >&2 <<EOF
error: no Python found with the required modules (zstandard, pynacl).

Either set up the repo venv:

  make setup-venv        # installs requirements-dev.txt, which includes both

or make a throwaway one:

  python3 -m venv .venv-demo && .venv-demo/bin/pip install pynacl zstandard

then re-run this script. To use a specific interpreter:

  PYTHON=/path/to/python $0
EOF
  exit 1
fi
echo "using python: $PYTHON"

# Kill only listeners on our ports. Note: a bare \`lsof -ti:PORT\` also matches processes holding an
# outbound connection to that port, which means it would match Relay itself and kill it.
cleanup() {
  for port in "$RELAY_PORT" "$SINK_PORT"; do
    pids="$(lsof -ti:"$port" -sTCP:LISTEN 2>/dev/null || true)"
    [ -n "$pids" ] && kill $pids 2>/dev/null || true
  done
}
trap cleanup EXIT
cleanup

rm -rf "$DEMO"
mkdir -p "$DEMO/projects"

info "Building relay and process-event"
(cd "$REPO" && cargo build -p relay -p process-event)

info "Generating a keypair"
# Public half goes into the project config; secret half stays here and never reaches Relay.
"$REPO/target/debug/process-event" --keygen 2>"$DEMO/secret.raw" >"$DEMO/keyconfig.json"
sed 's/.*: //' "$DEMO/secret.raw" >"$DEMO/secret.key"
PUBKEY="$("$PYTHON" -c "import json;print(json.load(open('$DEMO/keyconfig.json'))['vars']['publicKey'])")"
echo "  public key (goes to Relay): $PUBKEY"
echo "  secret key (stays here):    $(cat "$DEMO/secret.key")"

info "Writing static project config"
cat >"$DEMO/projects/$PROJECT_ID.json" <<EOF
{
  "projectId": $PROJECT_ID,
  "slug": "encrypt-demo",
  "publicKeys": [
    {"publicKey": "$SENTRY_KEY", "isEnabled": true, "numericId": 1}
  ],
  "config": {
    "allowedDomains": ["*"],
    "trustedRelays": [],
    "piiConfig": {
      "vars": {"publicKey": "$PUBKEY"},
      "rules": {
        "recoverable": {"type": "anything", "redaction": {"method": "encrypt"}}
      },
      "applications": {
        "user.**": ["recoverable"],
        "\$request.data.**": ["recoverable"],
        "extra.**": ["@creditcard:mask"]
      }
    }
  }
}
EOF

cat >"$DEMO/config.yml" <<EOF
relay:
  mode: static
  upstream: http://127.0.0.1:$SINK_PORT/
  host: 127.0.0.1
  port: $RELAY_PORT
logging:
  level: warn
limits:
  shutdown_timeout: 1
EOF

# Throwaway upstream. Relay compresses envelopes with zstd streaming frames, which have no declared
# content size, so `decompress()` fails and `stream_reader` is required.
cat >"$DEMO/sink.py" <<'PYEOF'
import gzip, io, json, zlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import zstandard

OUT = "/tmp/relay-encrypt-demo/captured.json"

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        enc = (self.headers.get("Content-Encoding") or "").lower()
        if enc == "gzip":
            body = gzip.decompress(body)
        elif enc in ("deflate", "zlib"):
            body = zlib.decompress(body)
        elif enc == "zstd":
            with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(body)) as r:
                body = r.read()
        for line in body.split(b"\n"):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict) and "event_id" in obj and len(obj) > 3:
                with open(OUT, "w") as f:
                    json.dump(obj, f, indent=2)
        self._ok()

    def do_GET(self):
        self._ok()

    def _ok(self):
        payload = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *a):
        pass

HTTPServer(("127.0.0.1", 8999), Handler).serve_forever()
PYEOF
sed -i.bak "s|/tmp/relay-encrypt-demo/captured.json|$DEMO/captured.json|" "$DEMO/sink.py"
sed -i.bak "s|(\"127.0.0.1\", 8999)|(\"127.0.0.1\", $SINK_PORT)|" "$DEMO/sink.py"

info "Starting upstream sink on :$SINK_PORT"
"$PYTHON" "$DEMO/sink.py" >"$DEMO/sink.log" 2>&1 &
sleep 1

info "Starting Relay on :$RELAY_PORT (static mode, no Sentry)"
"$REPO/target/debug/relay" run --config "$DEMO" >"$DEMO/relay.log" 2>&1 &
for _ in $(seq 1 30); do
  curl -sf "http://127.0.0.1:$RELAY_PORT/api/relay/healthcheck/ready/" >/dev/null 2>&1 && break
  sleep 1
done

info "Sending an event with PII"
curl -s -X POST \
  "http://127.0.0.1:$RELAY_PORT/api/$PROJECT_ID/store/?sentry_key=$SENTRY_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "event_id": "7b9e89cf79ee451986112e0425fa9fd4",
    "level": "error",
    "message": "Checkout failed",
    "platform": "python",
    "user": {"id": "42", "email": "bruno@example.com"},
    "request": {"url": "https://shop.example.com/checkout", "data": {"ssn": "078-05-1120"}},
    "extra": {"card": "4111111111111111"}
  }' -o /dev/null -w '  HTTP %{http_code}\n'

for _ in $(seq 1 20); do
  [ -f "$DEMO/captured.json" ] && break
  sleep 1
done
if [ ! -f "$DEMO/captured.json" ]; then
  echo "error: upstream never received the event; see $DEMO/relay.log" >&2
  exit 1
fi

info "What Sentry would store (this is all any employee sees)"
"$PYTHON" - "$DEMO/captured.json" <<'PYEOF'
import json, sys
e = json.load(open(sys.argv[1]))
for k in ("user", "request", "extra"):
    if k in e:
        print(f"  {k}: {json.dumps(e[k])}")
blob = e.get("_encrypted_pii", "")
print(f"  _encrypted_pii: {blob[:64]}... ({len(blob)} chars)")
PYEOF

info "Decrypted with the private key"
"$PYTHON" "$REPO/scripts/decrypt-pii.py" decrypt --key-file "$DEMO/secret.key" "$DEMO/captured.json" \
  | sed 's/^/  /'

info "Decrypted with the WRONG key (the point of the whole thing)"
"$PYTHON" "$REPO/scripts/decrypt-pii.py" keygen --key-file "$DEMO/wrong.key" >/dev/null 2>&1
if "$PYTHON" "$REPO/scripts/decrypt-pii.py" decrypt --key-file "$DEMO/wrong.key" \
    "$DEMO/captured.json" 2>&1 | sed 's/^/  /'; then
  echo "  UNEXPECTED: decryption succeeded with the wrong key" >&2
  exit 1
fi

info "Same event twice produces different ciphertext"
first="$("$PYTHON" -c "import json;print(json.load(open('$DEMO/captured.json'))['_encrypted_pii'])")"
rm -f "$DEMO/captured.json"
curl -s -X POST \
  "http://127.0.0.1:$RELAY_PORT/api/$PROJECT_ID/store/?sentry_key=$SENTRY_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"7b9e89cf79ee451986112e0425fa9fd5","message":"Checkout failed","user":{"id":"42","email":"bruno@example.com"}}' \
  -o /dev/null
for _ in $(seq 1 20); do
  [ -f "$DEMO/captured.json" ] && break
  sleep 1
done
second="$("$PYTHON" -c "import json;print(json.load(open('$DEMO/captured.json'))['_encrypted_pii'])")"
if [ "$first" = "$second" ]; then
  echo "  UNEXPECTED: identical ciphertext across events" >&2
  exit 1
fi
echo "  first:  ${first:0:48}..."
echo "  second: ${second:0:48}..."
echo "  differ: yes"

info "Done. Artifacts in $DEMO"
