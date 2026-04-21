#!/usr/bin/env node
// Mock receiver for Relay's HTTP fanout tee.
//
// Stands in for the backend endpoint the forked Relay will POST to in
// Component 2 of the plan. Zero dependencies; Node 20+ stdlib only.
//
// Usage:
//   node dev/mock-fanout-receiver.mjs
//   PORT=3100 PREVIEW_BYTES=256 DECOMPRESS=1 node dev/mock-fanout-receiver.mjs

import http from "node:http";
import { Buffer } from "node:buffer";
import zlib from "node:zlib";
import { execFileSync, spawnSync } from "node:child_process";

const PORT = Number(process.env.PORT ?? 3100);
const HOST = process.env.HOST ?? "127.0.0.1";
const EXPECTED_PATH = process.env.PATH_OVERRIDE ?? "/api/internal/sentry-envelope";
const PREVIEW_BYTES = Number(process.env.PREVIEW_BYTES ?? 0);
const DECOMPRESS = process.env.DECOMPRESS === "1";

// Detect which zstd decoder we can use. Node 22.15+ ships `zlib.zstdDecompressSync`;
// earlier versions need the system `zstd` binary. We pick one at startup and log it,
// so a silent "it just didn't decompress" outcome is impossible.
function detectZstd() {
  if (typeof zlib.zstdDecompressSync === "function") {
    return { kind: "node-zlib", decode: (buf) => zlib.zstdDecompressSync(buf) };
  }
  const probe = spawnSync("zstd", ["--version"], { stdio: "ignore" });
  if (probe.status === 0) {
    return {
      kind: "system-zstd",
      decode: (buf) =>
        execFileSync("zstd", ["-d", "--stdout", "-q"], {
          input: buf,
          maxBuffer: 256 * 1024 * 1024,
        }),
    };
  }
  return { kind: "none", decode: () => null };
}
const ZSTD = detectZstd();

let seq = 0;

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on("data", (c) => {
      chunks.push(c);
      total += c.length;
    });
    req.on("end", () => resolve(Buffer.concat(chunks, total)));
    req.on("error", reject);
  });
}

function tryDecompress(buf, encoding) {
  if (!DECOMPRESS || !encoding || encoding === "identity") return null;
  try {
    if (encoding === "gzip") return zlib.gunzipSync(buf);
    if (encoding === "deflate") return zlib.inflateSync(buf);
    if (encoding === "br") return zlib.brotliDecompressSync(buf);
    if (encoding === "zstd") {
      if (ZSTD.kind === "none") {
        return {
          err: "no zstd decoder available (need Node >= 22.15 or system `zstd` in PATH)",
        };
      }
      const out = ZSTD.decode(buf);
      if (!out) return { err: `zstd decoder returned empty output (kind=${ZSTD.kind})` };
      return out;
    }
    return { err: `unsupported content-encoding: ${encoding}` };
  } catch (err) {
    return { err: err.message };
  }
}

// Printable ASCII + TAB + CR (newlines are used as the envelope item delimiter
// so they are split on, not included inside a line).
const PRINTABLE_LINE = /^[\x09\x0d\x20-\x7e]*$/;

function previewLine(line) {
  const text = line.toString("utf8");
  if (PRINTABLE_LINE.test(text)) return text;
  const head = line.subarray(0, Math.min(64, line.length)).toString("hex");
  const trail = line.length > 64 ? "..." : "";
  return `<binary: ${line.length} bytes, head=${head}${trail}>`;
}

function preview(buf, n) {
  if (!n || buf.length === 0) return "";
  const slice = buf.subarray(0, Math.min(n, buf.length));
  // Sentry envelopes (and most of what this mock will see) are newline-delimited
  // with possibly-binary item payloads. Classify each line independently so a
  // single binary chunk doesn't turn the whole preview into hex.
  const parts = [];
  let start = 0;
  for (let i = 0; i <= slice.length; i++) {
    if (i === slice.length || slice[i] === 0x0a /* \n */) {
      parts.push(previewLine(slice.subarray(start, i)));
      start = i + 1;
    }
  }
  return parts.join("\n");
}

function sentryHeaders(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase().startsWith("x-sentry-")) out[k] = v;
  }
  return out;
}

const server = http.createServer(async (req, res) => {
  const id = ++seq;
  const t0 = process.hrtime.bigint();
  const ts = new Date().toISOString();

  try {
    const body = await readBody(req);
    const encoding = (req.headers["content-encoding"] ?? "identity").toLowerCase();

    // Match real backend: ack immediately, log afterwards.
    if (req.method === "POST" && req.url === EXPECTED_PATH) {
      res.writeHead(202, { "content-type": "text/plain" });
      res.end("accepted");
    } else {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          error: "not_found",
          hint: `POST ${EXPECTED_PATH} is the only supported route`,
          got: `${req.method} ${req.url}`,
        }) + "\n",
      );
    }

    const dtMs = Number((process.hrtime.bigint() - t0) / 1000n) / 1000;
    const log = {
      seq: id,
      at: ts,
      remote: req.socket.remoteAddress,
      method: req.method,
      path: req.url,
      status: res.statusCode,
      duration_ms: dtMs,
      headers: {
        "content-type": req.headers["content-type"],
        "content-encoding": encoding,
        "content-length": req.headers["content-length"],
        "user-agent": req.headers["user-agent"],
        ...sentryHeaders(req.headers),
      },
      body_bytes: body.length,
    };

    if (PREVIEW_BYTES > 0) {
      const decompressed = tryDecompress(body, encoding);
      if (decompressed && !decompressed.err) {
        log.decompressed_bytes = decompressed.length;
        log.preview = preview(decompressed, PREVIEW_BYTES);
      } else {
        if (decompressed?.err) log.decompress_error = decompressed.err;
        log.raw_preview = preview(body, PREVIEW_BYTES);
      }
    }

    console.log(JSON.stringify(log));
  } catch (err) {
    console.error(JSON.stringify({ seq: id, at: ts, error: err.message }));
    if (!res.headersSent) {
      res.writeHead(500);
      res.end("error");
    }
  }
});

server.on("listening", () => {
  const addr = server.address();
  const url = `http://${addr.address}:${addr.port}${EXPECTED_PATH}`;
  const zstdNote =
    ZSTD.kind === "node-zlib"
      ? "node-zlib (built-in)"
      : ZSTD.kind === "system-zstd"
        ? "system `zstd` binary"
        : "UNAVAILABLE (zstd bodies will show as hex)";
  console.error(
    [
      "mock-fanout-receiver listening",
      `  url           ${url}`,
      `  preview_bytes ${PREVIEW_BYTES} (set PREVIEW_BYTES=NNN to inspect payloads)`,
      `  decompress    ${DECOMPRESS} (set DECOMPRESS=1 to gunzip/brotli/zstd)`,
      `  zstd decoder  ${zstdNote}`,
      "",
      "add to .relay/config.yml once the fork is wired:",
      "  fanout:",
      "    http:",
      "      enabled: true",
      `      url: ${url}`,
      "      timeout_ms: 500",
      "      sample_rate: 1.0",
      "",
      "hit ^C to stop.",
    ].join("\n"),
  );
});

for (const sig of ["SIGINT", "SIGTERM"]) {
  process.on(sig, () => {
    console.error(`\n${sig} received, shutting down`);
    server.close(() => process.exit(0));
  });
}

server.listen(PORT, HOST);
