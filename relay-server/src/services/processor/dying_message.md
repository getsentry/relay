# Dying message

We're capturing additional runtime information and this gets passed to Sentry as an attachment `dying_message.dat` by a crash handler.

The size is currently limited to 4096 bytes on the client so the format of the message is designed to be compact and allow compression inside the message itself (regardless of transport compression), in order to fit as much data as possible in the limited space.

> [!IMPORTANT]
> It's important to keep this protocol documentation and its implementation up-to-date between [sentry-switch SDK](https://github.com/getsentry/sentry-switch) and [Relay](https://github.com/getsentry/relay).

## Dying message file format

```text
4 bytes - Magic number (0x736E7472 = 'sntr')
1 byte - Format version (0-255)
n bytes - Version-specific data
```

### v0 file format

The version-specific data is as follows:

```text
1 byte - Payload encoding
2 bytes - Payload size, big-endian notation (before decompression if compressed) (0-65535)
n bytes - Payload
```

The payload encoding is stored as a single byte but stores multiple components by splitting bits to groups:

```text
2 bits (uint2) - format (after decompression), possible values:
- `0` = envelope items without envelope header
2 bits (uint2) - compression algorithm, possible values:
- `0` = none
- `1` = Zstandard
4 bits (uint4) - compression-algorithm-specific argument, e.g. dictionary identifier
```

The payload consists of envelope-items, as specified in the
[Envelope data format](https://develop.sentry.dev/sdk/data-model/envelopes/#serialization-format).
However, there is no envelope header as that is irrelevant in the context of DyingMessage.

Additionally, the [Event envelope item type](https://develop.sentry.dev/sdk/data-model/event-payloads/) is considered
a "patch" over the actual event envelope item in the "parent" envelope (the one that the DyingMessage is attached to).
This will result in an update of the parent envelope's event item with the data from the DyingMessage's event item.

Any other item is attached to the parent envelope.

#### ZStandard compression

In case the payload is compressed with Zstandard, it is configured to omit the following in the compressed data:

- ZStandard magic number
- Dictionary ID

#### Examples

##### Single-item with JSON payload

The following 25 bytes long example message contains a single uncompressed envelope item:

```text
{"type":"event"}\n
{"foo":"bar","level":"info","map":{"b":"c"}}\n
```

Hexadecimal representation:

```text
736E7472 00 00 0010 7B22...
```

```text
| magic number         | 0x736E7472 | sntr             |
| format version       | 0x00       | v0               |
| payload format       | 0b00       | envelope items   |
| payload compression  | 0b00       | uncompressed     |
| compression argument | 0b0000     | n/a              |
| payload size         | 0x003E     | 62 bytes         |
| payload data         | 0x7B...    | ...              |
```
