# Introduction

<p align="center">
  <p align="center">
    <img src="https://github.com/getsentry/relay/blob/master/artwork/relay-logo.png?raw=true" alt="Relay" width="480">
  </p>
</p>

The Sentry Relay is a work in progress service that pushes some functionality
from the Sentry SDKs as well as the Sentry server into a proxy process.

## Getting started

The Relay server is called `relay`. Binaries can be downloaded from the [GitHub
releases page](https://github.com/getsentry/relay/releases). After downloading,
place the binary somewhere on your `PATH` and make it executable.

The `config init` command is provided to initialize the initial config. The
config will be created the folder it's run from in a hidden `.relay`
subdirectory:

    $ relay config init

The wizard will ask a few questions:

1. default vs custom config. In the default config the relay will connect to
   sentry.io for sending and will generally use the defaults. In the custom
   config a different upstream can be configured.
2. Relay internal crash reporting can be enabled or disabled. When enabled the
   relay will report its own internal errors to sentry.io to help us debug it.
3. Lastly the relay will ask it should run authenticated with credentials or
   not. Currently we do not yet support authenticated mode against sentry.io.

You now have a folder named `.relay` in your current working directory. To
launch the server, run:

    $ relay run

If you moved your config folder somewhere else, you can use the `--config` option:

    $ relay run --config ./my/custom/relay_folder/

### Running in Docker

Docker image for `relay` can be found at `us.gcr.io/sentryio/relay`.

For example, you can start the latest version of `relay` as follows:

```sh
docker run -v $(pwd)/configs/:/etc/relay/ us.gcr.io/sentryio/relay run --config /etc/relay
```

The command assumes that Relay's configuration (`config.yml` and
`credentials.json`) are stored in `./configs/` directory on the host machine.

## Upstream Registration

When Relay runs, it registers with the upstream configured Sentry instance. Each
Relay is identified by the `(relay_id, public_key)` tuple upstream. Multiple
Relays can share the same public key if they run with different Relay IDs.

At present, Sentry requires Relays to be explicitly whitelisted by their public
key. This is done through the `SENTRY_RELAY_WHITELIST_PK` config key which is a
list of permitted public keys.

## Metrics and Crash Reporting

By default, Relay currently reports directly to sentry.io. This can be disabled
by setting the `sentry.enabled` key to `false`. Additionally a different DSN can
be supplied with `sentry.dsn`. Crash reporting will become opt-in before the
initial public release.

Stats can be submitted to a statsd server by configuring `metrics.statsd` key.
It can be put to a `ip:port` tuple. Additionally `metrics.prefix` can be
configured to have a different prefix (the default is `sentry.relay`). This
prefix is added in front of all metrics.

## Setting up a Project

Right now Relay is only really usable in "simple proxy mode" (without
credentials), and as such calls the same exact endpoints on Sentry that an SDK
would. That also means you have to configure each project individually in Relay.

Create a new file in the form `project_id.json`:

```
.relay/projects/___PROJECT_ID___.json
```

With the following content:

```json
{
  "publicKeys": [
    {
      "publicKey": "___PUBLIC_KEY___",
      "isEnabled": true
    }
  ],
  "config": {
    "allowedDomains": ["*"],
    "piiConfig": {}
  }
}
```

The relay has to know all public keys (i.e. the secret part of the DSN) that
will send events to it. DSNs unknown for this project will be rejected.

## Sending a Test Event

Launch the server with `relay run`, and set up any SDK with the following DSN:

```
http://___PUBLIC_KEY___@127.0.0.1:3000/___PROJECT_ID___
```

As you can see we only changed the host and port of the DSN to point to your
Relay setup. You should be able to use the SDK normally at this point. Events
arrive at the Sentry instance that Relay is configured to use in
`.relay/config.yml`.

## PII Stripping

Now let's get to the entire point of this proxy setup: Stripping sensitive data.

The easiest way to go about this is if you already have a raw JSON payload from
some SDK. Go to our PII config editor
[Piinguin](https://getsentry.github.io/piinguin/), and:

1. Paste in a raw event
2. Click on data you want eliminated
3. Paste in other payloads and see if they look fine, go to step **2** if
   necessary.

After iterating on the config, paste it back into the project config you created
earlier:

```
.relay/projects/___PROJECT_ID___.json
```

For example:

```json
{
  "publicKeys": [
    {
      "publicKey": "___PUBLIC_KEY___",
      "isEnabled": true
    }
  ],
  "config": {
    "allowedDomains": ["*"],
    "piiConfig": {
      "rules": {
        "device_id": {
          "type": "pattern",
          "pattern": "d/[a-f0-9]{12}",
          "redaction": {
            "method": "hash"
          }
        }
      },
      "applications": {
        "freeform": ["device_id"]
      }
    }
  }
}
```
