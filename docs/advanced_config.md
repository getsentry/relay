# Advanced Configuration

When configuring a Relay server the most important thing it is to understand the major modes in which it can operate.

As seen in the introduction in the `config.yml` file under the `relay` key there is a `mode` field. Currently there are four modes that can be specified `managed`, `static`, `proxy` and `capture`. The capture mode is meant for testing and will not be discussed in this document.

Event processing in Sentry can be configured using a two level hierachical configuration. At the top level there are organization wide settings. An organization  contains projects and event processing can be controlled with settings at project level . 

From Relay's point of view events are processed according to the settings of the project it belongs to. It is the responsability of the  upstream (Sentry) to merge the organization level settings into the project level settings and offer the equivalent project settings to Relay.

Relay can obtain project settings in a few ways and the mode in which it obtains the settings is controlled by the `mode` field.

## Relay mode

### `managed`

This is the default mode in which Relay operates. In `managed` mode Relay will first authenticate with upstream and then, as it receives events from applications, it will request project configurations from upstream in order to process the events. If the upstream is unable to provide the configuration for a particular project, all messages for that project will be discared.

### `static`

In this mode projects need to be configured manually. In static configuration Relay will process events for projects that have been configured (see below for static project configuration) and will reject events for all other projects. **Note:** that in `static` mode Relay does not register with upstream (since it does not need any information from it) and, after processing events for the configured projects it forwards them to the upstream with the authentication information (DSN) set by the application (or SDK) that sent the original request.

### `proxy`

This mode is similar with `static` mode but it handles differently events from projects that are not statically configured. 

In `proxy` mode events for statically configured projects are handled identically with how they are handled in `static` mode. 

Events for unknown projects (projects for which there are no statically configured settings) are just forwarded (proxied) to upstream with minimal processing (event is normalized). **Note:** Rate limiting is still being applied in `proxy` mode for all projects regardless if they are statically configured or proxied.

## Processing

Relays can be processing and non-processing. In order for a Relay to be processing it needs to use a binary compiled with the `processing` feature on and to have processing enabled in configuration via: `processing.enabled:true` flag.

Only the last Relay in a chain can be processing so unless you are using an onpremise installation and do not connect to Sentry's infrastructure all your Relay must be non processing Relays.

Non processing relays can do PII strpping , rate limitting and... (TODO RaduW enumerate every thing that can be done in a non-processing Relay )

For technical reasons at the moment filtering can only be done by processing Relays.

## Setting up static Project configurations

When running a Relay in `static` or `proxy` mode Relay obtains the project configurations from the file system.

Static project configurations are found under the `projects` subdirectory of the Relay configuration directory, by default this is the `.relay/projects` sub-directory  under the directory where the `relay` binary is installed (see [Introduction](./..) for how to configure a different location).

For each project that a Relay needs to handle a file with the name `__<PROJECT_ID>__.json` needs to be added to the projects subdirectory (e.g.  if  `project_id== 23`  the project configuration file should be `__23__.json`.

Set up a project configuration like in the example below

```json
{
  "publicKeys": [
    {
      "publicKey": "<SECRET_DSN>",
      "isEnabled": true
    }
  ],
  "config": {
    "allowedDomains": ["*"],
    "piiConfig": {}
  }
}
```

Note that the publicKey (<SECRET_DSN>) is the project DSN key and it has nothing to do with the Relay public key that is used for Relay registration.

One can obtain the key by going into the Sentry project config ui and select Client Keys (DSN). The public key can be extracted from the DSN. In the DSN example below:

```
https://12345abcdb1e4c123490ecec89c1f199@o1.ingest.sentry.io/2244 
```

the key is:

```
12345abcdb1e4c123490ecec89c1f199
```

A project may contain multiple public keys, only messages using enabled project keys will be processed.



## Metrics and Crash Reporting

By default, Relay currently reports directly to sentry.io. This can be disabled
by setting the `sentry.enabled` key to `false`. Additionally a different DSN can
be supplied with `sentry.dsn`. Crash reporting will become opt-in before the
initial public release.

Stats can be submitted to a statsd server by configuring `metrics.statsd` key.
It can be put to a `ip:port` tuple. Additionally `metrics.prefix` can be
configured to have a different prefix (the default is `sentry.relay`). This
prefix is added in front of all metrics.

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
