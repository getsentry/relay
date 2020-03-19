# Project Configuration

See [_Getting Started_](index.md) for a short introduction to project configs.

The configuration for a project with the ID *123* lives in the file `.relay/projects/123.json`. You can get the project ID from the path section of a DSN.

This page enumerates a few options that may be relevant for running Relay in proxy mode. All other options are currently not stable enough that we feel confident in documenting them.

## Basic Options

### `disabled`

```json
{"disabled": false}
```

Whether the project is disabled. If set to `true`, the Relay will drop all
events sent to this project.

### `publicKeys`

```json
{"publicKeys": [{"publicKey": "deadbeef", "isEnabled": true}]}
```

A map enumerating known public keys (the public key in a DSN) and whether
events using that key should be accepted.

## `config.allowedDomains`

```json
{"config": {"allowedDomains": ["*"]}}
```

Configure origin URLs which Sentry should accept events from. This is corresponds to the "Allowed Domains" setting in the Sentry UI.

Note that an empty array will reject all origins. Use the default `["*"]` to allow all origins.


## `config.piiConfig`

See [_PII Configuration_](pii-config/index.md).
