# Project Configuration

A Relay will generally not accept or forward events where it doesn't know the project. See [_Getting Started_](index.md) for a short introduction to project configs. This page enumerates all possible options.

The configuration for a project with the ID *123* lives in the file
`.semaphore/projects/123.json`. You can get the project ID from the path section
of a DSN.

## Basic Options

`disabled`

: *boolean, default: `false`*

  Whether the project is disabled. If set to `true`, the Relay will drop all
  events sent to this project.

`publicKeys` 

: *array of objects, required*

  A map enumerating known public keys (the public key in a DSN) and whether
  events using that key should be accepted. Example:

  ```json
  {
    "publicKeys": [
        {
            "publicKey": "___PUBLIC_KEY___",
            "isEnabled": true
        }
    ]
  }
  ```

{:.config-key}
## Config

`config.allowedDomains`

: *array of origins (strings), default: `["*"]`*

  Configure origin URLs which Sentry should accept events from. This is
  corresponds to the "Allowed Domains" setting in the Sentry UI.
  
  Note that an empty array will reject all origins. Use the default `["*"]` to
  allow all origins.

`config.piiConfig`

: See [_PII Configuration_](pii-config/index.md).
