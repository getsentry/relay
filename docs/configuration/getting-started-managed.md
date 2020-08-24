# Getting Started (`managed` mode)

**Note: presently `managed` mode is only supported for on-premise installations. If you are
using Relay to connect to `sentry.io`  and want to use `managed` mode contact us.**

In this section we will discuss the specifics of getting started with Relay in `managed` mode.
It is assumed that you already went through _[getting started]_ and here we will only discuss
the relevant differences for using `managed` mode. Everything except project configuration from
_[getting started]_ is relevant for `managed` mode.

### Initializing Configuration

For managed mode you can select the default configuration or if using a custom configuration select
"Managed through upstream".

### Credentials

*Not applicable in `proxy`/`static` mode.*

Besides `config.yml`, the `init` command has also created a credentials file `credentials.json` in the same `.relay` directory. This file contains the a public and private key used by Relay to authenticate with the upstream server.

**As such, it is important that this file should be adequatly protected from modification or viewing by unauthorized entities**.

Here's an example of the contents of a typical credentials file:

```json
{
  "secret_key": "5gkTAfwOrJ0lMy9aOAOmHKO1k6gd8ApYkAInmg5VfWk",
  "public_key": "fQzvlvqLM2pJwLDwM_sXD2Lk5swzx-Oml4WhsOquon4",
  "id": "cde0d72e-0c4e-4550-a934-c1867d8a177c"
}
```

You will be using the `public_key` to register your Relay with the upstream server when running it in `managed` mode.

### Registering Relay with Sentry

*Not applicable in `proxy`/`static` mode.*

To operate in `managed` mode, Relay pulls configuration for PII stripping,
filtering, and rate limiting from your organization and project settings at
Sentry. Since these settings may contain sensitive information, their access is
restricted by Sentry and requires authorization.

In order to register Relay with Sentry, get the contents of the public key,
either by inspecting the `credentials.json` file or by running:

```sh
❯ ./relay credentials show
Credentials:
  relay id: 8cd24a0e-384d-4052-9010-68a21392b33c
  public key: nDJl79SbEYH9-8NEJAI7ezrgYfolPW3Bnkg00k1zOfA
```

After copying the public key, go to the organization settings in Sentry by clicking on _Settings_ in the main navigation on the left, then go to _Relays_.

<p align="center">
    <img src="../../img/add-relay-key.png" alt="Relays Settings" />
</p>

Click _New Relay Key_ to add the key and save it:

<p align="center">
    <img src="../../img/edit-relay-key.png" alt="Add Relay Key" >
</p>

Now your Relay is registered with Sentry and ready to send messages. See
_[Configuration Options]_ to learn more about further Relay configuration
options.

### Running Relay

Now you are ready to start relay follow the [running relay] section from the [getting started]

[getting started]: ../../
[running relay]: ../../#running-relay
