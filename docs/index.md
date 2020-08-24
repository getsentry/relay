# Introduction

<p align="center">
  <p align="center">
    <img src="https://github.com/getsentry/relay/blob/master/artwork/relay-logo.png?raw=true" alt="Relay" width="480">
  </p>
</p>

Sentry Relay is a standalone service that allows you to scrub personal
information and improve event response time. It acts as a middle layer between
your application and Sentry.io.

**Note: Relay is still work in progress. The default Relay mode is at the moment only supported 
for on-premises installations, if you are using Relay to connect to Sentry you need to use
`proxy` or `static` mode. If you want to beta test the `managed` mode while connecting to 
 Sentry please get in contact with us. See _[Relay Modes]_ for more information.**

## Use Cases for Relay

Relay was designed with a few usage scenarios in mind.

### Scrubbing Personally Identifiable Information (PII)

Sentry allows to scrub PII in two places: in the SDK before sending the event,
and upon arrival on Sentry's infrastructure. Relay adds a third option that
allows to scrub data in a central place before sending it to Sentry.

To choose the right place for data scrubbing, consider:

- If you prefer to configure data scrubbing in a central place, you can let
  Sentry handle data scrubbing. Upon arrival, Sentry immediately applies
  [server-side scrubbing] and guaratees that personal information is never
  stored.

- If you cannot send PII outside your infrastructure but you still prefer to
  configure data scrubbing in one centralized place, run Relay and configure
  your SDKs to send events there. Relay uses the privacy settings configured in
  Sentry, and scrubs PII before forwarding data to Sentry.

- For the most strict data privacy requirements, you can configure SDKs to scrub
  PII using the [before_send hooks], which prevents data from being collected
  on the device. This may require you to replicate the same logic across your
  applications and could lead to a performance impact.

### Improved Response Time

Relay is designed to respond very quickly to requests. Having Relay installed
close to your infrastructure will further improve the response when sending
events.

This can particularly reduce the roundtrip time in remote locations.

### Enterprise Domain Name

By default, SDKs need to be configured with a DSN that points to `sentry.io`. If
you need to restrict all HTTP communication to a custom domain name, Relay can
act as an opaque proxy that reliably forwards events to Sentry.

# Getting Started

In this section we will create a simple setup using the default settings. Check
the _[Configuration Options]_ page for a detail discussion
of various operating scenarious for Relay.

The Relay server is called `relay`. Binaries can be downloaded from [GitHub
Releases] and a Docker image is provided on [DockerHub].

### Initializing Configuration

In order to create the initial configuration, Relay provides the `relay config
init` command. The command puts configuration files in the `.relay` folder
under the current working directory:

We will be creating a custom configuration since the default configuration creates a 
`managed` mode configuration which is not supported by sentry at the current time.

```sh
❯ ./relay config init
Initializing relay in /<current_directory>/.relay
Do you want to create a new config?:
  Yes, create default config
> Yes, create custom config
  No, abort
```

After selecting custom config you will be able to customize the following basic parameters:

- The `mode` setting configures the major mode in which Relay operates. For more
  information on available relay modes, refer to _[Relay Modes]_.

  **Right now, the only options supported by `sentry.io` are `proxy` and `static` mode.**

- The `upstream` setting configures the server to which Relay will forward the
  events (by default the main `sentry.io` URL).

- The `port` and `host` settings configure the TCP port at which Relay will
  listen to. This is the address to which SDKs send events.

- The `tls` settings configure TLS support (HTTPS support), used for
  the cases where the communication between the SDK and Relay needs to be
  secured.


In our example we will be going with "Statically configured" relay with the default
options.

After going though the questions you should see something like this:

```sh
❯ ./relay config init
Initializing relay in /Users/some_user/relay_root/.relay
Do you want to create a new config?: Yes, create custom config
How should this relay operate?: Statically configured
upstream: https://sentry.io/
listen interface: 127.0.0.1
listen port: 3000
do you want listen to TLS no
All done!

```

Settings are placed in `.relay/config.yml`. Note that all configuration values are optional.
After using the options above the config file looks like below: 

```yaml
---
relay:
  mode: static
  upstream: "https://sentry.io/"
  host: 127.0.0.1
  port: 3000
  tls_port: ~
  tls_identity_path: ~
  tls_identity_password: ~
```

All configurations are explained in detail in the section [Configuration
Options].

### Configuring projects

Before running Relay we need to configure at least one project that belongs to our organization.
Please see: _[projects]_ for a detail expalantion of project configuration.

Create the following configuration using your own project id ( in the example a project with id `1234` was used).

```
.relay/
└── projects/
    └── 1234.json
```

The project file should look like the following ( replace the DSN and the 'slug' with your project DSN and
name, both found in project configuration).

```json
{
  "slug": "<project_name>",
  "publicKeys": [
    {
      "publicKey": "<DSN_KEY>",
      "isEnabled": true
    }
  ],
  "config": {
    "allowedDomains": ["*"]
  }
}
```


## Running Relay

Now it is time to start your relay.
You should be seeing something like:

```sh
❯ ./relay run
 INFO  relay::setup > launching relay from config folder /Users/some_user/relay_root/.relay
 INFO  relay::setup >   relay mode: static
 INFO  relay::setup >   relay id: -
 INFO  relay::setup >   public key: -
 INFO  relay::setup >   log level: INFO
 INFO  relay_server::actors::upstream > upstream relay started
 INFO  relay_server::actors::events   > starting 12 event processing workers
 INFO  relay_server::service          > spawning http server
 INFO  relay_server::service          >   listening on: http://127.0.0.1:3000/
 INFO  actix_net::server::server      > Starting 12 workers
 INFO  actix_net::server::server      > Starting server on 127.0.0.1:3000
 INFO  relay_server::actors::controller > relay server starting
 INFO  relay_server::actors::connector  > metered connector started
 INFO  relay_server::actors::events     > event manager started
 INFO  relay_server::actors::project_local > project local cache started
 INFO  relay_server::actors::project_upstream > project upstream cache started
 INFO  relay_server::actors::project_cache    > project cache started
 INFO  relay_server::actors::project_keys     > project cache started
 INFO  relay_server::actors::relays           > key cache started
```

If you moved your config folder somewhere else (e.g. for security reasons), you 
can use the `--config` option to specify the location:

```sh
❯ relay run --config ./my/custom/relay_folder/
```

### Running in Docker

As an alternative to directly running the Relay binary, Sentry also provides 
a Docker image that can be used to run Relay. It can be found on [DockerHub].

Similar to directly running the `relay` binary, running the docker image needs a
directory in which it can find the configuration files.

Providing the configuration directory can be done with the standard mechanisms 
offered by docker, either by mounting [docker volumes] or by building a new container 
and copying the files in.

For example, you can start the latest version of `relay` as follows:

```sh
❯ docker run -v $(pwd)/configs/:/work/.relay/ getsentry/relay run
```

This example command assumes that Relay's configuration (`config.yml` ) is stored in `./configs/` 
directory on the host machine.


## Logging and healthcheck

Now you have a running relay, you might have noticed that relay displays some `INFO` messages,
including:
```sh
INFO  relay::setup >   log level: INFO
```

This is the default logging level you can change this to show more or less info. 
For details about configuring logging please see _[logging]_ on the options page.

Relay provides two urls for health check and monitoring the live status:

In our example relay is running at "http://localhost:3000" so the following urls are
going to return a status check JSON file.

`GET http://localhost:3000/api/relay/healthcheck/ready/`
`GET http://localhost:3000/api/relay/healthcheck/live/`

will return:

```json
{
"is_healthy": true
}
```

### Sending a Test Event

Once Relay is running and authenticated with Sentry, it is time to send a test event.

Get the DSN of your project by navigating to your _Project Settings > Client Keys (DSN)_. From the _Client Keys_ page, get the DSN, which looks something like:

```
https://12345abcdb1e4c123490ecec89c1f199@o1.ingest.sentry.io/2244
```

Next, replace parts of the DSN to match the address at which Relay is reachable. For instance, if Relay listens at `http://localhost:3000`, change the protocol and host of the DSN to:

```
http://12345abcdb1e4c123490ecec89c1f199@localhost:3000/2244
```

Use the new DSN in your SDK configuration. To test this, you can send a message with `sentry-cli`:

```sh
❯ export SENTRY_DSN='http://12345abcdb1e4c123490ecec89c1f199@127.0.0.1:3000/2244'
❯ sentry-cli send-event -m 'A test event'
```

After a few seconds, the event should appear in the issues stream in your
project.

[before_send hooks]: https://docs.sentry.io/error-reporting/configuration/?platform=rust#before-send
[server-side scrubbing]: https://docs.sentry.io/data-management/sensitive-data/#server-side-scrubbing
[github releases]: https://github.com/getsentry/relay/releases
[configuration options]: ./configuration/options
[relay modes]: ./configuration/modes
[dockerhub]: https://hub.docker.com/r/getsentry/relay/
[docker volumes]: https://docs.docker.com/storage/volumes/
[projects]: ./configuration/projects
[logging]: ./configuration/options/#logging
