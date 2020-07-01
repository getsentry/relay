# Relay Modes

When configuring a Relay server, the most important thing it is to understand the major modes in which it can operate.

As seen in the introduction, the configuration file contains a `relay.mode` field. Currently, there are three modes that can be specified: `managed`, `static`, `proxy`.

Event processing in Sentry can be configured using a two level hierachical configuration. At the top level, there are organization wide settings. An organization contains projects and event processing can be controlled with settings at project level.

From Relay's point of view, events are processed according to the settings of the project it belongs to. Sentry includes all organization settings, such as organization-wide privacy controls, into settings of all projects belonging to your organization. The mode controls the way Relay obtains these project settings.

## `managed` Mode

This is the default mode in which Relay operates. To activate managed mode, set this configuration:

```yaml
relay:
  mode: managed
```

Since configuration is obtained from Sentry, authentication is required in this mode. If authentication fails, no events will be accepted by Relay.

As Relay receives events from applications, it will request project configurations from Sentry in order to process the events. If Sentry is unable to provide the configuration for a particular project, all messages for that project will be discared.

Configuration is refreshed in regular intervals. See [Configuration Options] for ways to configure intervals, timeouts and retries.

## `static` Mode

In this mode, projects must be configured manually. To activate static mode, set this configuration:

```yaml
relay:
  mode: static
```

In static configuration, Relay will process events only for statically configured projects and rejects events for all other projects. This is useful when you know the projects sending events upfront, and you wish to explicitly control the projects that are allowed to send events through this Relay.

To configure projects, add files of format `projects/<PROJECT_ID>.json` to your Relay configuration folder. For a description of the contents of this file, refer to [Project Configuration].

**Note:** In `static` mode, Relay does not register with upstream since it does not query information from it. After processing events for the configured projects, it forwards them to the upstream with the authentication information (DSN) set by the client that sent the original request.

## `proxy` Mode

This mode is similar to `static` mode but it forwards events from unknown projects. To activate proxy mode, set this configuration:

```yaml
relay:
  mode: proxy
```

In `proxy` mode, events for statically configured projects are handled identically to `static` mode. Events for unknown projects -- projects for which there are no statically configured settings -- are forwarded (proxied) with minimal processing.

**Note:** Rate limiting is still being applied in `proxy` mode for all projects regardless of whether they are statically configured or proxied.

[configuration options]: ../options
[project configuration]: ../projects
