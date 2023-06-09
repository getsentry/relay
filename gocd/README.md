# Relay Pipelines

Relay is in the process of moving to a set of rendered jsonnet pipelines.

## Jsonnet

You can render the jsonnet pipelines by running:

```
make gocd
```

This will clean, fmt, lint and generate the GoCD pipelines to
`./gocd/generated-pipelines`.
