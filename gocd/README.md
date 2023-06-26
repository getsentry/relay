# Relay Pipelines

Relay is in the process of moving to a set of rendered jsonnet pipelines.

## Jsonnet

You can render the jsonnet pipelines by running:

```
make gocd
```

This will clean, fmt, lint and generate the GoCD pipelines to
`./gocd/generated-pipelines`.


The Relay pipelines are using the https://github.com/getsentry/gocd-jsonnet
libraries to generate the pipeline for each region.
