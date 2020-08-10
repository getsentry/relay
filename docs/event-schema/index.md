# Event schema

This page is intended to eventually replace [our current event schema documentation]. As opposed to the current one, this one is automatically generated from source code and therefore more likely to be up-to-date and exhaustive. The plan is to eventually make this document the source of truth, i.e. move it into `develop.sentry.dev`.

**It is still a work-in-progress.** Right now we recommend using our existing docs as linked above and only fall back to this doc to resolve ambiguities.

In addition to documentation the event schema is documented in machine-readable form:

- [Download JSON schema](event.schema.json) (which is what this document is generated from)

{% include "event-schema/event.schema.md" %}


[our current event schema documentation]: https://develop.sentry.dev/sdk/event-payloads/
