# Path of an Event through Relay

## Processing enabled vs not?

Relay can run as part of a Sentry installation, such as within `sentry.io`'s
infrastructure, or next to the application as a forwarding proxy. A lot of
steps described here are skipped or run in a limited form when Relay is *not*
running in processing mode:

*  Event normalization does different (less) things.

*  In `static` mode, project config is read from disk instead of fetched from
   an HTTP endpoint. In `proxy` mode, project config is just filled out with
   defaults.

*  Events are forwarded to an HTTP endpoint instead of being written to Kafka.

*  Rate limits are not calculated using Redis, instead Relay just honors 429s
   from previously mentioned endpoint.

*  Filters are not applied at all.

## Inside the endpoint

When an SDK hits `/api/X/store` on Relay, the code in
`server/src/endpoints/store.rs` is called before returning a HTTP response.

That code looks into an in-memory cache to answer basic information about a project such as:

*  Does it exist? Is it suspended/disabled?

*  Is it rate limited right now? If so, which key is rate limited?

*  Which DSNs are valid for this project?

Some of the data for this cache is coming from the [projectconfigs
endpoint](https://github.com/getsentry/sentry/blob/c868def30e013177383f8ca5909090c8bdbd8f6f/src/sentry/api/endpoints/relay_projectconfigs.py).
It is refreshed every couple of minutes, depending on configuration.

If the cache is fresh, we may return a `429` for rate limits or a `4xx` for
invalid auth information.

That cache might be empty or stale. If that is the case, Relay does not
actually attempt to populate it at this stage. **It just returns a `200` even
though the event might be dropped later.** This implies:

*  The first store request that runs into a rate limit doesn't actually result
   in a `429`, but a subsequent request will (because by that time the project
   cache will have been updated).

*  A store request for a non-existent project may result in a `200`, but
   subsequent ones will not.

*  A store request with wrong auth information may result in a `200`, but
   subsequent ones will not.

*  Filters are also not applied at this stage, so **a filtered event will
   always result in a `200`**. This matches the Python behavior since [a while
   now](https://github.com/getsentry/sentry/pull/14561).

These examples assume that a project receives one event at a time. In practice
one may observe that a highly concurrent burst of store requests for a single
project results in `200 OK`s only. However, a multi-second flood of incoming
events should quickly result in eventually consistent and correct status codes.

The response is completed at this point. All expensive work (such as talking to
external services) is deferred into a background task. Except for responding to
the HTTP request, there's no I/O done in the endpoint in any form. We didn't
even hit Redis to calculate rate limits.

The fast response times are supposed to benefit application environments which
cannot send HTTP requests fully asynchronously (PHP and certain serverless
platorms), at the cost of status codes being inaccurate. Note that we still
emit accurate outcomes into Kafka if configured to do so.

## The background task

The HTTP response is out, with a status code that is just a best-effort guess
at what the actual outcome of the event is going to be. The rest of what used
to happen synchronously in the Python `StoreView` is done asynchronously, but
still in the same process.

So, now to the real work:

1.  **Project config is fetched.** If the project cache is stale or
    missing, we fetch it. We may wait a couple milliseconds here to be able to
    batch multiple project config fetches into the same HTTP request to not
    overload Sentry too much.

    At this stage Relay may drop the event because it realized that the DSN was
    invalid or the project didn't even exist. The next incoming event will get a
    proper 4xx status code.

1.  **The event is parsed.** In the endpoint we only did decompression, a basic
    JSON syntax check, and extraction of the event ID to be able to return it as
    part of the response.

    Now we create an `Event` struct, which conceptually is the equivalent to
    parsing it into a Python dictionary: We allocate more memory.

1.  **The event is datascrubbed.** We have a PII config (new format) and a
    datascrubbing config (old format, converted to new format on the fly) as
    part of the project config fetched from Sentry.

1.  **The event is normalized.** Event normalization is probably the most
    CPU-intensive task running in Relay. It discards invalid data, moves data
    from deprecated fields to newer fields and generally just does schema
    validation.

1.  **Filters ("inbound filters") are applied.** Event may be discarded because of IP
    addresses, patterns on the error message or known web crawlers.

1.  **Exact rate limits ("quotas") are applied.** `is_rate_limited.lua` is
    executed on Redis. The input parameters for `is_rate_limited.lua` ("quota
    objects") are part of the project config. See [this pull
    request](https://github.com/getsentry/sentry/pull/14558) for an explanation
    of what quota objects are.

    The event may be discarded here. If so, we write the rate limit info
    (reason and expiration timestamp) into the in-memory project cache so that
    the next store request returns a 429 in the endpoint and doesn't hit Redis
    at all.

    This contraption has the advantage that suspended or permanently
    rate-limited projects are very cheap to handle, and do not involve external
    services (ignoring the polling of the project config every couple of
    minutes).

1.  **Event is written to Kafka.**

**Note:** If we discard an event at any point, an outcome is written to Kafka
if Relay is configured to do so.


## The outcomes consumer

Outcomes are small messages in Kafka that contain an event ID and information
about whether that event was rejected, and if so, why.

The outcomes consumer is mostly responsible for updating (user-visible)
counters in Sentry (buffers/counters and tsdb, which are two separate systems).

## The ingest consumer

The ingest consumer reads accepted events from Kafka, and also updates some
stats. Some of *those* stats are billing-relevant.

Its main purpose is to do what `insert_data_to_database` in Python store did:
Call `preprocess_event`, after which comes sourcemap processing, native
symbolication, grouping, snuba and all that other stuff that is of no concern
to Relay.
