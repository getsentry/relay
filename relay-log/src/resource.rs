//! Attaches OpenTelemetry resource attributes to transactions sent to Sentry.
//!
//! The attributes are read from the standard `OTEL_RESOURCE_ATTRIBUTES` environment variable,
//! for example `k8s.pod.name=relay-abc,cloud.provider=gcp`, and added as data to the trace
//! context and every span of each transaction. Existing data is never overwritten.

use std::collections::BTreeMap;
use std::sync::Arc;

use sentry::protocol::{Context, EnvelopeItem, Transaction, Value};
use sentry::transports::DefaultTransportFactory;
use sentry::{Envelope, Transport, TransportFactory, TransportOptions};

/// The standard OpenTelemetry environment variable for resource attributes.
const ENV_VAR: &str = "OTEL_RESOURCE_ATTRIBUTES";

/// Resource attributes describing where this Relay instance runs.
pub type ResourceAttributes = BTreeMap<String, String>;

/// Reads resource attributes from `OTEL_RESOURCE_ATTRIBUTES`.
pub fn from_env() -> ResourceAttributes {
    std::env::var(ENV_VAR)
        .map(|value| parse(&value))
        .unwrap_or_default()
}

/// Parses a comma-separated list of `key=value` pairs.
///
/// Keys and values are trimmed and values are percent-decoded. Malformed entries are skipped.
fn parse(value: &str) -> ResourceAttributes {
    value
        .split(',')
        .filter_map(|entry| {
            let (key, value) = entry.split_once('=')?;
            let key = key.trim();
            let value = percent_decode(value.trim())?;
            (!key.is_empty() && !value.is_empty()).then(|| (key.to_owned(), value))
        })
        .collect()
}

/// Decodes `%XX` escapes. Returns `None` for invalid escapes or non-UTF-8 results.
fn percent_decode(value: &str) -> Option<String> {
    let mut bytes = value.bytes();
    let mut decoded = Vec::with_capacity(value.len());

    while let Some(byte) = bytes.next() {
        if byte == b'%' {
            let hex = [bytes.next()?, bytes.next()?];
            let hex = std::str::from_utf8(&hex).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
        } else {
            decoded.push(byte);
        }
    }

    String::from_utf8(decoded).ok()
}

/// Adds the attributes to the trace context and all spans of the transaction.
fn apply(attributes: &ResourceAttributes, transaction: &mut Transaction<'static>) {
    let trace_data = transaction
        .contexts
        .get_mut("trace")
        .and_then(|context| match context {
            Context::Trace(trace) => Some(&mut trace.data),
            _ => None,
        });

    let span_data = transaction.spans.iter_mut().map(|span| &mut span.data);

    for data in trace_data.into_iter().chain(span_data) {
        for (key, value) in attributes {
            data.entry(key.clone())
                .or_insert_with(|| Value::String(value.clone()));
        }
    }
}

/// A transport factory that wraps the default transport and enriches transactions.
pub struct ResourceTransportFactory {
    attributes: Arc<ResourceAttributes>,
}

impl ResourceTransportFactory {
    /// Creates a factory that adds the given attributes to all transactions.
    pub fn new(attributes: ResourceAttributes) -> Self {
        Self {
            attributes: Arc::new(attributes),
        }
    }
}

impl TransportFactory for ResourceTransportFactory {
    fn create_transport_with_options(&self, options: TransportOptions) -> Arc<dyn Transport> {
        Arc::new(ResourceTransport {
            attributes: self.attributes.clone(),
            inner: DefaultTransportFactory.create_transport_with_options(options),
        })
    }
}

struct ResourceTransport {
    attributes: Arc<ResourceAttributes>,
    inner: Arc<dyn Transport>,
}

impl Transport for ResourceTransport {
    fn send_envelope(&self, envelope: Envelope) {
        let has_transaction = envelope
            .items()
            .any(|item| matches!(item, EnvelopeItem::Transaction(_)));

        if !has_transaction {
            return self.inner.send_envelope(envelope);
        }

        let mut enriched = Envelope::new().with_headers(envelope.headers().clone());
        for mut item in envelope.into_items() {
            if let EnvelopeItem::Transaction(ref mut transaction) = item {
                apply(&self.attributes, transaction);
            }
            enriched.add_item(item);
        }

        self.inner.send_envelope(enriched);
    }

    fn flush(&self, timeout: std::time::Duration) -> bool {
        self.inner.flush(timeout)
    }

    fn shutdown(&self, timeout: std::time::Duration) -> bool {
        self.inner.shutdown(timeout)
    }
}

#[cfg(test)]
mod tests {
    use sentry::protocol::Span;

    use super::*;

    fn attrs(pairs: &[(&str, &str)]) -> ResourceAttributes {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_parse() {
        let parsed = parse(
            " k8s.pod.name = relay-abc ,k8s.namespace.name=relay,\
             cloud.account.id=internal-sentry",
        );
        assert_eq!(
            parsed,
            attrs(&[
                ("cloud.account.id", "internal-sentry"),
                ("k8s.namespace.name", "relay"),
                ("k8s.pod.name", "relay-abc"),
            ])
        );
    }

    #[test]
    fn test_parse_percent_encoded() {
        assert_eq!(
            parse("service.name=my%20relay%2Cx"),
            attrs(&[("service.name", "my relay,x")])
        );
    }

    #[test]
    fn test_parse_malformed() {
        assert_eq!(parse(""), attrs(&[]));
        assert_eq!(
            parse("novalue,=nokey,empty=,bad=%zz,short=%2,k8s.node.name=gke-n1,,"),
            attrs(&[("k8s.node.name", "gke-n1")])
        );
    }

    #[test]
    fn test_apply() {
        let mut existing = sentry::protocol::Map::new();
        existing.insert("k8s.pod.name".to_owned(), "from-span".into());

        let mut transaction = Transaction {
            spans: vec![
                Span::default(),
                Span {
                    data: existing,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        transaction
            .contexts
            .insert("trace".to_owned(), Context::Trace(Box::default()));

        apply(
            &attrs(&[("k8s.pod.name", "relay-abc"), ("cloud.provider", "gcp")]),
            &mut transaction,
        );

        let Some(Context::Trace(trace)) = transaction.contexts.get("trace") else {
            panic!("missing trace context");
        };
        assert_eq!(trace.data["k8s.pod.name"], "relay-abc");
        assert_eq!(trace.data["cloud.provider"], "gcp");
        assert_eq!(transaction.spans[0].data["k8s.pod.name"], "relay-abc");
        assert_eq!(transaction.spans[1].data["k8s.pod.name"], "from-span");
        assert_eq!(transaction.spans[1].data["cloud.provider"], "gcp");
    }

    #[derive(Default)]
    struct CaptureTransport(std::sync::Mutex<Vec<Envelope>>);

    impl Transport for CaptureTransport {
        fn send_envelope(&self, envelope: Envelope) {
            self.0.lock().unwrap().push(envelope);
        }
    }

    #[test]
    fn test_transport_enriches_transactions() {
        let inner = Arc::new(CaptureTransport::default());
        let transport = ResourceTransport {
            attributes: Arc::new(attrs(&[("k8s.pod.name", "relay-abc")])),
            inner: inner.clone(),
        };

        let mut envelope = Envelope::new();
        envelope.add_item(Transaction {
            spans: vec![Span::default()],
            ..Default::default()
        });
        transport.send_envelope(envelope);

        let sent = inner.0.lock().unwrap();
        let Some(EnvelopeItem::Transaction(transaction)) = sent[0].items().next() else {
            panic!("missing transaction");
        };
        assert_eq!(transaction.spans[0].data["k8s.pod.name"], "relay-abc");
    }
}
