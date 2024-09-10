use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// OpenTelemetry Context
///
/// If an event has this context, it was generated from an OpenTelemetry signal (trace, metric, log).
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct OtelContext {
    /// Attributes of the OpenTelemetry span that maps to a Sentry event.
    ///
    /// <https://github.com/open-telemetry/opentelemetry-proto/blob/724e427879e3d2bae2edc0218fff06e37b9eb46e/opentelemetry/proto/trace/v1/trace.proto#L174-L186>
    #[metastructure(pii = "maybe", max_depth = 7, max_bytes = 8192)]
    attributes: Annotated<Object<Value>>,

    /// Information about an OpenTelemetry resource.
    ///
    /// <https://github.com/open-telemetry/opentelemetry-proto/blob/724e427879e3d2bae2edc0218fff06e37b9eb46e/opentelemetry/proto/resource/v1/resource.proto>
    #[metastructure(pii = "maybe", max_depth = 7, max_bytes = 8192)]
    resource: Annotated<Object<Value>>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for OtelContext {
    fn default_key() -> &'static str {
        "otel"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::Otel(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::Otel(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::Otel(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::Otel(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    fn test_otel_context_roundtrip() {
        let json = r#"{
  "attributes": {
    "app.payment.amount": 394.25,
    "rpc.grpc.status_code": "1",
    "rpc.method": "Charge",
    "rpc.service": "hipstershop.PaymentService",
    "rpc.system": "grpc"
  },
  "resource": {
    "process.command": "/usr/src/app/index.js",
    "process.command_line": "/usr/local/bin/node /usr/src/app/index.js",
    "process.executable.name": "node",
    "process.pid": 1,
    "process.runtime.description": "Node.js",
    "process.runtime.name": "nodejs",
    "process.runtime.version": "16.18.0",
    "service.name": "paymentservice",
    "telemetry.sdk.language": "nodejs",
    "telemetry.sdk.name": "opentelemetry",
    "telemetry.sdk.version": "1.7.0"
  },
  "other": "value",
  "type": "otel"
}"#;
        let context = Annotated::new(Context::Otel(Box::new(OtelContext {
            attributes: Annotated::new(Object::from([
                (
                    "app.payment.amount".into(),
                    Annotated::new(Value::F64(394.25)),
                ),
                (
                    "rpc.grpc.status_code".into(),
                    Annotated::new(Value::String("1".into())),
                ),
                (
                    "rpc.method".into(),
                    Annotated::new(Value::String("Charge".into())),
                ),
                (
                    "rpc.service".into(),
                    Annotated::new(Value::String("hipstershop.PaymentService".into())),
                ),
                (
                    "rpc.system".into(),
                    Annotated::new(Value::String("grpc".into())),
                ),
            ])),
            resource: Annotated::new(Object::from([
                (
                    "process.command".into(),
                    Annotated::new(Value::String("/usr/src/app/index.js".into())),
                ),
                (
                    "process.command_line".into(),
                    Annotated::new(Value::String(
                        "/usr/local/bin/node /usr/src/app/index.js".into(),
                    )),
                ),
                (
                    "process.executable.name".into(),
                    Annotated::new(Value::String("node".into())),
                ),
                ("process.pid".into(), Annotated::new(Value::I64(1))),
                (
                    "process.runtime.description".into(),
                    Annotated::new(Value::String("Node.js".into())),
                ),
                (
                    "process.runtime.name".into(),
                    Annotated::new(Value::String("nodejs".into())),
                ),
                (
                    "process.runtime.version".into(),
                    Annotated::new(Value::String("16.18.0".into())),
                ),
                (
                    "service.name".into(),
                    Annotated::new(Value::String("paymentservice".into())),
                ),
                (
                    "telemetry.sdk.language".into(),
                    Annotated::new(Value::String("nodejs".into())),
                ),
                (
                    "telemetry.sdk.name".into(),
                    Annotated::new(Value::String("opentelemetry".into())),
                ),
                (
                    "telemetry.sdk.version".into(),
                    Annotated::new(Value::String("1.7.0".into())),
                ),
            ])),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".into(),
                    Annotated::new(Value::String("value".into())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
