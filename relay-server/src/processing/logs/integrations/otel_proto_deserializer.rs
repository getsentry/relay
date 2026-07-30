use bytes::Buf;
use opentelemetry_proto::tonic::logs::*;
use prost::Message as _;
use prost::encoding::{DecodeContext, WireType, check_wire_type, decode_key, decode_varint};
use std::fmt::Display;

use crate::processing::logs;
use crate::services::outcome::DiscardReason;

/// Field tag of `LogsData::resource_logs`.
const TAG_RESOURCE_LOGS: u32 = 1;
/// Field tag of `ResourceLogs::scope_logs`.
const TAG_SCOPE_LOGS: u32 = 2;
/// Field tag of `ScopeLogs::log_records`.
const TAG_LOG_RECORDS: u32 = 2;

pub enum Error {
    MeterExhausted,
    BufferUnderflow,
    DelimitedLengthExceeded,
    ProstError(prost::DecodeError),
}

struct Meter {
    remaining: usize,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MeterExhausted => write!(f, "meter exhausted"),
            Error::BufferUnderflow => write!(f, "buffer underflow "),
            Error::DelimitedLengthExceeded => write!(f, "delimited length exceeded "),
            Error::ProstError(decode_error) => {
                write!(f, "prost decoding error: {}", decode_error)
            }
        }
    }
}

impl From<prost::DecodeError> for Error {
    fn from(value: prost::DecodeError) -> Self {
        Self::ProstError(value)
    }
}

impl Meter {
    fn new(max: usize) -> Self {
        Self { remaining: max }
    }

    fn spend(&mut self) -> Result<(), Error> {
        if self.remaining == 0 {
            Err(Error::MeterExhausted)
        } else {
            self.remaining -= 1;
            Ok(())
        }
    }

    fn is_empty(&self) -> bool {
        self.remaining == 0
    }
}

/// Merges a top-level `v1::LogsData` message, which spans the entire buffer.
fn merge_logs_data<B: Buf>(
    meter: &mut Meter,
    out: &mut v1::LogsData,
    buf: &mut B,
    ctx: DecodeContext,
) -> Result<(), Error> {
    while buf.has_remaining() {
        let (tag, wire_type) = decode_key(buf)?;
        merge_logs_data_field(meter, out, tag, wire_type, buf, ctx.clone())?;
    }
    Ok(())
}

fn merge_logs_data_field<B: Buf>(
    meter: &mut Meter,
    out: &mut v1::LogsData,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
) -> Result<(), Error> {
    match tag {
        TAG_RESOURCE_LOGS => {
            check_wire_type(WireType::LengthDelimited, wire_type)?;
            meter.spend()?;

            let mut resource_logs = v1::ResourceLogs::default();
            merge_loop(&mut resource_logs, buf, ctx, |value, buf, ctx| {
                let (tag, wire_type) = decode_key(buf)?;
                merge_resource_logs_field(meter, value, tag, wire_type, buf, ctx)
            })?;
            out.resource_logs.push(resource_logs);
            Ok(())
        }
        _ => Ok(out.merge_field(tag, wire_type, buf, ctx)?),
    }
}

fn merge_resource_logs_field<B: Buf>(
    meter: &mut Meter,
    out: &mut v1::ResourceLogs,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
) -> Result<(), Error> {
    match tag {
        TAG_SCOPE_LOGS => {
            check_wire_type(WireType::LengthDelimited, wire_type)?;
            meter.spend()?;

            let mut scope_logs = v1::ScopeLogs::default();
            merge_loop(&mut scope_logs, buf, ctx, |value, buf, ctx| {
                let (tag, wire_type) = decode_key(buf)?;
                merge_scope_logs_field(meter, value, tag, wire_type, buf, ctx)
            })?;
            out.scope_logs.push(scope_logs);
            Ok(())
        }
        _ => Ok(out.merge_field(tag, wire_type, buf, ctx)?),
    }
}

fn merge_scope_logs_field<B: Buf>(
    meter: &mut Meter,
    out: &mut v1::ScopeLogs,
    tag: u32,
    wire_type: WireType,
    buf: &mut B,
    ctx: DecodeContext,
) -> Result<(), Error> {
    if tag == TAG_LOG_RECORDS {
        meter.spend()?;
    }
    Ok(out.merge_field(tag, wire_type, buf, ctx)?)
}

fn merge_loop<T, M, B>(
    value: &mut T,
    buf: &mut B,
    ctx: DecodeContext,
    mut merge: M,
) -> Result<(), Error>
where
    M: FnMut(&mut T, &mut B, DecodeContext) -> Result<(), Error>,
    B: Buf,
{
    let len = decode_varint(buf)?;
    let remaining = buf.remaining();
    if len > remaining as u64 {
        return Err(Error::BufferUnderflow);
    }

    let limit = remaining - len as usize;
    while buf.remaining() > limit {
        merge(value, buf, ctx.clone())?;
    }

    if buf.remaining() != limit {
        return Err(Error::DelimitedLengthExceeded);
    }
    Ok(())
}

/// Deserialize the supplied protobuf into v1::LogsData, returning an error if the number of logs
/// elements in the payload exceeds the supplied maximum.
pub fn deserialize(mut payload: &[u8], max: usize) -> Result<v1::LogsData, logs::Error> {
    let mut meter = Meter::new(max);
    let mut out = v1::LogsData::default();

    match merge_logs_data(&mut meter, &mut out, &mut payload, DecodeContext::default()) {
        Ok(()) => Ok(out),
        Err(_) if meter.is_empty() => Err(logs::Error::TooManyExpandedLogs),
        Err(_) => Err(logs::Error::Invalid(DiscardReason::InvalidProtobuf)),
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::*;
    use opentelemetry_proto::tonic::resource::v1::Resource;

    use super::*;

    fn log_record(body: &str) -> LogRecord {
        LogRecord {
            time_unix_nano: 123,
            observed_time_unix_nano: 123,
            severity_number: 2,
            severity_text: "Information".to_owned(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(body.to_owned())),
            }),
            attributes: vec![KeyValue {
                key: "attribute".to_owned(),
                value: Some(AnyValue {
                    value: Some(Value::StringValue("value".to_owned())),
                }),
            }],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: "5B8EFFF798038103D269B633813FC60C".into(),
            span_id: "EEE19B7EC3C1B174".into(),
            event_name: "".to_owned(),
        }
    }

    fn scope_logs(num_records: usize) -> ScopeLogs {
        ScopeLogs {
            scope: Some(InstrumentationScope {
                name: "test-library".to_owned(),
                version: "".to_owned(),
                attributes: vec![],
                dropped_attributes_count: 0,
            }),
            log_records: (0..num_records)
                .map(|i| log_record(&i.to_string()))
                .collect(),
            schema_url: "".to_owned(),
        }
    }

    fn resource_logs(num_scope_logs: usize, records: usize) -> ResourceLogs {
        ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_owned(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("test-service".to_owned())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: (0..num_scope_logs).map(|_| scope_logs(records)).collect(),
            schema_url: "http://example.com".to_owned(),
        }
    }

    fn logs_data(num_resource_logs: usize, num_scope_logs: usize, num_records: usize) -> LogsData {
        LogsData {
            resource_logs: (0..num_resource_logs)
                .map(|_| resource_logs(num_scope_logs, num_records))
                .collect(),
        }
    }

    #[test]
    fn test_basic_protobuf() {
        let expected = logs_data(2, 2, 3);
        let payload = expected.encode_to_vec();

        assert_eq!(deserialize(&payload, 1000).unwrap(), expected);
    }

    #[test]
    fn test_empty_payload() {
        assert_eq!(deserialize(&[], 1000).unwrap(), LogsData::default());
    }

    #[test]
    fn test_invalid_protobuf() {
        let err = deserialize(b"this is not protobuf", 1000).unwrap_err();
        assert!(matches!(
            err,
            logs::Error::Invalid(DiscardReason::InvalidProtobuf)
        ));
    }

    #[test]
    fn test_exact_limit() {
        let payload = logs_data(1, 1, 8).encode_to_vec();

        assert!(deserialize(&payload, 10).is_ok());
        assert!(matches!(
            deserialize(&payload, 9).unwrap_err(),
            logs::Error::TooManyExpandedLogs
        ));
    }

    #[test]
    fn test_too_many_log_records() {
        let payload = logs_data(1, 1, 1_001).encode_to_vec();

        assert!(matches!(
            deserialize(&payload, 1000).unwrap_err(),
            logs::Error::TooManyExpandedLogs
        ));
    }

    #[test]
    fn test_too_many_scope_logs() {
        let payload = logs_data(1, 1_001, 0).encode_to_vec();

        assert!(matches!(
            deserialize(&payload, 1000).unwrap_err(),
            logs::Error::TooManyExpandedLogs
        ));
    }

    #[test]
    fn test_too_many_resource_logs() {
        let payload = logs_data(1_001, 0, 0).encode_to_vec();

        assert!(matches!(
            deserialize(&payload, 1000).unwrap_err(),
            logs::Error::TooManyExpandedLogs
        ));
    }
}
