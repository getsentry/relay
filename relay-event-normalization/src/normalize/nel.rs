//! Contains helper function for NEL reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Attribute, AttributeValue, NetworkReportRaw, OurLog, OurLogLevel, Timestamp, TraceId,
};
use relay_protocol::{Annotated, Object};

/// Creates a [`OurLog`] from the provided [`NetworkReportRaw`].
pub fn create_log(nel: Annotated<NetworkReportRaw>, received_at: DateTime<Utc>) -> Option<OurLog> {
    let nel = nel.into_value()?;
    let body = nel.body.into_value()?;

    let message = if nel.ty.value().is_some_and(|v| v.as_str() == "http.error") {
        format!(
            "{} / {} ({})",
            body.phase.as_str().unwrap_or("<unknown-phase>"),
            body.ty.as_str().unwrap_or("<unknown-type>"),
            body.status_code.value().unwrap_or(&0)
        )
    } else {
        format!(
            "{} / {}",
            body.phase.as_str().unwrap_or("<unknown-phase>"),
            body.ty.as_str().unwrap_or("<unknown-type>"),
        )
    };

    let timestamp = received_at
        .checked_sub_signed(Duration::milliseconds(*nel.age.value().unwrap_or(&0)))
        .unwrap_or(received_at);

    let mut attributes: Object<Attribute> = Default::default();

    macro_rules! add_attribute {
        ($name:literal, $value:expr) => {{
            if let Some(value) = $value.into_value() {
                attributes.insert(
                    $name.to_owned(),
                    Annotated::new(Attribute {
                        value: AttributeValue::from(value),
                        other: Default::default(),
                    }),
                );
            }
        }};
    }

    add_attribute!("todo.nel.referrer", body.referrer);
    add_attribute!(
        "todo.nel.server_ip",
        body.server_ip.map_value(|s| s.to_string())
    );
    add_attribute!("todo.nel.elapsed_time", body.elapsed_time);
    add_attribute!("todo.nel.error_type", body.ty);
    add_attribute!("todo.nel.phase", body.phase.map_value(|s| s.to_string()));
    add_attribute!("todo.nel.sampling_fraction", body.sampling_fraction);

    Some(OurLog {
        timestamp: Annotated::new(Timestamp::from(timestamp)),
        trace_id: Annotated::new(TraceId::random()),
        level: Annotated::new(OurLogLevel::Info),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}
