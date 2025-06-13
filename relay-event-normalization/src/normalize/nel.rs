//! Contains helper function for NEL reports.

use chrono::{DateTime, Utc};
use relay_event_schema::protocol::{
    Attribute, AttributeType, AttributeValue, NetworkReportRaw, OurLog, OurLogLevel, Timestamp,
    TraceId,
};
use relay_protocol::{Annotated, Object, Value};

/// Enriches the event with new values using the provided [`NetworkReportRaw`].
pub fn create_log(nel: Annotated<NetworkReportRaw>, received_at: DateTime<Utc>) -> Option<OurLog> {
    // If the incoming NEL report is empty or it contains an empty body, just exit.
    let nel = nel.into_value()?;
    let body = nel.body.into_value()?;

    // event.logger = Annotated::from("nel".to_string());

    let message = if nel.ty.value().map_or("<unknown-type>", |v| v.as_str()) == "http.error" {
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

    // let request = event.request.get_or_insert_with(Request::default);
    // request.url = nel.url;
    // request.method = body.method;
    // request.protocol = body.protocol;

    // TODO: this should still be part of the envelope headers at this point
    // if let Some(ref user_agent) = nel.user_agent.value() {
    //     if !user_agent.is_empty() {
    //         headers.insert(
    //             HeaderName::new("user-agent"),
    //             HeaderValue::new(user_agent).into(),
    //         );
    //     }
    // }

    // let nel_context = contexts.get_or_default::<NelContext>();
    // nel_context.server_ip = body.server_ip;
    // nel_context.elapsed_time = body.elapsed_time;
    // nel_context.error_type = body.ty;
    // nel_context.phase = body.phase;
    // nel_context.sampling_fraction = body.sampling_fraction;

    // Set response status code only if it's bigger than zero.
    // let status_code = body
    //     .status_code
    //     .map_value(|v| u64::try_from(v).unwrap_or(0));
    // if status_code.value().unwrap_or(&0) > &0 {
    //     let response_context = contexts.get_or_default::<ResponseContext>();
    //     response_context.status_code = status_code;
    // }

    // Set the timestamp on the event when it actually occurred.
    // let event_time = event
    //     .timestamp
    //     .value_mut()
    //     .map_or(Utc::now(), |timestamp| timestamp.into_inner());
    // if let Some(event_time) =
    //     event_time.checked_sub_signed(Duration::milliseconds(*nel.age.value().unwrap_or(&0)))
    // {
    //     event.timestamp = Annotated::new(Timestamp::from(event_time))
    // }

    let mut attributes: Object<Attribute> = Default::default();

    macro_rules! add_attribute {
        ($name:literal, $value:expr) => {{
            attributes.insert(
                $name.to_owned(),
                Annotated::new(Attribute {
                    value: AttributeValue {
                        ty: Annotated::new(AttributeType::String),
                        value: $value.map_value(Into::into),
                    },
                    other: Default::default(),
                }),
            );
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
        // TODO: nel age correction
        timestamp: Annotated::new(Timestamp::from(received_at)),
        trace_id: Annotated::new(TraceId::new()),
        level: Annotated::new(OurLogLevel::Info),
        body: Annotated::new(message),
        attributes: Annotated::new(attributes),
        ..Default::default()
    })
}
