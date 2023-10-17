//! Contains helper function for NEL reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Contexts, Event, HeaderName, HeaderValue, Headers, LogEntry, NelContext, NetworkReportRaw,
    Request, ResponseContext, Timestamp,
};
use relay_protocol::Annotated;

/// Enriches the event with new values using the provided [`NetworkReportRaw`].
pub fn enrich_nel_event(event: &mut Event, nel: Annotated<NetworkReportRaw>) {
    // If the incoming NEL report is empty or it contains an empty body, just exit.
    let Some(nel) = nel.into_value() else {
        return;
    };
    let Some(body) = nel.body.into_value() else {
        return;
    };

    event.logger = Annotated::from("nel".to_string());

    event.logentry = Annotated::new(LogEntry::from({
        if nel.ty.value().map_or("<unknown-type>", |v| v.as_str()) == "http.error" {
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
        }
    }));

    let request = event.request.get_or_insert_with(Request::default);
    request.url = nel.url;
    request.method = body.method;
    request.protocol = body.protocol;

    let headers = request.headers.get_or_insert_with(Headers::default);

    if let Some(ref user_agent) = nel.user_agent.value() {
        if !user_agent.is_empty() {
            headers.insert(
                HeaderName::new("user-agent"),
                HeaderValue::new(user_agent).into(),
            );
        }
    }

    if let Some(referrer) = body.referrer.value() {
        headers.insert(
            HeaderName::new("referer"),
            HeaderValue::new(referrer).into(),
        );
    }

    let contexts = event.contexts.get_or_insert_with(Contexts::new);

    let nel_context = contexts.get_or_default::<NelContext>();
    nel_context.server_ip = body.server_ip;
    nel_context.elapsed_time = body.elapsed_time;
    nel_context.error_type = body.ty;
    nel_context.phase = body.phase;
    nel_context.sampling_fraction = body.sampling_fraction;

    let response_context = contexts.get_or_default::<ResponseContext>();
    response_context.status_code = body
        .status_code
        .map_value(|v| u64::try_from(v).unwrap_or(0));

    // Set the timestamp on the event when it actually occurred.
    let now: DateTime<Utc> = Utc::now();
    if let Some(event_time) =
        now.checked_sub_signed(Duration::milliseconds(*nel.age.value().unwrap_or(&0)))
    {
        event.timestamp = Annotated::new(Timestamp::from(event_time));
    }
}
