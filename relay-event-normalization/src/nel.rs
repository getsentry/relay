//! Contains normalization for NEL reports.

use chrono::{DateTime, Duration, Utc};
use relay_event_schema::protocol::{
    Contexts, Event, HeaderName, HeaderValue, Headers, LogEntry, NelContext, NetworkReportRaw,
    PairList, Request, ResponseContext, Timestamp,
};
use relay_protocol::Annotated;

/// Normalize the event using the provided [`NetworkReportRaw`].
pub fn normalize(event: &mut Event, nel: Annotated<NetworkReportRaw>) {
    // if the provided raw report is empty, just exit.
    let Some(nel) = nel.value() else {
        return;
    };

    if let Some(body) = nel.body.value() {
        event.logentry = Annotated::new(LogEntry::from({
            if nel.ty.value().map_or("", |v| v.as_str()) == "http.error" {
                format!(
                    "{} / {} ({})",
                    body.phase.as_str().unwrap_or(""),
                    body.ty.as_str().unwrap_or(""),
                    body.status_code.value().unwrap_or(&0)
                )
            } else {
                format!(
                    "{} / {}",
                    body.phase.as_str().unwrap_or(""),
                    body.ty.as_str().unwrap_or(""),
                )
            }
        }));

        let headers = match nel.user_agent.value() {
            Some(ref user_agent) if !user_agent.is_empty() => {
                Annotated::new(Headers(PairList(vec![Annotated::new((
                    Annotated::new(HeaderName::new("User-Agent")),
                    Annotated::new(HeaderValue::new(user_agent)),
                ))])))
            }
            Some(_) | None => Annotated::empty(),
        };
        let request = Request {
            url: nel.url.clone(),
            headers,
            method: body.method.clone(),
            protocol: body.protocol.clone(),
            ..Default::default()
        };

        event.request = request.into();
        event.logger = Annotated::from("nel".to_string());

        let contexts = event.contexts.get_or_insert_with(Contexts::new);

        let nel_context = contexts.get_or_default::<NelContext>();
        nel_context.ty = nel.ty.clone();
        nel_context.age = nel.age.clone();

        let response_context = contexts.get_or_default::<ResponseContext>();
        response_context.origin = body.server_ip.clone();
        response_context.status_code = body
            .status_code
            .clone()
            .map_value(|v| u64::try_from(v).unwrap_or(0));

        // Set the timestamp on the event when it actually occurred.
        let now: DateTime<Utc> = Utc::now();
        if let Some(event_time) =
            now.checked_sub_signed(Duration::milliseconds(*nel.age.value().unwrap_or(&0)))
        {
            event.timestamp = Annotated::new(Timestamp::from(event_time));
        }
    }
}
