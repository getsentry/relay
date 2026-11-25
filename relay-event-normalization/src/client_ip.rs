//! Resolve the client address and finish its derived identity before span tag extraction.
use std::mem;
use std::net::IpAddr;

use relay_event_schema::processor::{ProcessingAction, ProcessingResult};
use relay_event_schema::protocol::{
    AutoInferSetting, Event, IpAddr as ProtocolIp, ParsedIp, Request, User,
};
use relay_pii::ProjectPii;
use relay_protocol::{Annotated, Empty, Value};

use crate::NormalizationConfig;

pub fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
}

fn remote_addr(request: &Annotated<Request>) -> Option<&Annotated<Value>> {
    request.value()?.env.value()?.get("REMOTE_ADDR")
}

fn remote_redacted(request: &Annotated<Request>) -> bool {
    redacted(request)
        || request.value().is_some_and(|r| redacted(&r.env))
        || remote_addr(request).is_some_and(redacted)
}

/// Resolve the request marker in place. Keep SDK-supplied address spelling for IP hash rules.
fn normalize_remote(
    request: &mut Annotated<Request>,
    client: Option<(&ProtocolIp, IpAddr)>,
) -> Option<IpAddr> {
    let env = request.value_mut().as_mut()?.env.value_mut().as_mut()?;
    let remote = env.get_mut("REMOTE_ADDR")?;
    match remote.as_str()?.parse::<ParsedIp>().ok()? {
        ParsedIp::Address(ip) => Some(ip),
        ParsedIp::Auto => match client {
            Some((text, ip)) => {
                remote.set_value(Some(Value::String(text.to_string())));
                Some(ip)
            }
            None => {
                env.remove("REMOTE_ADDR");
                None
            }
        },
    }
}

/// Look up the original address, scrub its request source, then build and scrub the user.
/// The caller extracts span tags from the finished user.
pub fn process_client_ip(
    event: &mut Event,
    config: &NormalizationConfig<'_>,
    rules: ProjectPii<'_>,
) -> ProcessingResult {
    let infer = event
        .client_sdk
        .value()
        .and_then(|sdk| sdk.settings.value())
        .map(|s| s.infer_ip())
        .unwrap_or_default();
    let transport = config
        .client_ip
        .and_then(|text| text.as_str().parse::<IpAddr>().ok().map(|ip| (text, ip)));
    let may_hash = rules.advanced.is_some_and(|r| !r.applications.is_empty());
    let client = transport
        .filter(|_| infer != AutoInferSetting::Never && (config.infer_ip_address || may_hash));
    let remote_removed = remote_redacted(&event.request);
    let remote = if remote_removed {
        None
    } else {
        normalize_remote(&mut event.request, client)
    };

    let user_ip = event.user.value().map(|u| &u.ip_address);
    let protected = redacted(&event.user) || user_ip.is_some_and(redacted);
    let supplied = user_ip.and_then(|v| v.value()).map(ProtocolIp::parsed);
    let auto = matches!(supplied, Some(Ok(ParsedIp::Auto)));
    let missing = supplied.is_none() || auto;
    let infer_user = !protected
        && missing
        && !remote_removed
        && match infer {
            AutoInferSetting::Never => false,
            AutoInferSetting::Auto => true,
            AutoInferSetting::Legacy => {
                auto || remote.is_some()
                    || matches!(
                        event.platform.as_str(),
                        Some("javascript" | "cocoa" | "objc")
                    )
            }
        };
    let csp_client = !protected
        && missing
        && event.csp.value().is_some()
        && config.infer_ip_address
        && client.is_some();
    let use_remote = infer_user && remote.is_some() && !csp_client;
    let lookup = match supplied {
        _ if protected => None,
        Some(Ok(ParsedIp::Address(ip))) => Some(ip),
        Some(Err(_)) => None,
        _ if use_remote => remote,
        _ if !remote_removed || csp_client => transport.map(|(_, ip)| ip),
        _ => None,
    };
    if let (Some(db), Some(ip)) = (config.geoip_lookup, lookup) {
        let user = event.user.get_or_insert_with(User::default);
        if user.geo.value().is_none() && !redacted(&user.geo) {
            user.geo.set_value(db.lookup(ip));
        }
    }
    if let Some(db) = config.asn_lookup {
        crate::client_asn::enrich_event_asn(event, lookup, db, rules);
    }

    // Run the real project selectors at request.env, including rules on its ancestors.
    // Other request fields retain their usual place in filtering and scrubbing.
    if !rules.is_empty()
        && let Some(request) = event.request.value_mut()
        && !request.env.is_empty()
    {
        let mut fields = Annotated::new(Event {
            request: Annotated::new(Request {
                env: mem::take(&mut request.env),
                ..Default::default()
            }),
            ..Default::default()
        });
        rules.scrub_event(&mut fields)?;
        let scrubbed = fields
            .into_value()
            .ok_or(ProcessingAction::DeleteValueHard)?
            .request;
        if let Some(r) = scrubbed.0 {
            request.env = r.env;
        } else {
            event.request = scrubbed;
        }
    }

    // A scrubbed request address cannot backfill the user or supply a user-ID hash.
    let fallback = if use_remote {
        remote_addr(&event.request)
            .filter(|field| !redacted(field))
            .and_then(|v| v.as_str())
            .map(|s| ProtocolIp(s.to_owned()))
    } else if csp_client || infer_user {
        client.map(|(ip, _)| ip.clone())
    } else {
        None
    };
    let hash_only = fallback.is_some() && !use_remote && !config.infer_ip_address;
    let mut user = mem::take(&mut event.user);
    if let Some(ip) = fallback {
        user.get_or_insert_with(User::default)
            .ip_address
            .set_value(Some(ip));
    } else if auto
        && !protected
        && let Some(user) = user.value_mut()
    {
        user.ip_address.set_value(None);
    }
    if !rules.is_empty() && !user.is_empty() {
        let mut fields = Annotated::new(Event {
            user,
            ..Default::default()
        });
        rules.scrub_event_with_ip_fallback(&mut fields, hash_only)?;
        user = fields
            .into_value()
            .ok_or(ProcessingAction::DeleteValueHard)?
            .user;
        if hash_only && let Some(user) = user.value_mut() {
            user.ip_address.set_value(None);
        }
    }
    event.user = user;
    Ok(())
}
