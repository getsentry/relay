//! Resolve the client address while the original event fields are still available.

use std::mem;
use std::net::IpAddr;

use relay_event_schema::processor::{ProcessingResult, ProcessingState, Processor};
use relay_event_schema::protocol::{
    AutoInferSetting, ClientSdkInfo, Event, IpAddr as ProtocolIp, Request, User,
};
use relay_pii::ProjectPii;
use relay_protocol::{Annotated, Meta, Remark, RemarkType, Value};

use crate::NormalizationConfig;

pub fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
}

fn remote_addr(request: &Annotated<Request>) -> Option<&Annotated<Value>> {
    request.value()?.env.value()?.get("REMOTE_ADDR")
}

pub fn remote_redacted(request: &Annotated<Request>) -> bool {
    redacted(request)
        || request.value().is_some_and(|r| redacted(&r.env))
        || remote_addr(request).is_some_and(redacted)
}

fn lookup_ip(
    event: &Event,
    client_ip: Option<&relay_event_schema::protocol::IpAddr>,
) -> Option<IpAddr> {
    let field = event.user.value().map(|u| &u.ip_address);
    if redacted(&event.user) || field.is_some_and(redacted) {
        return None;
    }
    let ip = field
        .and_then(|field| field.value())
        .filter(|ip| !ip.is_auto());
    if ip.is_none() && remote_redacted(&event.request) {
        return None;
    }
    ip.or(client_ip)?.as_str().parse().ok()
}

/// The IP shown to filters and the source to consult when the real event is scrubbed.
/// This context lives for one event on one Relay and is never serialized.
#[derive(Clone, Copy, Debug, Default)]
pub struct EventIp {
    source: Source,
    span_tag_limit: Option<usize>,
    trim: bool,
}

#[derive(Clone, Copy, Debug, Default)]
enum Source {
    #[default]
    Event,
    RemoteAddr,
    TransientTransport(IpAddr),
}

impl EventIp {
    /// Whether to publish the shared user tags after scrubbing the real user.
    pub fn defers_span_tags(self) -> bool {
        self.span_tag_limit.is_some()
    }

    /// Apply source rules, process the complete user, and then scrub the remaining event.
    pub fn scrub(self, event: &mut Annotated<Event>, rules: ProjectPii<'_>) -> ProcessingResult {
        let Some(inner) = event.value_mut() else {
            return Ok(());
        };
        if rules.is_empty() {
            return Ok(());
        }
        if let Source::TransientTransport(ip) = self.source {
            // Filters have already run. Expose the address only to the actual user scrubber.
            inner
                .user
                .get_or_insert_with(User::default)
                .ip_address
                .set_value(Some(ip.into()));
        }
        let mut fields = Annotated::new(Event::default());
        if matches!(self.source, Source::Event) && inner.spans.value().is_none_or(Vec::is_empty) {
            return rules.scrub_event(event);
        }
        // Only a request-derived user IP depends on what the request scrubber does.
        // Move the actual request, including ancestor metadata and original values.
        if matches!(self.source, Source::RemoteAddr) {
            fields.value_mut().as_mut().unwrap().request = mem::take(&mut inner.request);
            rules.scrub_event(&mut fields)?;
            let Some(request) = fields.value_mut() else {
                *event = fields;
                return Ok(());
            };
            let remote = remote_addr(&request.request).and_then(|v| v.as_str());
            if remote_redacted(&request.request)
                || remote.is_none()
                || remote != inner.user.value().and_then(|u| u.ip_address.as_str())
            {
                discard_backfill(&mut inner.user);
            }
        }
        let scrubbed_request = fields
            .value_mut()
            .as_mut()
            .map(|e| mem::take(&mut e.request));
        // Decide the fallback on the complete user after both sets of identity rules.
        fields.value_mut().as_mut().unwrap().user = mem::take(&mut inner.user);
        rules.scrub_event_with_ip_fallback(
            &mut fields,
            matches!(self.source, Source::TransientTransport(_)),
        )?;
        let Some(identity) = fields.value_mut() else {
            *event = fields;
            return Ok(());
        };
        let added_tags =
            self.span_tag_limit
                .zip(identity.user.value())
                .is_some_and(|(limit, user)| {
                    crate::span::tag_extraction::copy_scrubbed_span_user_tags(inner, user, limit)
                });
        rules.scrub_event(event)?;
        if let Some(inner) = event.value_mut() {
            inner.user = mem::take(&mut identity.user);
            if self.trim && added_tags {
                // The whole event was trimmed during normalization. Enforce the same
                // span-array budget after publishing the optional shared identity tags.
                identity.spans = mem::take(&mut inner.spans);
                let _ = crate::TrimmingProcessor::new().process_event(
                    identity,
                    &mut Meta::default(),
                    ProcessingState::root(),
                );
                inner.spans = mem::take(&mut identity.spans);
            }
            if matches!(self.source, Source::RemoteAddr)
                && let Some(request) = scrubbed_request
            {
                inner.request = request;
            }
        }
        Ok(())
    }
}

/// Resolve the IP, enrich from it, then decide which raw values may be copied.
/// Record the selected source while it is unambiguous, before filtering and sampling.
pub fn normalize_event_ip(event: &mut Event, config: &NormalizationConfig<'_>) -> EventIp {
    let rules = config.ip_pii_rules;
    let may_hash = rules.advanced.is_some_and(|r| !r.applications.is_empty());
    let security_report = event.csp.value().is_some();
    let backfill = resolve_backfill(
        &mut event.request,
        &event.user,
        event.platform.as_str(),
        config
            .client_ip
            .filter(|_| config.infer_ip_address || may_hash),
        event.client_sdk.value(),
        security_report && config.infer_ip_address,
    );

    let ip = match &backfill {
        Backfill::Remote(ip) | Backfill::Transport(ip) => ip.as_str().parse().ok(),
        _ => lookup_ip(event, config.client_ip),
    };
    if let (Some(db), Some(ip)) = (config.geoip_lookup, ip)
        && !redacted(&event.user)
    {
        let user = event.user.get_or_insert_with(User::default);
        if user.geo.value().is_none() && !redacted(&user.geo) {
            user.geo.set_value(db.lookup(ip));
        }
    }

    let source = match &backfill {
        Backfill::Remote(_) => Source::RemoteAddr,
        Backfill::Transport(ip) if !config.infer_ip_address => {
            // Transport candidates were validated by the resolver.
            Source::TransientTransport(ip.as_str().parse().expect("valid transport address"))
        }
        _ => Source::Event,
    };
    if matches!(source, Source::TransientTransport(_)) {
        Backfill::ClearAuto.apply(&mut event.user);
    } else {
        backfill.apply(&mut event.user);
    }
    let defer_tags = config.enrich_spans
        && !rules.is_empty()
        && (matches!(source, Source::TransientTransport(_))
            || event
                .user
                .value()
                .is_some_and(|u| u.ip_address.value().is_some()));
    EventIp {
        source,
        span_tag_limit: defer_tags.then_some(config.max_tag_value_length),
        trim: config.enable_trimming,
    }
}

fn discard_backfill(user: &mut Annotated<User>) {
    let ip = &mut user.get_or_insert_with(User::default).ip_address;
    ip.set_value(None);
    ip.meta_mut()
        .add_remark(Remark::new(RemarkType::Removed, "pii:ip_address"));
}

pub enum Backfill {
    Unchanged,
    ClearAuto,
    Remote(ProtocolIp),
    Transport(ProtocolIp),
}

impl Backfill {
    pub fn apply(self, user: &mut Annotated<User>) {
        match self {
            Self::Remote(ip) | Self::Transport(ip) => user
                .get_or_insert_with(User::default)
                .ip_address
                .set_value(Some(ip)),
            Self::ClearAuto => {
                if let Some(user) = user.value_mut() {
                    user.ip_address.set_value(None);
                }
            }
            Self::Unchanged => {}
        }
    }
}

pub fn resolve_backfill(
    request: &mut Annotated<Request>,
    user: &Annotated<User>,
    platform: Option<&str>,
    client_ip: Option<&ProtocolIp>,
    sdk: Option<&ClientSdkInfo>,
    security_report: bool,
) -> Backfill {
    let infer = sdk
        .and_then(|s| s.settings.0.as_ref())
        .map(|s| s.infer_ip())
        .unwrap_or_default();
    let client_ip =
        client_ip.filter(|ip| infer != AutoInferSetting::Never && !ip.is_auto() && ip.is_valid());
    let removed = remote_redacted(request);
    let remote = if removed {
        None
    } else {
        normalize_remote_addr(request, client_ip)
    };
    let current = user.value().map(|u| &u.ip_address);
    if redacted(user) || current.is_some_and(redacted) {
        return Backfill::Unchanged;
    }
    let current = current.and_then(|v| v.value());
    let auto = current.is_some_and(ProtocolIp::is_auto);
    if current.is_some() && !auto {
        return Backfill::Unchanged;
    }
    if security_report && let Some(client) = client_ip {
        return Backfill::Transport(client.clone());
    }
    let enabled = !removed
        && match infer {
            AutoInferSetting::Never => false,
            AutoInferSetting::Auto => true,
            AutoInferSetting::Legacy => {
                auto || remote.is_some()
                    || matches!(platform, Some("javascript" | "cocoa" | "objc"))
            }
        };
    if enabled {
        if let Some(remote) = remote {
            return Backfill::Remote(remote);
        }
        if let Some(client) = client_ip {
            return Backfill::Transport(client.clone());
        }
    }
    if auto {
        Backfill::ClearAuto
    } else {
        Backfill::Unchanged
    }
}

/// Resolves an explicit REMOTE_ADDR marker in place, or removes it if inference is unavailable.
fn normalize_remote_addr(
    request: &mut Annotated<Request>,
    client_ip: Option<&ProtocolIp>,
) -> Option<ProtocolIp> {
    let env = request.value_mut().as_mut()?.env.value_mut().as_mut()?;
    let remote_addr = env.get_mut("REMOTE_ADDR")?;
    let ip = ProtocolIp::parse(remote_addr.as_str()?).ok()?;
    if !ip.is_auto() {
        return Some(ip);
    }

    if let Some(ip) = client_ip {
        remote_addr.set_value(Some(Value::String(ip.to_string())));
    } else {
        env.remove("REMOTE_ADDR");
    }
    client_ip.cloned()
}
