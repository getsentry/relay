//! Derive client ASN from the scrubbed IP and apply destination rules to the new fields.

use std::net::IpAddr;

use relay_conventions::attributes::{
    CLIENT__ADDRESS, CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION, USER__IP_ADDRESS,
};
use relay_dynamic_config::Feature;
use relay_event_normalization::AsnLookup;
use relay_event_schema::processor::{ProcessingAction, ProcessingState, ValueType};
use relay_event_schema::protocol::{
    Attribute, Attributes, Contexts, Event, Measurements, Span, SpanData, SpanV2, Tags,
    TraceContext,
};
use relay_protocol::{Annotated, Value};

use crate::processing::{Context, utils};
use crate::services::processor::ProcessingError;
use crate::services::projects::project::ProjectInfo;

const ASN_KEYS: [&str; 2] = [CLIENT__AS__NUMBER, CLIENT__AS__ORGANIZATION];

/// ASN processing selected from the Relay and project configuration.
#[derive(Clone, Copy)]
pub enum Mode<'a> {
    Lookup,
    LookupWithPii(&'a ProjectInfo),
}

impl<'a> Mode<'a> {
    pub fn for_project(ctx: Context<'a>) -> Option<Self> {
        let config = &ctx.project_info.config;
        if !ctx.is_processing()
            || !ctx.project_info.has_feature(Feature::ClientAsnLookup)
            || config.datascrubbing_settings.scrub_ip_addresses
        {
            return None;
        }

        if config.pii_config.is_some() || config.datascrubbing_settings.pii_config().is_some() {
            Some(Self::LookupWithPii(ctx.project_info))
        } else {
            Some(Self::Lookup)
        }
    }
}

fn redacted<T>(value: &Annotated<T>) -> bool {
    value.meta().iter_remarks().next().is_some()
}

fn unredacted<T>(value: &Annotated<T>) -> Option<&Annotated<T>> {
    (!redacted(value)).then_some(value)
}

fn trace_redacted(event: &Event) -> bool {
    redacted(&event.contexts)
        || event
            .contexts
            .value()
            .and_then(|contexts| contexts.0.get("trace"))
            .is_some_and(redacted)
}

fn unscrubbed_user_ip(event: &Event) -> Option<&str> {
    let user = unredacted(&event.user)?.value()?;
    let ip = unredacted(&user.ip_address)?.value()?;
    // Normalization can backfill user.ip_address from request.env.REMOTE_ADDR.
    if redacted(&event.request)
        || event.request.value().is_some_and(|request| {
            redacted(&request.env)
                || request
                    .env
                    .value()
                    .and_then(|env| env.get("REMOTE_ADDR"))
                    .is_some_and(redacted)
        })
    {
        return None;
    }
    Some(ip.as_str())
}

enum SpanSource<'a> {
    Standalone(&'a Attributes),
    Segment(&'a Event),
    Child(&'a Span),
}

struct SpanAddress<'a> {
    ip: IpAddr,
    key: &'static str,
    text: &'a str,
}

fn attribute_value(entry: &Annotated<Attribute>) -> Option<&Annotated<Value>> {
    let attribute = unredacted(entry)?.value()?;
    unredacted(&attribute.value.value)
}

/// Selects a surviving client address, respecting removal remarks on either alias.
fn span_address(source: SpanSource<'_>) -> Option<SpanAddress<'_>> {
    let data = match source {
        SpanSource::Standalone(_) => None,
        SpanSource::Segment(event) => {
            let data = &event.context::<TraceContext>()?.data;
            if redacted(data) || redacted(&event.tags) || trace_redacted(event) {
                return None;
            }
            data.value()
        }
        SpanSource::Child(span) => {
            if redacted(&span.data) || redacted(&span.tags) {
                return None;
            }
            span.data.value()
        }
    };

    let mut selected = None;
    // Inspect both aliases. A redaction on either one vetoes the lookup.
    for key in [CLIENT__ADDRESS, USER__IP_ADDRESS] {
        let text = if let Some(value) = data.and_then(|data| data.get(key)) {
            unredacted(value)?.as_str()
        } else {
            match source {
                SpanSource::Standalone(attrs) => {
                    let Some(entry) = attrs.0.get(key) else {
                        continue;
                    };
                    attribute_value(entry)?.as_str()
                }
                SpanSource::Segment(event) => {
                    let Some(tag) = event.tags.value().and_then(|tags| tags.0.get(key)) else {
                        continue;
                    };
                    unredacted(tag)?.as_str()
                }
                SpanSource::Child(span) => {
                    let Some(tag) = span.tags.value().and_then(|tags| tags.get(key)) else {
                        continue;
                    };
                    unredacted(tag)?.as_str()
                }
            }
        };
        if selected.is_none() {
            selected = Some((key, text?));
        }
    }

    let (key, text) = match (selected, source) {
        (Some(address), _) => address,
        (None, SpanSource::Segment(event)) => (USER__IP_ADDRESS, unscrubbed_user_ip(event)?),
        _ => return None,
    };
    let ip = text.parse().ok()?;
    Some(SpanAddress { ip, key, text })
}

pub fn scrub_span_attributes(
    attrs: Attributes,
    project: &ProjectInfo,
) -> Result<Attributes, ProcessingAction> {
    // Use the real span schema so object selectors and legacy value selectors see
    // their usual paths. Only these attributes go through the scrubber.
    let mut span = Annotated::new(SpanV2 {
        attributes: Annotated::new(attrs),
        ..Default::default()
    });
    let config = &project.config;
    relay_pii::eap::scrub(
        ValueType::Span,
        &mut span,
        config.pii_config.as_ref(),
        config.datascrubbing_settings.pii_config().as_ref(),
    )?;
    Ok(span
        .into_value()
        .and_then(|span| span.attributes.into_value())
        .unwrap_or_default())
}

fn lookup_fields(db: &AsnLookup, ip: IpAddr) -> Option<Attributes> {
    let (number, organization) = db.lookup(ip)?;
    let mut fields = Attributes::new();
    fields.insert(CLIENT__AS__NUMBER, i64::from(number));
    if let Some(organization) = organization {
        fields.insert(CLIENT__AS__ORGANIZATION, organization);
    }
    Some(fields)
}

pub fn enrich_span(
    span: &mut Annotated<SpanV2>,
    db: &AsnLookup,
    prepare: impl FnOnce(Attributes) -> Result<Attributes, ProcessingAction>,
) -> Result<(), ProcessingAction> {
    let Some(span) = span.value_mut() else {
        return Ok(());
    };
    if redacted(&span.attributes) {
        return Ok(());
    }
    let Some(attrs) = span.attributes.value_mut() else {
        return Ok(());
    };
    if !ASN_KEYS.iter().any(|key| attrs.contains_key(*key))
        && let Some(address) = span_address(SpanSource::Standalone(attrs))
        && let Some(fields) = lookup_fields(db, address.ip)
    {
        attrs.0.extend(prepare(fields)?);
    }
    Ok(())
}

pub fn enrich_error(
    event: &mut Annotated<Event>,
    db: &AsnLookup,
    mode: Mode<'_>,
) -> Result<(), ProcessingError> {
    let Some(event) = event.value_mut() else {
        return Ok(());
    };
    if has_asn_tags(event)
        || trace_redacted(event)
        || event
            .context::<TraceContext>()
            .is_some_and(|trace| has_asn_data(&trace.data))
    {
        return Ok(());
    }
    let Some((number, organization)) = unscrubbed_user_ip(event)
        .and_then(|text| text.parse().ok())
        .and_then(|ip| db.lookup(ip))
    else {
        return Ok(());
    };

    let mut tags = Tags::default();
    tags.0.insert(
        CLIENT__AS__NUMBER.into(),
        Annotated::new(number.to_string()),
    );
    if let Some(organization) = organization {
        tags.0.insert(
            CLIENT__AS__ORGANIZATION.into(),
            Annotated::new(organization.chars().take(200).collect()),
        );
    }
    if let Mode::LookupWithPii(project) = mode {
        let mut fields = Annotated::new(Event {
            tags: Annotated::new(tags),
            ..Default::default()
        });
        utils::event::scrub_pii(&mut fields, ProcessingState::root(), project)?;
        let Some(scrubbed) = fields
            .into_value()
            .and_then(|event| event.tags.into_value())
        else {
            return Ok(());
        };
        tags = scrubbed;
    }
    event
        .tags
        .get_or_insert_with(Default::default)
        .extend(tags.0.0);
    Ok(())
}

fn has_asn_tags(event: &Event) -> bool {
    redacted(&event.tags)
        || event
            .tags
            .value()
            .is_some_and(|tags| ASN_KEYS.iter().any(|key| tags.0.contains(key)))
}

fn has_asn_data(data: &Annotated<SpanData>) -> bool {
    redacted(data)
        || data
            .value()
            .is_some_and(|data| ASN_KEYS.iter().any(|key| data.contains(key)))
}

fn has_asn_measurement(measurements: &Annotated<Measurements>) -> bool {
    redacted(measurements)
        || measurements
            .value()
            .is_some_and(|m| ASN_KEYS.iter().any(|key| m.contains_key(*key)))
}

fn span_data(fields: Attributes) -> SpanData {
    SpanData {
        other: fields
            .into_iter()
            .filter_map(|(key, attr)| Some((key, attr.into_value()?.value.value)))
            .collect(),
    }
}

enum TransactionTarget {
    Segment,
    Child(usize),
}

fn scrubbed_transaction_data(
    db: &AsnLookup,
    address: SpanAddress<'_>,
    target: TransactionTarget,
    project: &ProjectInfo,
) -> Result<SpanData, ProcessingAction> {
    // Extracted spans do not pass through the standalone span scrubber.
    let mut source = Attributes::new();
    source.insert(address.key, address.text);
    let scrubbed = scrub_span_attributes(source, project)?;
    if scrubbed
        .0
        .get(address.key)
        .and_then(attribute_value)
        .and_then(|v| v.as_str())
        != Some(address.text)
    {
        return Ok(SpanData::default());
    }

    let Some(fields) = lookup_fields(db, address.ip) else {
        return Ok(SpanData::default());
    };
    let data = span_data(scrub_span_attributes(fields, project)?);
    if data.other.is_empty() {
        return Ok(data);
    }

    // Apply transaction selectors at the path where the derived fields will be stored.
    match target {
        TransactionTarget::Segment => {
            let mut contexts = Contexts::new();
            contexts.add(TraceContext {
                data: Annotated::new(data),
                ..Default::default()
            });
            let mut fields = Annotated::new(Event {
                contexts: Annotated::new(contexts),
                ..Default::default()
            });
            utils::event::scrub_pii(&mut fields, ProcessingState::root(), project)?;
            Ok(fields
                .value_mut()
                .as_mut()
                .and_then(Event::context_mut::<TraceContext>)
                .and_then(|trace| trace.data.value_mut().take())
                .unwrap_or_default())
        }
        TransactionTarget::Child(index) => {
            let mut fields = Annotated::new(Span {
                data: Annotated::new(data),
                ..Default::default()
            });
            let array = ProcessingState::root().enter_borrowed("spans", None, [ValueType::Array]);
            let state =
                array.enter_index(index, array.inner_attrs(), ValueType::for_field(&fields));
            utils::event::scrub_pii(&mut fields, &state, project)?;
            Ok(fields
                .into_value()
                .and_then(|span| span.data.into_value())
                .unwrap_or_default())
        }
    }
}

fn insert_span_data(target: &mut Annotated<SpanData>, data: SpanData) {
    if !data.other.is_empty() {
        target
            .get_or_insert_with(Default::default)
            .other
            .extend(data.other);
    }
}

pub fn enrich_transaction(
    event: &mut Annotated<Event>,
    db: &AsnLookup,
    mode: Mode<'_>,
) -> Result<(), ProcessingError> {
    let Some(event) = event.value_mut() else {
        return Ok(());
    };
    match mode {
        Mode::Lookup => enrich_transaction_fields(event, |address, _| {
            Ok(lookup_fields(db, address.ip)
                .map(span_data)
                .unwrap_or_default())
        }),
        Mode::LookupWithPii(project) => enrich_transaction_fields(event, |address, target| {
            scrubbed_transaction_data(db, address, target, project)
        }),
    }
}

fn enrich_transaction_fields(
    event: &mut Event,
    fields: impl Fn(SpanAddress<'_>, TransactionTarget) -> Result<SpanData, ProcessingAction>,
) -> Result<(), ProcessingError> {
    if !has_asn_tags(event)
        && !has_asn_measurement(&event.measurements)
        && let Some(trace) = event.context::<TraceContext>()
        && !has_asn_data(&trace.data)
        && let Some(address) = span_address(SpanSource::Segment(event))
    {
        let data = fields(address, TransactionTarget::Segment)?;
        if let Some(trace) = event.context_mut::<TraceContext>() {
            insert_span_data(&mut trace.data, data);
        }
    }
    for (index, span) in event.spans.value_mut().iter_mut().flatten().enumerate() {
        let Some(span) = span.value_mut() else {
            continue;
        };
        if span
            .tags
            .value()
            .is_some_and(|tags| ASN_KEYS.iter().any(|key| tags.contains_key(*key)))
            || has_asn_measurement(&span.measurements)
            || has_asn_data(&span.data)
        {
            continue;
        }
        let Some(address) = span_address(SpanSource::Child(span)) else {
            continue;
        };
        let data = fields(address, TransactionTarget::Child(index))?;
        insert_span_data(&mut span.data, data);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use relay_conventions::attributes::{CLIENT__ADDRESS, USER__IP_ADDRESS};
    use relay_event_schema::protocol::Attributes;
    use relay_protocol::{Remark, RemarkType};

    use super::{SpanSource, span_address};

    #[test]
    fn test_span_address_precedence() {
        let mut attrs = Attributes::new();
        attrs.insert(USER__IP_ADDRESS, "192.0.2.1");
        attrs.insert(CLIENT__ADDRESS, "2600::1");
        let address = span_address(SpanSource::Standalone(&attrs)).unwrap();
        assert_eq!((address.key, address.text), (CLIENT__ADDRESS, "2600::1"));

        attrs.insert(CLIENT__ADDRESS, "invalid");
        assert!(span_address(SpanSource::Standalone(&attrs)).is_none());

        attrs.remove(CLIENT__ADDRESS);
        let address = span_address(SpanSource::Standalone(&attrs)).unwrap();
        assert_eq!((address.key, address.text), (USER__IP_ADDRESS, "192.0.2.1"));
    }

    #[test]
    fn test_span_address_redacted_alias() {
        let mut attrs = Attributes::new();
        attrs.insert(CLIENT__ADDRESS, "192.0.2.1");
        attrs.insert(USER__IP_ADDRESS, "192.0.2.2");
        let alias = attrs.0.get_mut(USER__IP_ADDRESS).unwrap();
        alias
            .meta_mut()
            .add_remark(Remark::new(RemarkType::Substituted, "@ip"));
        assert!(span_address(SpanSource::Standalone(&attrs)).is_none());

        attrs.0.get_mut(USER__IP_ADDRESS).unwrap().set_value(None);
        assert!(span_address(SpanSource::Standalone(&attrs)).is_none());
    }
}
