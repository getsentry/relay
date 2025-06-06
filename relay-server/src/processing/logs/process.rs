use relay_event_normalization::{
    ClientHints, FromUserAgentInfo as _, RawUserAgentInfo, SchemaProcessor,
};
use relay_event_schema::processor::{ProcessingState, process_value};
use relay_event_schema::protocol::{Attribute, AttributeType, BrowserContext, OurLog};
use relay_ourlogs::OtelLog;
use relay_pii::PiiProcessor;
use relay_protocol::{Annotated, ErrorKind, Value};
use smallvec::SmallVec;

use crate::envelope::{ContainerItems, ContainerParseError, Item, ItemContainer};
use crate::extractors::RequestMeta;
use crate::processing::logs::{EinsLog, ZweiLog};
use crate::processing::{Context, Managed};
use crate::services::processor::ProcessingError;

pub fn expand(logs: Managed<EinsLog>) -> Managed<ZweiLog> {
    // TODO: if we have to drop singular elements here, how do we keep track which ones were
    // dropped?

    logs.map(|logs, records| {
        // TODO: loop over logs
        // TODO: error handling -> Outcomes for individual elements
        let mut all_logs = SmallVec::with_capacity(logs.count());

        for logs in logs.logs {
            let expanded = expand_log_container(&logs);
            let expanded = records.or_default(expanded, logs);
            all_logs.extend(expanded);
        }

        all_logs.reserve_exact(logs.otel_logs.len());
        for otel_log in logs.otel_logs {
            records.with(expand_otel_log(&otel_log), |log| all_logs.push(log));
        }

        ZweiLog {
            headers: logs.headers,
            logs: all_logs,
        }
    })
}

/// TODO: this could store errors in the annotated, but I am not sure how much sense this makes
fn expand_otel_log(item: &Item) -> Result<Annotated<OurLog>, ()> {
    let log = serde_json::from_slice::<OtelLog>(&item.payload())
        .inspect_err(|err| {
            relay_log::debug!("failed to parse OTel Log: {err}");
        })
        .map_err(drop)?;

    let log = relay_ourlogs::otel_to_sentry_log(log)
        .inspect_err(|err| {
            relay_log::debug!("failed to convert OTel Log to Sentry Log: {:?}", err);
        })
        .map_err(drop)?;

    Ok(Annotated::new(log))
}

fn expand_log_container(item: &Item) -> Result<ContainerItems<OurLog>, ContainerParseError> {
    let mut logs = ItemContainer::parse(item)
        .inspect_err(|err| {
            relay_log::debug!("failed to parse logs container: {err}");
        })?
        .into_items();

    for log in &mut logs {
        relay_ourlogs::ourlog_merge_otel(log);
    }

    Ok(logs)
}

pub fn process(logs: &mut Managed<ZweiLog>, ctx: Context<'_>) {
    // TODO: some signature that allows to directly retain items seems thinkable
    logs.modify(|logs, records| {
        let meta = logs.headers.meta();
        logs.logs
            .retain_mut(|log| records.or_default(process_log(log, meta, ctx).map(|_| true), &*log));
    });
}

fn process_log(
    log: &mut Annotated<OurLog>,
    meta: &RequestMeta,
    ctx: Context<'_>,
) -> Result<(), ProcessingError> {
    scrub(log, ctx).inspect_err(|err| {
        relay_log::debug!("failed to scrub pii from log: {err}");
    })?;

    normalize(log, meta).inspect_err(|err| {
        relay_log::debug!("failed to normalize log: {err}");
    })?;

    Ok(())
}

fn scrub(log: &mut Annotated<OurLog>, ctx: Context<'_>) -> Result<(), ProcessingError> {
    let pii_config = ctx
        .project_info
        .config
        .datascrubbing_settings
        .pii_config()
        .map_err(|e| ProcessingError::PiiConfigError(e.clone()))?;

    if let Some(ref config) = ctx.project_info.config.pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(log, &mut processor, ProcessingState::root())?;
    }

    if let Some(config) = pii_config {
        let mut processor = PiiProcessor::new(config.compiled());
        process_value(log, &mut processor, ProcessingState::root())?;
    }

    Ok(())
}

fn normalize(log: &mut Annotated<OurLog>, meta: &RequestMeta) -> Result<(), ProcessingError> {
    process_value(log, &mut SchemaProcessor, ProcessingState::root())?;

    let Some(log) = log.value_mut() else {
        return Err(ProcessingError::NoEventPayload);
    };

    process_attribute_types(log);
    populate_ua_fields(log, meta.user_agent(), meta.client_hints().as_deref());

    Ok(())
}

fn process_attribute_types(log: &mut OurLog) {
    let Some(attributes) = log.attributes.value_mut() else {
        return;
    };

    let attributes = attributes.iter_mut().map(|(_, attr)| attr);

    for attribute in attributes {
        use AttributeType::*;

        let Some(inner) = attribute.value_mut() else {
            continue;
        };

        match (&mut inner.value.ty, &mut inner.value.value) {
            (Annotated(Some(Boolean), _), Annotated(Some(Value::Bool(_)), _)) => (),
            (Annotated(Some(Integer), _), Annotated(Some(Value::I64(_)), _)) => (),
            (Annotated(Some(Integer), _), Annotated(Some(Value::U64(_)), _)) => (),
            (Annotated(Some(Double), _), Annotated(Some(Value::I64(_)), _)) => (),
            (Annotated(Some(Double), _), Annotated(Some(Value::U64(_)), _)) => (),
            (Annotated(Some(Double), _), Annotated(Some(Value::F64(_)), _)) => (),
            (Annotated(Some(String), _), Annotated(Some(Value::String(_)), _)) => (),
            // Note: currently the mapping to Kafka requires that invalid or unknown combinations
            // of types and values are removed from the mapping.
            //
            // Usually Relay would only modify the offending values, but for now, until there
            // is better support in the pipeline here, we need to remove the entire attribute.
            (Annotated(Some(Unknown(_)), _), _) => {
                let original = attribute.value_mut().take();
                attribute.meta_mut().add_error(ErrorKind::InvalidData);
                attribute.meta_mut().set_original_value(original);
            }
            (Annotated(Some(_), _), Annotated(Some(_), _)) => {
                let original = attribute.value_mut().take();
                attribute.meta_mut().add_error(ErrorKind::InvalidData);
                attribute.meta_mut().set_original_value(original);
            }
            (Annotated(None, _), _) | (_, Annotated(None, _)) => {
                let original = attribute.value_mut().take();
                attribute.meta_mut().add_error(ErrorKind::MissingAttribute);
                attribute.meta_mut().set_original_value(original);
            }
        }
    }
}

fn populate_ua_fields(log: &mut OurLog, user_agent: Option<&str>, client_hints: ClientHints<&str>) {
    let attributes = log.attributes.get_or_insert_with(Default::default);
    let Some(context) = BrowserContext::from_hints_or_ua(&RawUserAgentInfo {
        user_agent,
        client_hints,
    }) else {
        return;
    };

    if !attributes.contains_key("sentry.browser.name") {
        if let Some(name) = context.name.value() {
            attributes.insert(
                "sentry.browser.name".to_owned(),
                Annotated::new(Attribute::new(
                    AttributeType::String,
                    Value::String(name.to_owned()),
                )),
            );
        }
    }

    if !attributes.contains_key("sentry.browser.version") {
        if let Some(version) = context.version.value() {
            attributes.insert(
                "sentry.browser.version".to_owned(),
                Annotated::new(Attribute::new(
                    AttributeType::String,
                    Value::String(version.to_owned()),
                )),
            );
        }
    }
}
