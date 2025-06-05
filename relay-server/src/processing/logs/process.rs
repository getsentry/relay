use relay_event_schema::protocol::OurLog;
use relay_ourlogs::OtelLog;
use relay_protocol::Annotated;
use smallvec::SmallVec;

use crate::envelope::{ContainerItems, ContainerParseError, Item, ItemContainer};
use crate::processing::logs::{EinsLog, ZweiLog};
use crate::processing::{Context, Managed};
use crate::services::processor::ProcessingError;

pub fn expand(logs: Managed<EinsLog>) -> Result<Managed<ZweiLog>, ProcessingError> {
    // TODO: if we have to drop singular elements here, how do we keep track which ones were
    // dropped?

    logs.try_map(|logs, records| {
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

        Ok(ZweiLog {
            headers: logs.headers,
            logs: all_logs,
        })
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

pub fn process(logs: &mut Managed<ZweiLog>, _ctx: Context<'_>) -> Result<(), ProcessingError> {
    // TODO: some signature that allows to directly retain items seems thinkable
    logs.modify(|logs, records| {
        logs.logs
            .retain_mut(|log| records.or_default(process_log(log).map(|_| true), &*log));
    });

    Ok(())
}

fn process_log(log: &mut Annotated<OurLog>) -> Result<(), ProcessingError> {
    // scrub(log).inspect_err(|err| {
    //     relay_log::debug!("failed to scrub pii from log: {err}");
    // })?;
    //
    // normalize(log).inspect_err(|err| {
    //     relay_log::debug!("failed to normalize log: {err}");
    // })?;

    Ok(())
}

fn normalize(_log: &mut Annotated<OurLog>) -> Result<(), ProcessingError> {
    todo!()
}

fn scrub(_log: &mut Annotated<OurLog>) -> Result<(), ProcessingError> {
    todo!()
}
