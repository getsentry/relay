use relay_base_schema::events::EventType;
use relay_event_schema::protocol::{
    Csp, Event, ExpectCt, ExpectStaple, Hpkp, LenientString, Metrics, SecurityReportType,
};
use relay_protocol::Annotated;
use relay_quotas::DataCategory;

use crate::envelope::{Item, ItemType};
use crate::extractors::RequestMeta;
use crate::managed::{Counted, Quantities, RecordKeeper};
use crate::processing::ForwardContext;
use crate::processing::errors::Result;
use crate::processing::errors::errors::{Context, Expansion, SentryError, utils};
use crate::services::processor::ProcessingError;

#[derive(Debug)]
pub struct RawSecurity;

impl SentryError for RawSecurity {
    fn event_category(&self) -> DataCategory {
        DataCategory::Security
    }

    fn try_expand(items: &mut Vec<Item>, ctx: Context<'_>) -> Result<Option<Expansion<Self>>> {
        let Some(item) = utils::take_item_of_type(items, ItemType::RawSecurity) else {
            return Ok(None);
        };

        let mut metrics = Metrics::default();

        let (event, len) = event_from_security_report(item, ctx.envelope.meta())?;

        metrics.bytes_ingested_event = Annotated::new(len as u64);

        Ok(Some(Expansion {
            event: Box::new(event),
            attachments: utils::take_items_of_type(items, ItemType::Attachment),
            user_reports: utils::take_items_of_type(items, ItemType::UserReport),
            error: Self,
            metrics,
            fully_normalized: false,
        }))
    }

    fn apply_rate_limit(
        &mut self,
        _category: DataCategory,
        _limits: relay_quotas::RateLimits,
        _records: &mut RecordKeeper<'_>,
    ) -> Result<()> {
        Ok(())
    }

    fn serialize_into(self, _items: &mut Vec<Item>, _ctx: ForwardContext<'_>) -> Result<()> {
        Ok(())
    }

    fn minidump_mut(&mut self) -> Option<&mut Item> {
        None
    }
}

impl Counted for RawSecurity {
    fn quantities(&self) -> Quantities {
        Default::default()
    }
}

fn event_from_security_report(
    item: Item,
    meta: &RequestMeta,
) -> Result<(Annotated<Event>, usize), ProcessingError> {
    let len = item.len();
    let mut event = Event::default();

    let bytes = item.payload();
    let data = &bytes;
    let Some(report_type) =
        SecurityReportType::from_json(data).map_err(ProcessingError::InvalidJson)?
    else {
        return Err(ProcessingError::InvalidSecurityType(bytes));
    };

    let (apply_result, event_type) = match report_type {
        SecurityReportType::Csp => (Csp::apply_to_event(data, &mut event), EventType::Csp),
        SecurityReportType::ExpectCt => (
            ExpectCt::apply_to_event(data, &mut event),
            EventType::ExpectCt,
        ),
        SecurityReportType::ExpectStaple => (
            ExpectStaple::apply_to_event(data, &mut event),
            EventType::ExpectStaple,
        ),
        SecurityReportType::Hpkp => (Hpkp::apply_to_event(data, &mut event), EventType::Hpkp),
        SecurityReportType::Unsupported => return Err(ProcessingError::UnsupportedSecurityType),
    };

    if let Err(json_error) = apply_result {
        // logged in extract_event
        relay_log::configure_scope(|scope| {
            scope.set_extra("payload", String::from_utf8_lossy(data).into());
        });

        return Err(ProcessingError::InvalidSecurityReport(json_error));
    }

    if let Some(release) = item.sentry_release() {
        event.release = Annotated::from(LenientString(release.to_owned()));
    }

    if let Some(env) = item.sentry_environment() {
        event.environment = Annotated::from(env.to_owned());
    }

    if let Some(origin) = meta.origin() {
        event
            .request
            .get_or_insert_with(Default::default)
            .headers
            .get_or_insert_with(Default::default)
            .insert("Origin".into(), Annotated::new(origin.to_string().into()));
    }

    // Explicitly set the event type. This is required so that a `Security` item can be created
    // instead of a regular `Event` item.
    event.ty = Annotated::new(event_type);

    Ok((Annotated::new(event), len))
}
