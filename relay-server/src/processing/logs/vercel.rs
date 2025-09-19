use relay_event_schema::protocol::{OurLog, OurLogHeader, VercelLog};
use relay_ourlogs::vercel_to_sentry_log;
use relay_quotas::DataCategory;

use crate::envelope::{ContainerItems, ContentType, Item, ItemType, WithHeader};
use crate::managed::RecordKeeper;
use crate::processing::logs::Error;
use crate::services::outcome::DiscardReason;

pub fn expand_into(
    result: &mut ContainerItems<OurLog>,
    records: &mut RecordKeeper<'_>,
    items: Vec<Item>,
) {
    for item in items {
        debug_assert_eq!(item.ty(), &ItemType::VercelLog);

        let vercel_logs = match parse_vercel_logs(&item) {
            Ok(logs) => logs,
            Err(err) => {
                records.reject_err(err, &item);
                continue;
            }
        };

        // Undo all the base item quantities, as they will be completely taken over by the parsed
        // contents, which contains an arbitrary amount of items (even 0).
        for (category, quantity) in item.quantities() {
            records.modify_by(category, -(quantity as isize));
        }

        for vercel_log in vercel_logs {
            let log = vercel_to_sentry_log(vercel_log);
            let byte_size = relay_ourlogs::calculate_size(&log);

            records.modify_by(DataCategory::LogItem, 1);
            records.modify_by(DataCategory::LogByte, byte_size as isize);

            result.push(WithHeader {
                header: Some(OurLogHeader {
                    byte_size: Some(byte_size),
                    other: Default::default(),
                }),
                value: log.into(),
            });
        }
    }
}

fn parse_vercel_logs(item: &Item) -> Result<Vec<VercelLog>, Error> {
    match item.content_type() {
        Some(&ContentType::Json) => {
            // Vercel logs are expected to be sent as a JSON array
            serde_json::from_slice::<Vec<VercelLog>>(&item.payload()).map_err(|e| {
                relay_log::debug!(
                    error = &e as &dyn std::error::Error,
                    "Failed to parse Vercel logs as JSON array"
                );
                Error::Invalid(DiscardReason::InvalidJson)
            })
        }
        _ => Err(Error::Invalid(DiscardReason::ContentType)),
    }
}
