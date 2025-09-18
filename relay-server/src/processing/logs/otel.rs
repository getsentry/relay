use prost::Message as _;
use relay_event_schema::protocol::{OurLog, OurLogHeader};
use relay_ourlogs::otel_logs::LogsData;
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
        debug_assert_eq!(item.ty(), &ItemType::OtelLogsData);

        let logs = match parse_logs_data(&item) {
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

        for resource_logs in logs.resource_logs {
            let resource = resource_logs.resource.as_ref();
            for scope_logs in resource_logs.scope_logs {
                let scope = scope_logs.scope.as_ref();
                for log_record in scope_logs.log_records {
                    let log = relay_ourlogs::otel_to_sentry_log(log_record, resource, scope);
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
    }
}

fn parse_logs_data(item: &Item) -> Result<LogsData, Error> {
    match item.content_type() {
        Some(&ContentType::Json) => serde_json::from_slice(&item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as JSON"
            );
            Error::Invalid(DiscardReason::InvalidJson)
        }),
        Some(&ContentType::Protobuf) => LogsData::decode(item.payload()).map_err(|e| {
            relay_log::debug!(
                error = &e as &dyn std::error::Error,
                "Failed to parse logs data as protobuf"
            );
            Error::Invalid(DiscardReason::InvalidProtobuf)
        }),
        _ => Err(Error::Invalid(DiscardReason::ContentType)),
    }
}
