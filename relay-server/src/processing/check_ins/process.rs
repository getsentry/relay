use crate::envelope::ContentType;
use crate::managed::Managed;
use crate::processing::check_ins::{Error, SerializedCheckIns};

/// Normalizes all check-ins using the [`relay_monitors`] module.
///
/// Individual, invalid check-ins will be discarded.
///
/// Returns the monitor slug and environment of each valid check-in
pub fn normalize(check_ins: &mut Managed<SerializedCheckIns>) -> Vec<(String, String)> {
    let scoping = check_ins.scoping();
    let mut monitors = Vec::new();

    check_ins.retain(
        |check_ins| &mut check_ins.check_ins,
        |check_in, _| {
            let payload = check_in.payload();
            let result = relay_monitors::process_check_in(&payload, scoping.project_id)
                .inspect_err(|err| {
                    relay_log::debug!(
                        error = err as &dyn std::error::Error,
                        "dropped invalid monitor check-in"
                    )
                })?;

            check_in.set_routing_hint(result.routing_hint);
            check_in.set_payload(ContentType::Json, result.payload);

            monitors.push((result.monitor_slug, result.environment));
            Ok::<_, Error>(())
        },
    );

    monitors
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::envelope::{Envelope, Item, ItemType};
    use relay_quotas::DataCategory;

    use crate::managed::ManagedTestHandle;
    use crate::services::outcome::{DiscardReason, Outcome};

    use super::*;

    fn check_in_item(payload: &str) -> Item {
        let mut item = Item::new(ItemType::CheckIn);
        item.set_payload(ContentType::Json, payload.to_owned());
        item
    }

    fn managed(payloads: &[&str]) -> (Managed<SerializedCheckIns>, ManagedTestHandle) {
        let bytes = Bytes::from(
            "{\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}".to_owned(),
        );
        let headers = Envelope::parse_bytes(bytes).unwrap().headers().clone();
        let check_ins = payloads.iter().copied().map(check_in_item).collect();

        Managed::for_test(SerializedCheckIns { headers, check_ins }).build()
    }

    #[test]
    fn test_returned_monitors_line_up_with_the_kept_check_ins() {
        let (mut check_ins, mut handle) = managed(&[
            r#"{"check_in_id":"a460c25ff2554577b920fcfacae4e5eb","monitor_slug":"first","status":"ok"}"#,
            // Dropped: an empty slug is rejected.
            r#"{"check_in_id":"a460c25ff2554577b920fcfacae4e5eb","monitor_slug":"","status":"ok"}"#,
            r#"{"check_in_id":"a460c25ff2554577b920fcfacae4e5eb","monitor_slug":"second","environment":"prod","status":"ok"}"#,
            // Dropped: not valid json.
            r#"{"#,
            r#"{"check_in_id":"a460c25ff2554577b920fcfacae4e5eb","monitor_slug":"third","status":"ok"}"#,
        ]);

        let monitors = normalize(&mut check_ins);

        assert_eq!(
            monitors,
            vec![
                // The absent environments are normalized to "production", the same value the
                // routing key is built from.
                ("first".to_owned(), "production".to_owned()),
                ("second".to_owned(), "prod".to_owned()),
                ("third".to_owned(), "production".to_owned()),
            ]
        );
        assert_eq!(monitors.len(), check_ins.check_ins.len());

        drop(check_ins);
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::InvalidCheckIn),
            DataCategory::Monitor,
            1,
        );
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::InvalidJson),
            DataCategory::Monitor,
            1,
        );
        handle.assert_internal_outcome(DataCategory::Monitor, 3);
    }

    #[test]
    fn test_no_monitors_when_every_check_in_is_invalid() {
        let (mut check_ins, mut handle) =
            managed(&[r#"{"#, r#"{"monitor_slug":"","status":"ok"}"#]);

        let monitors = normalize(&mut check_ins);

        assert!(monitors.is_empty());
        assert!(check_ins.check_ins.is_empty());

        drop(check_ins);
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::InvalidJson),
            DataCategory::Monitor,
            1,
        );
        handle.assert_outcome(
            &Outcome::Invalid(DiscardReason::InvalidCheckIn),
            DataCategory::Monitor,
            1,
        );
        // Dropping an emptied `Managed` still reports, with nothing left to report on.
        handle.assert_internal_outcome(DataCategory::Monitor, 0);
    }
}
