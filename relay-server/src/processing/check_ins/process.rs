use crate::envelope::ContentType;
use crate::managed::Managed;
use crate::processing::check_ins::{Error, SerializedCheckIns};

/// Normalizes all check-ins using the [`relay_monitors`] module.
///
/// Individual, invalid check-ins will be discarded.
pub fn normalize(check_ins: &mut Managed<SerializedCheckIns>) {
    let scoping = check_ins.scoping();

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

            Ok::<_, Error>(())
        },
    );
}
