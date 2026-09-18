use crate::processing::attachments::{Error, SerializedAttachments};

/// Checks that the attachment has an event ID.
pub fn validate(attachments: &SerializedAttachments) -> Result<(), Error> {
    if attachments.headers.event_id().is_none() {
        return Err(Error::NoEventId);
    }

    Ok(())
}
