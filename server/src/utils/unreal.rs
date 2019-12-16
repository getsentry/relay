use symbolic::unreal::{Unreal4Crash, Unreal4Error, Unreal4File, Unreal4FileType};

use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};

fn insert_attachment_from_unreal_file(
    envelope: &mut Envelope,
    file: Unreal4File,
    content_type: ContentType,
    attachment_type: AttachmentType,
) {
    let mut item = Item::new(ItemType::Attachment);
    item.set_name(file.name());
    item.set_filename(file.name());
    item.set_payload(content_type, file.data());
    item.set_attachment_type(attachment_type);
    envelope.add_item(item);
}

/// Expands Unreal 4 items inside an envelope
/// If the envelope dose NOT contain an UnrealReport item it doesn't do anything
/// If the envelope DOES contain a UnrealReport item it removes it from the envelope and
/// inserts new items for each of its contents.
/// After this the EventProcessor should be able to process the envelope the same way
/// it processes a minidump.
pub fn expand_if_unreal_envelope(envelope: &mut Envelope) -> Result<&mut Envelope, Unreal4Error> {
    if let Some(unreal_item) = envelope.take_item_by(|item| item.ty() == ItemType::UnrealReport) {
        let payload = unreal_item.payload();
        let crash = Unreal4Crash::parse(&payload)?;

        for file in crash.files() {
            let file_name = file.name();
            match file.ty() {
                Unreal4FileType::Minidump => {
                    insert_attachment_from_unreal_file(
                        envelope,
                        file,
                        ContentType::OctetStream,
                        AttachmentType::Minidump,
                    );
                }
                Unreal4FileType::AppleCrashReport => {
                    insert_attachment_from_unreal_file(
                        envelope,
                        file,
                        ContentType::OctetStream,
                        AttachmentType::AppleCrashReport,
                    );
                }
                Unreal4FileType::Log => insert_attachment_from_unreal_file(
                    envelope,
                    file,
                    ContentType::OctetStream,
                    AttachmentType::UnrealLogs,
                ),
                Unreal4FileType::Config => {
                    // not used.
                }
                Unreal4FileType::Context => {
                    // Unreal 4 context attachment
                    insert_attachment_from_unreal_file(
                        envelope,
                        file,
                        ContentType::OctetStream,
                        AttachmentType::UnrealContext,
                    );
                }
                Unreal4FileType::Unknown => {
                    if file_name == ITEM_NAME_EVENT {
                        // this is a MsgPack event
                        insert_attachment_from_unreal_file(
                            envelope,
                            file,
                            ContentType::MsgPack,
                            AttachmentType::MsgpackEvent,
                        );
                    } else if file_name == ITEM_NAME_BREADCRUMBS1
                        || file_name == ITEM_NAME_BREADCRUMBS2
                    {
                        // this is a MsgPack breadcrumbs attachment
                        insert_attachment_from_unreal_file(
                            envelope,
                            file,
                            ContentType::MsgPack,
                            AttachmentType::Breadcrumbs,
                        );
                    } else {
                        // some unknown attachment just add it as is
                        insert_attachment_from_unreal_file(
                            envelope,
                            file,
                            ContentType::OctetStream,
                            AttachmentType::Attachment,
                        );
                    }
                }
            }
        }
    }
    Ok(envelope)
}
