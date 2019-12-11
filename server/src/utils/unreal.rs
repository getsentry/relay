use symbolic::unreal::{Unreal4Crash, Unreal4Error, Unreal4FileType};

//use semaphore_general::protocol::Event;
//use semaphore_general::types::Annotated;

use crate::constants::{ITEM_NAME_BREADCRUMBS1, ITEM_NAME_BREADCRUMBS2, ITEM_NAME_EVENT};
use crate::envelope::{AttachmentType, ContentType, Envelope, Item, ItemType};

pub fn expand_if_unreal_envelope(envelope: &mut Envelope) -> Result<&mut Envelope, Unreal4Error> {
    if let Some(unreal_item) = envelope.take_item_by(|item| item.ty() == ItemType::UnrealReport) {
        let payload = unreal_item.payload();
        let crash = Unreal4Crash::parse(&payload)?;

        //TODO handle context
        let _context = crash.context();

        for file in crash.files() {
            let file_name = file.name();
            match file.ty() {
                Unreal4FileType::Minidump => {
                    let mut item = Item::new(ItemType::Attachment);
                    item.set_payload(ContentType::OctetStream, file.data());
                    item.set_filename(file_name);
                    item.set_name(file_name);
                    item.set_attachment_type(AttachmentType::Minidump);
                    envelope.add_item(item);
                }
                Unreal4FileType::AppleCrashReport => {
                    let mut item = Item::new(ItemType::Attachment);
                    item.set_payload(ContentType::OctetStream, file.data());
                    item.set_filename(file_name);
                    item.set_name(file_name);
                    item.set_attachment_type(AttachmentType::AppleCrashReport);
                    envelope.add_item(item);
                }
                Unreal4FileType::Log => {
                    //TODO process log
                }
                Unreal4FileType::Config => {
                    // TODO see what we need to do.
                }
                Unreal4FileType::Context => {
                    // nothing to do context already handled
                }
                Unreal4FileType::Unknown => {
                    if file_name == ITEM_NAME_EVENT {
                        // this is a MsgPack event
                        let mut item = Item::new(ItemType::Attachment);
                        item.set_payload(ContentType::MsgPack, file.data());
                        item.set_filename(file_name);
                        item.set_name(file_name);
                        item.set_attachment_type(AttachmentType::MsgpackEvent);
                        envelope.add_item(item);
                    } else if file_name == ITEM_NAME_BREADCRUMBS1
                        || file_name == ITEM_NAME_BREADCRUMBS2
                    {
                        // this is a MsgPack breadcrumbs attachment
                        let mut item = Item::new(ItemType::Attachment);
                        item.set_payload(ContentType::MsgPack, file.data());
                        item.set_filename(file_name);
                        item.set_name(file_name);
                        item.set_attachment_type(AttachmentType::Breadcrumbs);
                        envelope.add_item(item);
                    } else {
                        // some unknown attachment just add it as is
                        let mut item = Item::new(ItemType::Attachment);
                        item.set_payload(ContentType::OctetStream, file.data());
                        item.set_filename(file_name);
                        item.set_name(file_name);
                        item.set_attachment_type(AttachmentType::Attachment);
                        envelope.add_item(item);
                    }
                }
            }
        }
    }
    Ok(envelope)
}
