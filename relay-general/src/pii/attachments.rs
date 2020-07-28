use crate::pii::CompiledPiiConfig;
use crate::processor::{ProcessingState, ValueType};

lazy_static::lazy_static! {
    // TODO: This could be const
    static ref ATTACHMENT_STATE: ProcessingState<'static> = ProcessingState::root()
        .enter_static("attachments", None, None);
}

fn plain_attachment_state(filename: &str) -> ProcessingState<'_> {
    ATTACHMENT_STATE.enter_borrowed(filename, None, Some(ValueType::Binary))
}

pub struct PiiAttachmentsProcessor<'a> {
    compiled_config: &'a CompiledPiiConfig,
}

impl<'a> PiiAttachmentsProcessor<'a> {
    pub fn new(compiled_config: &'a CompiledPiiConfig) -> PiiAttachmentsProcessor<'a> {
        // this constructor needs to be cheap... a new PiiProcessor is created for each event. Move
        // any init logic into CompiledPiiConfig::new.
        PiiAttachmentsProcessor { compiled_config }
    }

    pub fn scrub_attachment_bytes(&self, filename: &str, data: &mut [u8]) {
        let state = plain_attachment_state(filename);

        for (selector, rules) in &self.compiled_config.applications {
            if state.path().matches_selector(&selector) {
                for rule in rules {
                    unimplemented!();
                }
            }
        }
    }
}
