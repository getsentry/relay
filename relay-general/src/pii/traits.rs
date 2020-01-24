use crate::processor::PathItem;

#[derive(Debug, Copy, Clone)]
pub struct PiiAttrs {
    pub whitelist: &'static [PathItem<'static>],
    pub blacklist: &'static [PathItem<'static>],
}

impl Default for PiiAttrs {
    fn default() -> PiiAttrs {
        PiiAttrs {
            whitelist: &[],
            blacklist: &[],
        }
    }
}

impl PiiAttrs {
    pub fn should_strip_pii(&self, field: &PathItem<'_>) -> Option<bool> {
        if self.blacklist.contains(field) {
            Some(false)
        } else if self.whitelist.contains(field) {
            Some(true)
        } else {
            None
        }
    }
}
