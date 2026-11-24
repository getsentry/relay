//! IntegrationItem APIs and accounting.

use super::{ContentType, IntegrationItem};
use crate::envelope::RejectionQuantities;
use crate::integrations::Integration;
use crate::managed::{Counted, Quantities};
use relay_quotas::DataCategory;
use smallvec::smallvec;

impl IntegrationItem {
    /// Returns the integration named by the content-type header, if present and valid.
    pub fn integration_header(&self) -> Option<Integration> {
        match self.data().content_type() {
            Some(ContentType::Integration(integration)) => Some(integration),
            _ => None,
        }
    }

    /// Returns the integration, asserting that internally constructed items are valid.
    pub fn integration(&self) -> Option<Integration> {
        let integration = self.integration_header();
        debug_assert!(
            integration.is_some(),
            "integration item, but no integration content type"
        );
        integration
    }
}

impl Counted for IntegrationItem {
    fn quantities(&self) -> Quantities {
        match self.integration_header() {
            None => Quantities::new(),
            Some(Integration::Errors(_)) => smallvec![(DataCategory::Error, 1)],
            Some(Integration::Logs(_)) => smallvec![
                (DataCategory::LogItem, 1),
                (DataCategory::LogByte, self.data().len().max(1)),
            ],
            Some(Integration::Spans(_)) => {
                smallvec![(DataCategory::Span, 1), (DataCategory::SpanIndexed, 1)]
            }
        }
    }
}

impl RejectionQuantities for IntegrationItem {
    fn rejected_quantities(&self) -> Quantities {
        match self.integration_header() {
            Some(Integration::Errors(_)) => Quantities::new(),
            _ => self.quantities(),
        }
    }
}
