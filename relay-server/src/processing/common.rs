use crate::Envelope;
use crate::managed::{Managed, Rejected};
use crate::processing::check_ins::CheckInsProcessor;
use crate::processing::logs::LogsProcessor;
use crate::processing::sessions::SessionsProcessor;
use crate::processing::spans::SpansProcessor;
use crate::processing::trace_metrics::TraceMetricsProcessor;
use crate::processing::{Forward, Nothing, Processor};

#[cfg(feature = "processing")]
use std::collections::HashMap;

#[cfg(feature = "processing")]
use relay_event_schema::protocol::Attributes;
#[cfg(feature = "processing")]
use relay_protocol::{Annotated, IntoValue, MetaTree};
#[cfg(feature = "processing")]
use sentry_protos::snuba::v1::{AnyValue, any_value};
#[cfg(feature = "processing")]
use serde::Serialize;

macro_rules! outputs {
    ($($variant:ident => $ty:ty,)*) => {
        /// All known [`Processor`] outputs.
        #[derive(Debug)]
        pub enum Outputs {
            $(
                $variant(<$ty as Processor>::Output)
            ),*
        }

        impl Forward for Outputs {
            fn serialize_envelope(self) -> Result<Managed<Box<Envelope>>, Rejected<()>> {
                match self {
                    $(
                        Self::$variant(output) => output.serialize_envelope()
                    ),*
                }
            }

            #[cfg(feature = "processing")]
            fn forward_store(
                self,
                s: &relay_system::Addr<crate::services::store::Store>,
            ) -> Result<(), Rejected<()>> {
                match self {
                    $(
                        Self::$variant(output) => output.forward_store(s)
                    ),*
                }
            }
        }

        $(
            impl From<<$ty as Processor>::Output> for Outputs {
                fn from(value: <$ty as Processor>::Output) -> Self {
                    Self::$variant(value)
                }
            }
        )*
    };
}

outputs!(
    CheckIns => CheckInsProcessor,
    Logs => LogsProcessor,
    TraceMetrics => TraceMetricsProcessor,
    Spans => SpansProcessor,
    Sessions => SessionsProcessor,
);

impl From<Nothing> for Outputs {
    fn from(value: Nothing) -> Self {
        match value {}
    }
}

/// Shared metadata extraction utilities for trace items.
///
/// This module contains common functionality for extracting metadata from
/// annotated structures and converting them to TraceItem attributes.
#[cfg(feature = "processing")]
pub mod meta_extraction {
    use super::*;

    /// Represents metadata extracted from Relay's annotated model.
    ///
    /// This struct holds metadata about processing errors, transformations, and other
    /// information that occurred during processing of the original payload.
    ///
    /// The attribute metadata itself is serialized as a JSON string.
    #[derive(Debug, Serialize)]
    pub struct AttributeMeta {
        /// Meta as it was extracted from Relay's annotated model.
        pub meta: MetaTree,
    }

    impl AttributeMeta {
        /// Converts the metadata to an AnyValue for TraceItem attributes.
        ///
        /// Returns None if the metadata is empty, otherwise serializes the metadata
        /// to JSON and wraps it in a StringValue.
        pub fn to_any_value(&self) -> Option<AnyValue> {
            if self.meta.is_empty() {
                return None;
            }

            let s = serde_json::to_string(self)
                .inspect_err(|err| {
                    relay_log::error!(
                        error = err as &dyn std::error::Error,
                        "attribute meta serialization failed"
                    )
                })
                .ok()?;

            Some(AnyValue {
                value: Some(any_value::Value::StringValue(s)),
            })
        }
    }

    /// Extracts TraceItem meta attributes from any structure that implements IntoValue.
    ///
    /// The implementation piggy backs on [`IntoValue::extract_child_meta`],
    /// a lighter implementation using a [`relay_event_schema::processor::Processor`]
    /// which removes the meta instead of cloning.
    ///
    /// All extracted metadata is converted into [`Attributes`] compatible values,
    /// by building a metadata representation for each top level field and attribute,
    /// serializing the result into JSON and building an appropriate metadata key.
    ///
    /// The schema for metadata keys follows the format `sentry._meta.fields.{key}`,
    /// for attributes respectively `sentry._meta.fields.attributes.{key}`.
    pub fn extract_meta_attributes<T: IntoValue>(
        item: &T,
        attributes: &Annotated<Attributes>,
    ) -> HashMap<String, AnyValue> {
        let mut meta = IntoValue::extract_child_meta(item);
        // Attributes are the only 'nested' meta we allow.
        let attributes_meta = meta.remove("attributes");

        let mut result = HashMap::with_capacity(
            meta.len()
                + attributes_meta.as_ref().map_or(0, size_of_meta_tree)
                + attributes.value().map_or(0, |a| a.0.len()),
        );

        for (key, meta) in meta {
            let attr = AttributeMeta { meta };
            if let Some(value) = attr.to_any_value() {
                let key = format!("sentry._meta.fields.{key}");
                result.insert(key, value);
            }
        }

        let Some(mut attributes_meta) = attributes_meta else {
            return result;
        };

        for (key, meta) in std::mem::take(&mut attributes_meta.children) {
            let attr = AttributeMeta { meta };
            if let Some(value) = attr.to_any_value() {
                let key = format!("sentry._meta.fields.attributes.{key}");
                result.insert(key, value);
            }
        }

        // The `attributes` field itself can have metadata attached,
        // we already took out all the metadata of the children, so now just emit
        // the remaining metadata on the `attributes`.
        let meta = AttributeMeta {
            meta: attributes_meta,
        };
        if let Some(value) = meta.to_any_value() {
            result.insert("sentry._meta.fields.attributes".to_owned(), value);
        }

        result
    }

    /// Calculates the immediate size of the meta tree passed in.
    ///
    /// This only counts non empty meta elements of the passed in meta tree and its children,
    /// it does not recursively traverse the children.
    pub fn size_of_meta_tree(meta: &MetaTree) -> usize {
        let mut size = 0;

        if !meta.meta.is_empty() {
            size += 1;
        }
        for meta in meta.children.values() {
            if !meta.meta.is_empty() {
                size += 1;
            }
        }

        size
    }
}
