use std::ops::{Deref, DerefMut};

use crate::protocol::Measurements;
use crate::types::{Annotated, Error, FromValue, Object, Value};

/// A map of breakdowns.
/// Breakdowns may be available on any event type. A breakdown are product-defined measurement values
/// generated by the client, or materialized during ingestion. For example, for transactions, we may
/// emit span operation breakdowns based on the attached span data.
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
pub struct Breakdowns(pub Object<Measurements>);

impl Breakdowns {
    pub fn is_valid_breakdown_name(name: &str) -> bool {
        !name.is_empty()
            && name.starts_with(|c| matches!(c, 'a'..='z' | 'A'..='Z'))
            && name
                .chars()
                .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '.'))
    }
}

impl FromValue for Breakdowns {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        let mut processing_errors = Vec::new();

        let mut breakdowns = Object::from_value(value).map_value(|breakdowns| {
            let breakdowns = breakdowns
                .into_iter()
                .filter_map(|(name, object)| {
                    let name = name.trim();

                    if Breakdowns::is_valid_breakdown_name(name) {
                        return Some((name.into(), object));
                    } else {
                        processing_errors.push(Error::invalid(format!(
                            "breakdown name '{}' can contain only characters a-z0-9._",
                            name
                        )));
                    }

                    None
                })
                .collect();

            Self(breakdowns)
        });

        for error in processing_errors {
            breakdowns.meta_mut().add_error(error);
        }

        breakdowns
    }
}

impl Deref for Breakdowns {
    type Target = Object<Measurements>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Breakdowns {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
