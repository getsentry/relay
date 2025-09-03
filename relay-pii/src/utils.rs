use std::borrow::Cow;

use hmac::{Hmac, Mac};

use relay_event_schema::processor::{
    self, FieldAttrs, Pii, ProcessValue, ProcessingResult, ProcessingState, Processor, ValueType,
    enum_set,
};
use relay_event_schema::protocol::{AsPair, Attributes, PairList};
use sha1::Sha1;

use crate::AttributeMode;

pub fn process_pairlist<P: Processor, T: ProcessValue + AsPair>(
    slf: &mut P,
    value: &mut PairList<T>,
    state: &ProcessingState,
) -> ProcessingResult {
    // View pairlists as objects just for the purpose of PII stripping (e.g. `event.tags.mykey`
    // instead of `event.tags.42.0`). For other purposes such as trimming we would run into
    // problems:
    //
    // * tag keys need to be trimmed too and therefore need to have a path

    for (idx, annotated) in value.iter_mut().enumerate() {
        if let Some(pair) = annotated.value_mut() {
            let (key, value) = pair.as_pair_mut();
            let value_type = ValueType::for_field(value);

            if let Some(key_name) = key.as_str() {
                // if the pair has no key name, we skip over it for PII stripping. It is
                // still processed with index-based path in the invocation of
                // `process_child_values`.
                let entered = state.enter_borrowed(key_name, state.inner_attrs(), value_type);
                processor::process_value(value, slf, &entered)?;
            } else {
                let entered = state.enter_index(idx, state.inner_attrs(), value_type);
                processor::process_value(value, slf, &entered)?;
            }
        }
    }

    Ok(())
}

pub fn process_attributes<P: Processor>(
    value: &mut Attributes,
    slf: &mut P,
    state: &ProcessingState,
    attribute_mode: AttributeMode,
) -> ProcessingResult {
    for (key, annotated_attribute) in value.iter_mut() {
        if let Some(attribute) = annotated_attribute.value_mut() {
            match attribute_mode {
                AttributeMode::Object => {
                    // Process normally to allow explicit selectors like $log.attributes.KEY.value to work
                    let field_value_type = ValueType::for_field(annotated_attribute);
                    let key_state =
                        state.enter_borrowed(key, state.inner_attrs(), field_value_type);
                    processor::process_value(annotated_attribute, slf, &key_state)?;
                }
                AttributeMode::ValueOnly => {
                    // For the default rules, we want Pii::True since we want them to always be scrubbed.
                    let attrs = FieldAttrs::new().pii(Pii::True);
                    let inner_value = &mut attribute.value.value;
                    let inner_value_type = ValueType::for_field(inner_value);
                    let entered =
                        state.enter_borrowed(key, Some(Cow::Borrowed(&attrs)), inner_value_type);
                    processor::process_value(inner_value, slf, &entered)?;

                    let object_value_type = enum_set!(ValueType::Object);
                    for (key, value) in attribute.other.iter_mut() {
                        let other_state = state.enter_borrowed(
                            key,
                            Some(Cow::Borrowed(&attrs)),
                            object_value_type,
                        );
                        processor::process_value(value, slf, &other_state)?;
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn hash_value(data: &[u8]) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(&[]).unwrap();
    mac.update(data);
    format!("{:X}", mac.finalize().into_bytes())
}
