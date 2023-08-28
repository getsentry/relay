use hmac::{Hmac, Mac};
use relay_event_schema::processor::{
    self, ProcessValue, ProcessingResult, ProcessingState, Processor, ValueType,
};
use relay_event_schema::protocol::{AsPair, PairList};
use sha1::Sha1;

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
        if let Some(ref mut pair) = annotated.value_mut() {
            let (ref mut key, ref mut value) = pair.as_pair_mut();
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

pub fn hash_value(data: &[u8]) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(&[]).unwrap();
    mac.update(data);
    format!("{:X}", mac.finalize().into_bytes())
}
