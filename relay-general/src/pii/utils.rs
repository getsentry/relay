use std::cmp;

use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::{Sha256, Sha512};

use crate::pii::{HashAlgorithm};
use crate::processor::{process_value, ProcessValue, ProcessingState, Processor, ValueType};
use crate::protocol::{AsPair, PairList};
use crate::types::ProcessingResult;

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
            if let Some(ref key_name) = key.as_str() {
                // if the pair has no key name, we skip over it for PII stripping. It is
                // still processed with index-based path in the invocation of
                // `process_child_values`.
                process_value(
                    value,
                    slf,
                    &state.enter_borrowed(
                        key_name,
                        state.inner_attrs(),
                        ValueType::for_field(value),
                    ),
                )?;
            } else {
                process_value(
                    value,
                    slf,
                    &state.enter_index(idx, state.inner_attrs(), ValueType::for_field(value)),
                )?;
            }
        }
    }

    Ok(())
}


pub fn in_range(range: (Option<i32>, Option<i32>), pos: usize, len: usize) -> bool {
    fn get_range_index(idx: Option<i32>, len: usize, default: usize) -> usize {
        match idx {
            None => default,
            Some(idx) if idx < 0 => len.saturating_sub(-idx as usize),
            Some(idx) => cmp::min(idx as usize, len),
        }
    }

    let start = get_range_index(range.0, len, 0);
    let end = get_range_index(range.1, len, len);
    pos >= start && pos < end
}


pub fn hash_value(algorithm: HashAlgorithm, data: &[u8], key: Option<&str>) -> String {
    let key = key.unwrap_or("");
    macro_rules! hmac {
        ($ty:ident) => {{
            let mut mac = Hmac::<$ty>::new_varkey(key.as_bytes()).unwrap();
            mac.input(data);
            format!("{:X}", mac.result().code())
        }};
    }
    match algorithm {
        HashAlgorithm::HmacSha1 => hmac!(Sha1),
        HashAlgorithm::HmacSha256 => hmac!(Sha256),
        HashAlgorithm::HmacSha512 => hmac!(Sha512),
    }
}
