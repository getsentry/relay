use serde_json::Value;

enum IndexingState {
    LookingForLeftParenthesis,
    Accumulating(usize),
    Starting,
}

/// Updates a json Value at the specified path.
pub fn update_nested_value<V>(target: &mut Value, path: &[&str], value: V)
where
    V: Into<String>,
{
    let map = match target {
        Value::Object(map) => map,
        _ => return,
    };

    let (key, rest) = match path.split_first() {
        Some(tuple) => tuple,
        None => return,
    };

    let entry = map.entry(key.to_owned());

    if rest.is_empty() {
        entry.or_insert_with(|| Value::String(value.into()));
    } else {
        let sub_object = entry.or_insert_with(|| Value::Object(Default::default()));
        update_nested_value(sub_object, rest, value);
    }
}

/// Merge two serde values.
///
/// Taken (with small changes) from stack overflow answer:
/// https://stackoverflow.com/questions/47070876/how-can-i-merge-two-json-objects-with-rust
pub fn merge_values(a: &mut Value, b: Value) {
    match (a, b) {
        //recursively merge dicts
        (a @ &mut Value::Object(_), Value::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                merge_values(a.entry(k).or_insert(Value::Null), v);
            }
        }
        //fill in missing left values
        (a @ &mut Value::Null, b) => *a = b,
        //do not override existing values that are not maps
        (_a, _b) => {}
    }
}

/// Extracts indexes from a param string e.g. extracts `[String(abc),String(xyz)]` from `"sentry[abc][xyz]"`
fn get_indexes(full_string: &str) -> Result<Vec<&str>, ()> {
    let mut ret_vals = vec![];
    let mut state = IndexingState::Starting;
    //first iterate by byte (so we can get correct offsets)
    for (idx, by) in full_string.as_bytes().iter().enumerate() {
        match state {
            IndexingState::Starting => {
                if by == &b'[' {
                    state = IndexingState::Accumulating(idx + 1)
                }
            }
            IndexingState::LookingForLeftParenthesis => {
                if by == &b'[' {
                    state = IndexingState::Accumulating(idx + 1);
                } else if by == &b'=' {
                    return Ok(ret_vals);
                } else {
                    return Err(());
                }
            }
            IndexingState::Accumulating(start_idx) => {
                if by == &b']' {
                    let slice = &full_string[start_idx..idx];
                    ret_vals.push(slice);
                    state = IndexingState::LookingForLeftParenthesis;
                }
            }
        }
    }
    Ok(ret_vals)
}

/// Extracts indexes from a param of the form 'sentry[XXX][...]'
pub fn get_sentry_entry_indexes(param_name: &str) -> Option<Vec<&str>> {
    if param_name.starts_with("sentry[") {
        get_indexes(param_name).ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_parser() {
        let examples: &[(&str, Option<&[&str]>)] = &[
            ("fafdasd[a][b][33]", Some(&["a", "b", "33"])),
            ("fafdasd[a]b[33]", None),
            ("fafdasd[a][b33]xx", None),
            ("[23a][234][abc123]", Some(&["23a", "234", "abc123"])),
            ("sentry[abc][123][]=SomeVal", Some(&["abc", "123", ""])),
            ("sentry[Grüße][Jürgen][❤]", Some(&["Grüße", "Jürgen", "❤"])),
            (
                "[农22历][新年][b新年c]",
                Some(&["农22历", "新年", "b新年c"]),
            ),
            ("[ὈΔΥΣΣΕΎΣ][abc]", Some(&["ὈΔΥΣΣΕΎΣ", "abc"])),
        ];

        for &(example, expected_result) in examples {
            let indexes = get_indexes(example).ok();
            assert_eq!(indexes, expected_result.map(|vec| vec.into()));
        }
    }

    #[test]
    fn test_update_value() {
        let mut val = Value::Object(serde_json::Map::new());

        update_nested_value(&mut val, &["x", "y", "z"], "xx");

        insta::assert_json_snapshot!(val, @r###"
       ⋮{
       ⋮  "x": {
       ⋮    "y": {
       ⋮      "z": "xx"
       ⋮    }
       ⋮  }
       ⋮}
        "###);

        update_nested_value(&mut val, &["x", "y", "k"], "kk");
        update_nested_value(&mut val, &["w", ""], "w");
        update_nested_value(&mut val, &["z1"], "val1");
        insta::assert_json_snapshot!(val, @r###"
       ⋮{
       ⋮  "w": {
       ⋮    "": "w"
       ⋮  },
       ⋮  "x": {
       ⋮    "y": {
       ⋮      "k": "kk",
       ⋮      "z": "xx"
       ⋮    }
       ⋮  },
       ⋮  "z1": "val1"
       ⋮}
        "###);
    }

    #[test]
    fn test_merge_vals() {
        let mut original = serde_json::json!({
            "k1": "v1",
            "k2": {
                "k3": "v3",
                "k4": "v4"
            },
            "k5": [ 1,2,3]
        });

        let modified = serde_json::json!({
            "k1": "v1bis",
            "k2": {
                "k4": "v4bis",
                "k4-1": "v4-1"
            },
            "k6": "v6"
        });

        merge_values(&mut original, modified);
        insta::assert_json_snapshot!(original, @r###"
       ⋮{
       ⋮  "k1": "v1",
       ⋮  "k2": {
       ⋮    "k3": "v3",
       ⋮    "k4": "v4",
       ⋮    "k4-1": "v4-1"
       ⋮  },
       ⋮  "k5": [
       ⋮    1,
       ⋮    2,
       ⋮    3
       ⋮  ],
       ⋮  "k6": "v6"
       ⋮}
        "###);
    }
}
