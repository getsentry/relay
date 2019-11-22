use serde_json::Value;

enum IndexingState {
    LookingForLeftParenthesis,
    Accumulating(usize),
}

// updates a json Value at the specified path
pub fn update_json_object<'a, V: AsRef<str>>(obj: &'a mut Value, path: &[&str], val: V) {
    update_json_object_internal(obj, path, val, false)
}

fn update_json_object_internal<'a, V: AsRef<str>>(
    obj: &'a mut Value,
    path: &[&str],
    val: V,
    recursion_protection: bool,
) {
    if path.len() == 0 {
        return;
    }
    if let Value::Object(the_map) = obj {
        if path.len() == 1 {
            the_map.insert(path[0].into(), Value::String(val.as_ref().to_string()));
        } else {
            match the_map.get_mut(path[0]) {
                Some(inner) => {
                    if inner.is_object() {
                        // we have a member at the specified index and it is an object (we can insert at path)
                        update_json_object_internal(inner, &path[1..], val, false);
                    }
                }
                None => {
                    if recursion_protection {
                        //this is a bug we should NEVER be here
                        log::error!("update_value_internal, infinite recursion detected");
                    } else {
                        //nothing yet at the specified path create an object
                        the_map.insert(path[0].into(), Value::Object(serde_json::Map::new()));
                        // now we should have an object at the path, try again
                        update_json_object_internal(obj, path, val, true);
                    }
                }
            }
        }
    }
}

/// Merge two serde values
/// taken (with small changes) from stack overflow answer
/// https://stackoverflow.com/questions/47070876/how-can-i-merge-two-json-objects-with-rust
pub fn merge_vals(a: &mut Value, b: Value) {
    match (a, b) {
        //recursively merge dicts
        (a @ &mut Value::Object(_), Value::Object(b)) => {
            let a = a.as_object_mut().unwrap();
            for (k, v) in b {
                merge_vals(a.entry(k).or_insert(Value::Null), v);
            }
        }
        //fill in missing left values
        (a @ &mut Value::Null, b) => *a = b,
        //do not override existing values that are not maps
        (_a, _b) => {}
    }
}

/// Extracts indexes from a param string e.g. extracts `[String(abc),String(xyz)]` from `"sentry[abc][xyz]"`
fn get_indexes(full_string: &str) -> Vec<&str> {
    let mut ret_vals = vec![];
    let mut state = IndexingState::LookingForLeftParenthesis;
    //first iterate by byte (so we can get correct offsets)
    for (idx, by) in full_string.as_bytes().iter().enumerate() {
        match state {
            IndexingState::LookingForLeftParenthesis => {
                if by == &b'[' {
                    state = IndexingState::Accumulating(idx + 1);
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
    ret_vals
}

/// Extracts indexes from a param of the form 'sentry[XXX][...]'
pub fn get_sentry_entry_indexes(param_name: &str) -> Option<Vec<&str>> {
    if param_name.starts_with("sentry[") {
        Some(get_indexes(param_name))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_parser() {
        let examples: &[(&str, &[&str])] = &[
            ("fafdasd[a][b][33]", &["a", "b", "33"]),
            ("[23a][234][abc123]", &["23a", "234", "abc123"]),
            ("sentry[abc][123][]=SomeVal", &["abc", "123", ""]),
            ("sentry[Grüße][Jürgen][❤]", &["Grüße", "Jürgen", "❤"]),
            ("[农22历][新年][b新年c]", &["农22历", "新年", "b新年c"]),
            ("[ὈΔΥΣΣΕΎΣ][abc]", &["ὈΔΥΣΣΕΎΣ", "abc"]),
        ];

        for &(example, expected_result) in examples {
            let indexes = get_indexes(example);
            assert_eq!(&indexes[..], expected_result)
        }
    }

    #[test]
    fn test_update_value() {
        let mut val = Value::Object(serde_json::Map::new());

        update_json_object(&mut val, &["x", "y", "z"], "xx");

        insta::assert_json_snapshot!(val, @r###"
       ⋮{
       ⋮  "x": {
       ⋮    "y": {
       ⋮      "z": "xx"
       ⋮    }
       ⋮  }
       ⋮}
        "###);

        update_json_object(&mut val, &["x", "y", "k"], "kk");
        update_json_object(&mut val, &["w", ""], "w");
        update_json_object(&mut val, &["z1"], "val1");
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

        merge_vals(&mut original, modified);
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
