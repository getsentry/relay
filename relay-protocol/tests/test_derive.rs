#![cfg(feature = "derive")]

use relay_protocol::{Annotated, Array, Empty, Error, IntoValue, Object, Value};
use similar_asserts::assert_eq;

#[test]
fn test_unsigned_integers() {
    assert_eq!(Annotated::new(1u64), Annotated::from_json("1").unwrap(),);

    assert_eq!(
        Annotated::from_error(Error::expected("an unsigned integer"), Some(Value::I64(-1))),
        Annotated::<u64>::from_json("-1").unwrap(),
    );

    // Floats are rounded implicitly
    assert_eq!(Annotated::new(4u64), Annotated::from_json("4.2").unwrap(),);
}

#[test]
fn test_signed_integers() {
    assert_eq!(Annotated::new(1i64), Annotated::from_json("1").unwrap(),);

    assert_eq!(Annotated::new(-1i64), Annotated::from_json("-1").unwrap(),);

    // Floats are rounded implicitly
    assert_eq!(Annotated::new(4u64), Annotated::from_json("4.2").unwrap(),);
}

#[test]
fn test_floats() {
    assert_eq!(Annotated::new(1f64), Annotated::from_json("1").unwrap(),);

    assert_eq!(Annotated::new(-1f64), Annotated::from_json("-1").unwrap(),);

    assert_eq!(Annotated::new(4.2f64), Annotated::from_json("4.2").unwrap(),);
}

#[test]
fn test_skip_array_never() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "never")]
        items: Annotated<Array<String>>,
    }

    // "never" always serializes
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":null}"#);
}

#[test]
fn test_skip_array_null() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null")]
        items: Annotated<Array<String>>,
    }

    // "null" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "null" does not apply to empty
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[]}"#);
}

#[test]
fn test_skip_array_null_deep() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null_deep")]
        items: Annotated<Array<String>>,
    }

    // "null_deep" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "null_deep" applies to nested null
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![Annotated::default()]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[]}"#);

    // "null_deep" does not apply to nested empty
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![Annotated::new(String::new())]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
}

#[test]
fn test_skip_array_empty() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "empty")]
        items: Annotated<Array<String>>,
    }

    // "empty" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty" applies to empty
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty" -> "never" nested
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![Annotated::default()]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);

    // "empty" does not apply to nested empty
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![Annotated::new(String::new())]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
}

#[test]
fn test_skip_array_empty_deep() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "empty_deep")]
        items: Annotated<Array<String>>,
    }

    // "empty_deep" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty_deep" applies to empty nested
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![Annotated::new(String::new())]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty_deep" does not apply to non-empty value
    let helper = Annotated::new(Helper {
        items: Annotated::new(vec![Annotated::new("some".to_string())]),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":["some"]}"#);
}

#[test]
fn test_skip_object_never() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "never")]
        items: Annotated<Object<String>>,
    }

    // "never" always serializes
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":null}"#);
}

#[test]
fn test_skip_object_null() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null")]
        items: Annotated<Object<String>>,
    }

    // "null" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "null" does not apply to empty
    let helper = Annotated::new(Helper {
        items: Annotated::new(Object::new()),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":{}}"#);
}

#[test]
fn test_skip_object_null_deep() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null_deep")]
        items: Annotated<Object<String>>,
    }

    // "null_deep" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "null_deep" applies to nested null
    let helper = Annotated::new(Helper {
        items: Annotated::new({
            let mut obj = Object::<String>::new();
            obj.insert("foo".to_string(), Annotated::default());
            obj
        }),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":{}}"#);

    // "null_deep" does not apply to nested empty
    let helper = Annotated::new(Helper {
        items: Annotated::new({
            let mut obj = Object::<String>::new();
            obj.insert("foo".to_string(), Annotated::new(String::new()));
            obj
        }),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":""}}"#);
}

#[test]
fn test_skip_object_empty() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "empty")]
        items: Annotated<Object<String>>,
    }

    // "empty" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty" applies to empty
    let helper = Annotated::new(Helper {
        items: Annotated::new(Object::new()),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty" -> "never" nested
    let helper = Annotated::new(Helper {
        items: Annotated::new({
            let mut obj = Object::<String>::new();
            obj.insert("foo".to_string(), Annotated::default());
            obj
        }),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":null}}"#);

    // "empty" does not apply to nested empty
    let helper = Annotated::new(Helper {
        items: Annotated::new({
            let mut obj = Object::<String>::new();
            obj.insert("foo".to_string(), Annotated::new(String::new()));
            obj
        }),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":""}}"#);
}

#[test]
fn test_skip_object_empty_deep() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "empty_deep")]
        items: Annotated<Object<String>>,
    }

    // "empty_deep" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty_deep" applies to empty nested
    let helper = Annotated::new(Helper {
        items: Annotated::new({
            let mut obj = Object::<String>::new();
            obj.insert("foo".to_string(), Annotated::new(String::new()));
            obj
        }),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty_deep" does not apply to non-empty value
    let helper = Annotated::new(Helper {
        items: Annotated::new({
            let mut obj = Object::<String>::new();
            obj.insert("foo".to_string(), Annotated::new("some".to_string()));
            obj
        }),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":{"foo":"some"}}"#);
}

#[test]
fn test_skip_tuple_never() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "never")]
        items: Annotated<(Annotated<String>,)>,
    }

    // "never" always serializes
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":null}"#);
}

#[test]
fn test_skip_tuple_null() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null")]
        items: Annotated<(Annotated<String>,)>,
    }

    // "null" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "null" does not apply to nested
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::default(),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);
}

#[test]
fn test_skip_tuple_null_deep() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null_deep")]
        items: Annotated<(Annotated<String>,)>,
    }

    // "null_deep" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "null_deep" cannot remove tuple items
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::default(),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);

    // "null_deep" does not apply to nested empty
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::new(String::new()),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
}

#[test]
fn test_skip_tuple_empty() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "empty")]
        items: Annotated<(Annotated<String>,)>,
    }

    // "empty" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty" -> "null_deep" nested, but cannot remove tuple item
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::default(),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[null]}"#);

    // "empty" does not apply to nested empty
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::new(String::new()),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":[""]}"#);
}

#[test]
fn test_skip_tuple_empty_deep() {
    #[derive(Debug, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "empty_deep")]
        items: Annotated<(Annotated<String>,)>,
    }

    // "empty_deep" applies to null
    let helper = Annotated::new(Helper {
        items: Annotated::default(),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty_deep" applies to empty nested
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::new(String::new()),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{}"#);

    // "empty_deep" does not apply to non-empty value
    let helper = Annotated::new(Helper {
        items: Annotated::new((Annotated::new("some".to_string()),)),
    });
    assert_eq!(helper.to_json().unwrap(), r#"{"items":["some"]}"#);
}

#[test]
fn test_wrapper_structs_and_skip_serialization() {
    #[derive(Debug, Empty, IntoValue)]
    struct BasicWrapper(Array<String>);

    #[derive(Debug, Empty, IntoValue)]
    struct BasicHelper {
        #[metastructure(skip_serialization = "empty")]
        items: Annotated<BasicWrapper>,
    }

    let helper = Annotated::new(BasicHelper {
        items: Annotated::new(BasicWrapper(vec![])),
    });
    assert_eq!(helper.to_json().unwrap(), "{}");
}

#[test]
fn test_skip_serialization_on_regular_structs() {
    #[derive(Debug, Default, Empty, IntoValue)]
    struct Wrapper {
        foo: Annotated<u64>,
    }

    #[derive(Debug, Default, Empty, IntoValue)]
    struct Helper {
        #[metastructure(skip_serialization = "null")]
        foo: Annotated<Wrapper>,
    }

    let helper = Annotated::new(Helper {
        foo: Annotated::new(Wrapper::default()),
    });

    assert_eq!(helper.to_json().unwrap(), r#"{"foo":{}}"#);
}
