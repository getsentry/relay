#![cfg(feature = "derive")]

use relay_protocol::{
    assert_annotated_snapshot, Annotated, Empty, FromValue, IntoValue, Object, Value,
};

#[test]
fn test_from_value_flatten() {
    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Outer {
        id: Annotated<String>,

        #[metastructure(flatten)]
        inner: Inner,

        #[metastructure(additional_properties)]
        other: Object<Value>,
    }

    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Inner {
        #[metastructure(field = "foo")]
        not_foo: Annotated<String>,
        bar: Annotated<String>,
    }

    let outer = Annotated::<Outer>::from_json(
        r#"
        {
            "id": "my id",
            "foo": "foo_value",
            "bar": "bar_value",
            "future": "into"
        }
    "#,
    )
    .unwrap();

    insta::assert_debug_snapshot!(outer, @r###"
    Outer {
        id: "my id",
        inner: Inner {
            not_foo: "foo_value",
            bar: "bar_value",
        },
        other: {
            "future": String(
                "into",
            ),
        },
    }
    "###);

    assert_annotated_snapshot!(outer, @r###"
    {
      "id": "my id",
      "foo": "foo_value",
      "bar": "bar_value",
      "future": "into"
    }
    "###);
}

#[test]
fn test_from_value_flatten_nested_additional_properties() {
    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Outer {
        #[metastructure(flatten)]
        inner: Inner,

        id: Annotated<String>,
    }

    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Inner {
        #[metastructure(additional_properties)]
        other: Object<Value>,
    }

    let outer = Annotated::<Outer>::from_json(
        r#"
        {
            "id": "my id",
            "foo": "foo_value",
            "bar": "bar_value",
            "future": "into"
        }
    "#,
    )
    .unwrap();

    insta::assert_debug_snapshot!(outer, @r###"
    Outer {
        inner: Inner {
            other: {
                "bar": String(
                    "bar_value",
                ),
                "foo": String(
                    "foo_value",
                ),
                "future": String(
                    "into",
                ),
                "id": String(
                    "my id",
                ),
            },
        },
        id: ~,
    }
    "###);

    assert_annotated_snapshot!(outer, @r###"
    {
      "bar": "bar_value",
      "foo": "foo_value",
      "future": "into",
      "id": "my id"
    }
    "###);
}

#[test]
fn test_empty_flatten() {
    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Outer {
        #[metastructure(flatten)]
        inner: Inner,

        id: Annotated<String>,
    }

    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Inner {
        #[metastructure(additional_properties)]
        other: Object<Value>,
    }

    let outer: Annotated<Outer> = Annotated::empty();
    assert!(outer.is_empty());
    assert!(outer.is_deep_empty());

    let inner: Annotated<Inner> = Annotated::empty();
    assert!(inner.is_empty());
    assert!(inner.is_deep_empty());
}
