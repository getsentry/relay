#![cfg(feature = "derive")]

use relay_protocol::{Annotated, Empty, Error, ErrorKind, FromValue, IntoValue};
use similar_asserts::assert_eq;

#[test]
fn test_annotated_deserialize_with_meta() {
    #[derive(Debug, Empty, FromValue, IntoValue)]
    struct Foo {
        id: Annotated<u64>,
        #[metastructure(field = "type")]
        ty: Annotated<String>,
    }

    let annotated_value = Annotated::<Foo>::from_json(
        r#"
        {
            "id": "blaflasel",
            "type": "testing",
            "_meta": {
                "id": {
                    "": {
                        "err": ["unknown_error"]
                    }
                },
                "type": {
                    "": {
                        "err": ["invalid_data"]
                    }
                }
            }
        }
    "#,
    )
    .unwrap();

    assert_eq!(annotated_value.value().unwrap().id.value(), None);
    assert_eq!(
        annotated_value
            .value()
            .unwrap()
            .id
            .meta()
            .iter_errors()
            .collect::<Vec<&Error>>(),
        vec![
            &Error::new(ErrorKind::Unknown("unknown_error".to_string())),
            &Error::expected("an unsigned integer")
        ],
    );
    assert_eq!(
        annotated_value.value().unwrap().ty.as_str(),
        Some("testing")
    );
    assert_eq!(
        annotated_value
            .value()
            .unwrap()
            .ty
            .meta()
            .iter_errors()
            .collect::<Vec<&Error>>(),
        vec![&Error::new(ErrorKind::InvalidData)],
    );

    let json = annotated_value.to_json_pretty().unwrap();
    assert_eq!(
        json,
        r#"{
  "id": null,
  "type": "testing",
  "_meta": {
    "id": {
      "": {
        "err": [
          "unknown_error",
          [
            "invalid_data",
            {
              "reason": "expected an unsigned integer"
            }
          ]
        ],
        "val": "blaflasel"
      }
    },
    "type": {
      "": {
        "err": [
          "invalid_data"
        ]
      }
    }
  }
}"#
    );
}
