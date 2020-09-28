use crate::types::{Annotated, Error, FromValue, Object, Value};

/// An individual observed measurement.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurement {
    /// Value of observed measurement value.
    #[metastructure(required = "true", skip_serialization = "never")]
    pub value: Annotated<f64>,
}

/// A map of observed measurement values.
///
/// Measurements are only available on transactions. They contain measurement values of observed
/// values such as Largest Contentful Paint (LCP).
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurements(pub Object<Measurement>);

impl FromValue for Measurements {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        let mut processing_errors = Vec::new();

        let mut measurements = Object::<Measurement>::from_value(value).map_value(|measurements| {
            let measurements = measurements
                .into_iter()
                .filter_map(|(name, mut object)| {
                    let name = name.trim();

                    if is_valid_measurement_name(name) {
                        if let Some(measurement) = object.value() {
                            if measurement.value.value().is_some() {
                                return Some((name.to_lowercase(), object));
                            }

                            return Some((name.to_lowercase(), object));
                        }

                        object.set_value(Some(Measurement {
                            value: Annotated::empty(),
                        }));

                        return Some((name.to_lowercase(), object));
                    } else {
                        processing_errors.push(Error::invalid(format!(
                            "measurement name '{}' can contain only characters a-z0-9.-_",
                            name
                        )));
                    }

                    None
                })
                .collect();

            Self(measurements)
        });

        for error in processing_errors {
            measurements.meta_mut().add_error(error);
        }

        measurements
    }
}

fn is_valid_measurement_name(name: &str) -> bool {
    name.chars()
        .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '-' | '_' | '.'))
}

#[test]
fn test_measurements_serialization() {
    use crate::protocol::Event;

    let input = r#"{
    "measurements": {
        "LCP": {"value": 420.69},
        "   lcp_final.element-Size  ": {"value": 1},
        "fid": {"value": 2020},
        "cls": {"value": null},
        "fp": {"value": "im a first paint"},
        "Total Blocking Time": {"value": 3.14159},
        "missing_value": "string"
    }
}"#;

    let output = r#"{
  "measurements": {
    "cls": {
      "value": null
    },
    "fid": {
      "value": 2020.0
    },
    "fp": {
      "value": null
    },
    "lcp": {
      "value": 420.69
    },
    "lcp_final.element-size": {
      "value": 1.0
    }
  },
  "_meta": {
    "measurements": {
      "": {
        "err": [
          [
            "invalid_data",
            {
              "reason": "measurement name 'Total Blocking Time' can contain only characters a-z0-9.-_"
            }
          ],
          [
            "invalid_data",
            {
              "reason": "expected object"
            }
          ]
        ]
      },
      "cls": {
        "value": {
          "": {
            "err": [
              [
                "invalid_data",
                {
                  "reason": "measurement value must be numeric"
                }
              ]
            ]
          }
        }
      },
      "fp": {
        "value": {
          "": {
            "err": [
              [
                "invalid_data",
                {
                  "reason": "measurement value must be numeric"
                }
              ]
            ]
          }
        }
      }
    }
  }
}"#;

    let mut measurements = Annotated::new(Measurements({
        let mut measurements = Object::new();
        measurements.insert(
            "cls".to_owned(),
            Annotated::new(Measurement {
                value: Annotated::from_error(
                    Error::invalid("measurement value must be numeric"),
                    None,
                ),
            }),
        );
        measurements.insert(
            "lcp".to_owned(),
            Annotated::new(Measurement {
                value: Annotated::new(420.69),
            }),
        );
        measurements.insert(
            "lcp_final.element-size".to_owned(),
            Annotated::new(Measurement {
                value: Annotated::new(1f64),
            }),
        );
        measurements.insert(
            "fid".to_owned(),
            Annotated::new(Measurement {
                value: Annotated::new(2020f64),
            }),
        );
        measurements.insert(
            "fp".to_owned(),
            Annotated::new(Measurement {
                value: Annotated::from_error(
                    Error::invalid("measurement value must be numeric"),
                    None,
                ),
            }),
        );
        measurements
    }));

    let measurements_meta = measurements.meta_mut();

    measurements_meta.add_error(Error::invalid(
        "measurement name 'Total Blocking Time' can contain only characters a-z0-9.-_",
    ));

    measurements_meta.add_error(Error::expected("object"));

    let event = Annotated::new(Event {
        measurements,
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_str!(event.to_json_pretty().unwrap(), output);
}
