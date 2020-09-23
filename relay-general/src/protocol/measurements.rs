use crate::types::{Annotated, Error, FromValue, Object, Value};

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurement {
    /// Value of observed measurement value
    #[metastructure(required = "true")]
    pub value: Annotated<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurements(pub Object<Measurement>);

impl FromValue for Measurements {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        let mut processing_errors = vec![];

        let mut measurements = Object::<Measurement>::from_value(value).map_value(|measurements| {
            Self(measurements.into_iter().fold(
                Default::default(),
                |mut measurements: Object<Measurement>,
                 (original_name, value): (String, Annotated<Measurement>)| {
                    let name = original_name.trim().to_lowercase();

                    if let Some(measurement_value) = value.value() {
                        if is_valid_measurement_name(&name) {
                            if measurement_value.value.value().is_some() {
                                measurements.insert(name, value);
                                return measurements;
                            }

                            processing_errors.push(Error::invalid(
                                format!("measurement value for '{}' to be a number", original_name)
                                    .as_str(),
                            ));
                            return measurements;
                        }

                        processing_errors.push(Error::invalid(
                            format!(
                                "measurement name '{}' to contain only characters a-z0-9-_.",
                                original_name
                            )
                            .as_str(),
                        ));

                        return measurements;
                    }

                    processing_errors.push(Error::expected(
                        format!("measurement '{}' to have a value", original_name).as_str(),
                    ));

                    measurements
                },
            ))
        });

        let measurements_meta = measurements.meta_mut();

        for error in processing_errors {
            measurements_meta.add_error(error);
        }

        measurements
    }
}

fn is_valid_measurement_name(name: &str) -> bool {
    return name
        .chars()
        .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '-' | '_' | '.'));
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
        "Total Blocking Time": {"value": 3.14159}
    }
}"#;

    let output = r#"{
  "measurements": {
    "fid": {
      "value": 2020.0
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
              "reason": "measurement name 'Total Blocking Time' to contain only characters a-z0-9-_."
            }
          ],
          [
            "invalid_data",
            {
              "reason": "measurement value for 'cls' to be a number"
            }
          ],
          [
            "invalid_data",
            {
              "reason": "measurement value for 'fp' to be a number"
            }
          ]
        ]
      }
    }
  }
}"#;

    let mut measurements = Annotated::new(Measurements({
        let mut measurements = Object::new();
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
        measurements
    }));

    let measurements_meta = measurements.meta_mut();

    measurements_meta.add_error(Error::invalid(
        "measurement name 'Total Blocking Time' to contain only characters a-z0-9-_.",
    ));

    measurements_meta.add_error(Error::invalid("measurement value for 'cls' to be a number"));

    measurements_meta.add_error(Error::invalid("measurement value for 'fp' to be a number"));

    let event = Annotated::new(Event {
        measurements,
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_str!(event.to_json_pretty().unwrap(), output);
}
