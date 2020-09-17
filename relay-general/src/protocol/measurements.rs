use regex::Regex;

use crate::types::{Annotated, Error, FromValue, Object, Value};

lazy_static::lazy_static! {
    static ref MEASUREMENT_NAME: Regex = Regex::new("^[a-z0-9-._]+$").unwrap();
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurement {
    /// Value of observed measurement value
    #[metastructure(skip_serialization = "empty")]
    pub value: Annotated<f64>,
}

#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurements(#[metastructure(skip_serialization = "empty")] pub Object<Measurement>);

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
                        if MEASUREMENT_NAME.is_match(&name) {
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

                    return measurements;
                },
            ))
        });

        let measurements_meta = measurements.meta_mut();

        for error in processing_errors {
            measurements_meta.add_error(error);
        }

        measurements
    }
    // fn from_value(value: Annotated<Value>) -> Annotated<Self> {
    //     match value {
    //         Annotated(Some(Value::Object(items)), mut measurement_meta) => {
    //             let mut measurements = Object::<Measurement>::new();

    //             for (raw_name, raw_observed_value) in items.into_iter() {
    //                 let observed_value: Annotated<f64> = match raw_observed_value {
    //                     Annotated(Some(Value::Object(bag)), mut object_meta) => {
    //                         match bag.get("value") {
    //                             Some(Annotated(Some(Value::I64(value)), meta)) => {
    //                                 Annotated(Some(*value as f64), meta.clone())
    //                             }
    //                             Some(Annotated(Some(Value::U64(value)), meta)) => {
    //                                 Annotated(Some(*value as f64), meta.clone())
    //                             }
    //                             Some(Annotated(Some(Value::F64(value)), meta)) => {
    //                                 Annotated(Some(*value), meta.clone())
    //                             }
    //                             Some(Annotated(value, meta)) => {
    //                                 measurement_meta.add_error(Error::expected(
    //                                     format!(
    //                                         "measurement value for '{}' to be a number",
    //                                         raw_name
    //                                     )
    //                                     .as_str(),
    //                                 ));

    //                                 let mut meta = meta.clone();
    //                                 meta.add_error(Error::expected("number"));
    //                                 meta.set_original_value(value.clone());
    //                                 Annotated(None, meta)
    //                             }
    //                             None => {
    //                                 object_meta.add_error(Error::expected("value"));
    //                                 Annotated(None, object_meta)
    //                             }
    //                         }
    //                     }
    //                     Annotated(value, meta) => {
    //                         let mut meta = meta.clone();
    //                         meta.add_error(Error::expected("object"));
    //                         meta.set_original_value(value);
    //                         Annotated(None, meta.clone())
    //                     }
    //                 };

    //                 let measurement_name = raw_name.trim().to_lowercase();

    //                 if MEASUREMENT_NAME.is_match(&measurement_name) {
    //                     if observed_value.value().is_some() {
    //                         let measurement = Measurement {
    //                             value: observed_value,
    //                         };

    //                         measurements.insert(measurement_name, Annotated::new(measurement));
    //                     }
    //                 } else {
    //                     measurement_meta.add_error(Error::expected(
    //                         format!(
    //                             "measurement name '{}' to contain only characters a-z0-9-_.",
    //                             raw_name
    //                         )
    //                         .as_str(),
    //                     ));
    //                 }
    //             }

    //             Annotated(Some(Measurements(measurements)), measurement_meta)
    //         }
    //         Annotated(None, meta) => Annotated(None, meta),
    //         Annotated(Some(value), mut meta) => {
    //             meta.add_error(Error::expected("measurements"));
    //             meta.set_original_value(Some(value));
    //             Annotated(None, meta)
    //         }
    //     }
    // }
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
