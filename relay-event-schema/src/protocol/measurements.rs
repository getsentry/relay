use std::ops::{Deref, DerefMut};

use relay_base_schema::metrics::MetricUnit;
use relay_protocol::{Annotated, Empty, Error, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// An individual observed measurement.
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
pub struct Measurement {
    /// Value of observed measurement value.
    #[metastructure(required = "true", skip_serialization = "never")]
    pub value: Annotated<f64>,

    /// The unit of this measurement, defaulting to no unit.
    pub unit: Annotated<MetricUnit>,
}

/// A map of observed measurement values.
///
/// They contain measurement values of observed values such as Largest Contentful Paint (LCP).
#[derive(Clone, Debug, Default, PartialEq, Empty, IntoValue, ProcessValue)]
pub struct Measurements(pub Object<Measurement>);

impl Measurements {
    /// Returns the underlying object of measurements.
    pub fn into_inner(self) -> Object<Measurement> {
        self.0
    }

    /// Return the value of the measurement with the given name, if it exists.
    pub fn get_value(&self, key: &str) -> Option<f64> {
        self.get(key)
            .and_then(Annotated::value)
            .and_then(|x| x.value.value())
            .copied()
    }
}

impl FromValue for Measurements {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        let mut processing_errors = Vec::new();

        let mut measurements = Object::from_value(value).map_value(|measurements| {
            let measurements = measurements
                .into_iter()
                .filter_map(|(name, object)| {
                    let name = name.trim();

                    if name.is_empty() {
                        processing_errors.push(Error::invalid(format!(
                            "measurement name '{name}' cannot be empty"
                        )));
                    } else if is_valid_measurement_name(name) {
                        return Some((name.to_lowercase(), object));
                    } else {
                        processing_errors.push(Error::invalid(format!(
                            "measurement name '{name}' can contain only characters a-z0-9._"
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

impl Deref for Measurements {
    type Target = Object<Measurement>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Measurements {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

fn is_valid_measurement_name(name: &str) -> bool {
    name.starts_with(|c: char| c.is_ascii_alphabetic())
        && name
            .chars()
            .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '.'))
}

#[cfg(test)]
mod tests {
    use relay_base_schema::metrics::DurationUnit;
    use similar_asserts::assert_eq;

    use super::*;
    use crate::protocol::Event;

    #[test]
    fn test_measurements_serialization() {
        let input = r#"{
    "measurements": {
        "LCP": {"value": 420.69, "unit": "millisecond"},
        "   lcp_final.element-Size123  ": {"value": 1},
        "fid": {"value": 2020},
        "inp": {"value": 100.14},
        "cls": {"value": null},
        "fp": {"value": "im a first paint"},
        "Total Blocking Time": {"value": 3.14159},
        "missing_value": "string",
        "": {"value": 2.71828}
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
    "inp": {
      "value": 100.14
    },
    "lcp": {
      "value": 420.69,
      "unit": "millisecond"
    },
    "missing_value": null
  },
  "_meta": {
    "measurements": {
      "": {
        "err": [
          [
            "invalid_data",
            {
              "reason": "measurement name '' cannot be empty"
            }
          ],
          [
            "invalid_data",
            {
              "reason": "measurement name 'lcp_final.element-Size123' can contain only characters a-z0-9._"
            }
          ],
          [
            "invalid_data",
            {
              "reason": "measurement name 'Total Blocking Time' can contain only characters a-z0-9._"
            }
          ]
        ]
      },
      "fp": {
        "value": {
          "": {
            "err": [
              [
                "invalid_data",
                {
                  "reason": "expected a floating point number"
                }
              ]
            ],
            "val": "im a first paint"
          }
        }
      },
      "missing_value": {
        "": {
          "err": [
            [
              "invalid_data",
              {
                "reason": "expected measurement"
              }
            ]
          ],
          "val": "string"
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
                    value: Annotated::empty(),
                    unit: Annotated::empty(),
                }),
            );
            measurements.insert(
                "lcp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(420.69),
                    unit: Annotated::new(MetricUnit::Duration(DurationUnit::MilliSecond)),
                }),
            );
            measurements.insert(
                "fid".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(2020f64),
                    unit: Annotated::empty(),
                }),
            );
            measurements.insert(
                "inp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::new(100.14),
                    unit: Annotated::empty(),
                }),
            );
            measurements.insert(
                "fp".to_owned(),
                Annotated::new(Measurement {
                    value: Annotated::from_error(
                        Error::expected("a floating point number"),
                        Some("im a first paint".into()),
                    ),
                    unit: Annotated::empty(),
                }),
            );

            measurements.insert(
                "missing_value".to_owned(),
                Annotated::from_error(Error::expected("measurement"), Some("string".into())),
            );

            measurements
        }));

        let measurements_meta = measurements.meta_mut();

        measurements_meta.add_error(Error::invalid("measurement name '' cannot be empty"));

        measurements_meta.add_error(Error::invalid(
            "measurement name 'lcp_final.element-Size123' can contain only characters a-z0-9._",
        ));

        measurements_meta.add_error(Error::invalid(
            "measurement name 'Total Blocking Time' can contain only characters a-z0-9._",
        ));

        let event = Annotated::new(Event {
            measurements,
            ..Default::default()
        });

        assert_eq!(event, Annotated::from_json(input).unwrap());
        assert_eq!(event.to_json_pretty().unwrap(), output);
    }
}
