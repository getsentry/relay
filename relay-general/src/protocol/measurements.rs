use std::ops::{Deref, DerefMut};

use relay_common::MetricUnit;

use crate::processor::ProcessValue;
use crate::types::{
    Annotated, Empty, Error, ErrorKind, FromValue, IntoValue, Object, SkipSerialization, Value,
};

impl Empty for MetricUnit {
    #[inline]
    fn is_empty(&self) -> bool {
        // MetricUnit is never empty, even None carries significance over a missing unit.
        false
    }
}

impl FromValue for MetricUnit {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match String::from_value(value) {
            Annotated(Some(value), mut meta) => match value.parse() {
                Ok(unit) => Annotated(Some(unit), meta),
                Err(_) => {
                    meta.add_error(ErrorKind::InvalidData);
                    meta.set_original_value(Some(value));
                    Annotated(None, meta)
                }
            },
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

impl IntoValue for MetricUnit {
    fn into_value(self) -> Value
    where
        Self: Sized,
    {
        Value::String(format!("{}", self))
    }

    fn serialize_payload<S>(&self, s: S, _behavior: SkipSerialization) -> Result<S::Ok, S::Error>
    where
        Self: Sized,
        S: serde::Serializer,
    {
        serde::Serialize::serialize(&self.to_string(), s)
    }
}

impl ProcessValue for MetricUnit {}

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
                            "measurement name '{}' cannot be empty",
                            name
                        )));
                    } else if is_valid_measurement_name(name) {
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
    name.starts_with(|c| matches!(c, 'a'..='z' | 'A'..='Z'))
        && name
            .chars()
            .all(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.'))
}

#[cfg(test)]
use crate::testutils::{assert_eq_dbg, assert_eq_str};

#[test]
fn test_measurements_serialization() {
    use crate::protocol::Event;
    use relay_common::DurationUnit;

    let input = r#"{
    "measurements": {
        "LCP": {"value": 420.69, "unit": "millisecond"},
        "   lcp_final.element-Size123  ": {"value": 1},
        "fid": {"value": 2020},
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
    "lcp": {
      "value": 420.69,
      "unit": "millisecond"
    },
    "lcp_final.element-size123": {
      "value": 1.0
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
              "reason": "measurement name 'Total Blocking Time' can contain only characters a-z0-9.-_"
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
            "lcp_final.element-size123".to_owned(),
            Annotated::new(Measurement {
                value: Annotated::new(1f64),
                unit: Annotated::empty(),
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
        "measurement name 'Total Blocking Time' can contain only characters a-z0-9.-_",
    ));

    let event = Annotated::new(Event {
        measurements,
        ..Default::default()
    });

    assert_eq_dbg!(event, Annotated::from_json(input).unwrap());
    assert_eq_str!(event.to_json_pretty().unwrap(), output);
}
