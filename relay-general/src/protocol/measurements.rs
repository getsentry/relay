use regex::Regex;

use crate::types::{Annotated, Error, FromValue, Object, Value};

lazy_static::lazy_static! {
    static ref MEASUREMENT_NAME: Regex = Regex::new("^[a-z0-9-._]+$").unwrap();
}

#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurement {
    pub value: Annotated<f64>,
}

// impl Measurement {
//     fn new(value: f64) -> Self {
//         Measurement {
//             value: Annotated::new(value),
//         }
//     }
// }

// TODO: need bag_size?
#[derive(Clone, Debug, Default, PartialEq, Empty, ToValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct Measurements(pub Object<Measurement>);

impl Measurements {
    pub fn merge(&mut self, input: Annotated<Measurements>) {
        let input: Measurements = match input.into_value() {
            None => {
                return;
            }
            Some(measurements) => measurements,
        };

        if self.0.is_empty() {
            self.0 = input.0;
            return;
        }

        for (name, measurement) in input.0.into_iter() {
            self.0.insert(name, measurement);
        }
    }
}

impl FromValue for Measurements {
    fn from_value(value: Annotated<Value>) -> Annotated<Self> {
        match value {
            Annotated(Some(Value::Object(items)), mut meta) => {
                let mut measurements = Object::<Measurement>::new();

                for (raw_name, raw_value) in items.into_iter() {
                    let value: Annotated<f64> = match raw_value {
                        Annotated(Some(Value::Object(bag)), mut object_meta) => {
                            match bag.get("value") {
                                Some(Annotated(Some(Value::I64(value)), meta)) => {
                                    Annotated(Some(*value as f64), meta.clone())
                                }
                                Some(Annotated(Some(Value::U64(value)), meta)) => {
                                    Annotated(Some(*value as f64), meta.clone())
                                }
                                Some(Annotated(Some(Value::F64(value)), meta)) => {
                                    Annotated(Some(*value), meta.clone())
                                }
                                Some(Annotated(value, meta)) => {
                                    let mut meta = meta.clone();
                                    meta.add_error(Error::expected("number"));
                                    meta.set_original_value(value.clone());
                                    Annotated(None, meta)
                                }
                                None => {
                                    object_meta.add_error(Error::expected("value"));
                                    Annotated(None, object_meta)
                                }
                            }
                        }
                        Annotated(value, meta) => {
                            let mut meta = meta.clone();
                            meta.add_error(Error::expected("object"));
                            meta.set_original_value(value);
                            Annotated(None, meta.clone())
                        }
                    };

                    let measurement_name = raw_name.to_lowercase();

                    if !MEASUREMENT_NAME.is_match(&measurement_name) {
                        meta.add_error(Error::expected(
                            "measurement name to contain only characters a-zA-Z0-9-_.",
                        ));
                        meta.set_original_value(Some(raw_name));

                        // TODO: fix this
                    }

                    let measurement = Measurement { value };

                    measurements.insert(measurement_name, Annotated::new(measurement));
                }

                Annotated(Some(Measurements(measurements)), meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_error(Error::expected("measurements"));
                meta.set_original_value(Some(value));
                Annotated(None, meta)
            }
        }
    }
}

// impl std::ops::Deref for Measurements {
//     type Target = Object<Measurement>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

// impl std::ops::DerefMut for Measurements {
//     fn deref_mut(&mut self) -> &mut Self::Target {
//         &mut self.0
//     }
// }
