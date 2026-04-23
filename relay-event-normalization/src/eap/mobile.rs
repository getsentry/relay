//! Mobile-specific normalizations for SpanV2 attributes.

use relay_conventions::consts::*;
use relay_event_schema::protocol::{Attributes, DeviceClass};
use relay_protocol::Annotated;

use crate::normalize::utils::{MAIN_THREAD_NAME, MAX_DURATION_MOBILE_MS, MOBILE_SDKS};

/// Normalizes mobile-specific attributes on a span.
///
/// - Sets `sentry.mobile: "true"` if the SDK is a known mobile SDK.
/// - Sets `sentry.main_thread: "true"` if the SDK is mobile and `thread.name` is `"main"`.
/// - Removes mobile measurement attributes that exceed 180 seconds.
/// - Normalizes V1 `app_start_cold`/`app_start_warm` into unified `app.vitals.start.*` attributes.
/// - Derives `device.class` from device attributes if not already set.
pub fn normalize_mobile_attributes(attributes: &mut Annotated<Attributes>) {
    let Some(attrs) = attributes.value_mut() else {
        return;
    };

    if let Some(sdk_name) = attrs.get_value(SENTRY__SDK__NAME).and_then(|v| v.as_str())
        && MOBILE_SDKS.contains(&sdk_name)
    {
        attrs.insert(SENTRY__MOBILE, "true".to_owned());

        if let Some(thread_name) = attrs.get_value(THREAD__NAME).and_then(|v| v.as_str())
            && thread_name == MAIN_THREAD_NAME
        {
            attrs.insert(SENTRY__MAIN_THREAD, "true".to_owned());
        }
    }

    for key in [
        APP__VITALS__START__COLD__VALUE,
        APP__VITALS__START__WARM__VALUE,
        APP__VITALS__START__VALUE,
        APP__VITALS__TTID__VALUE,
        APP__VITALS__TTFD__VALUE,
    ] {
        if let Some(value) = attrs.get_value(key).and_then(|v| v.as_f64())
            && value > MAX_DURATION_MOBILE_MS
        {
            attrs.remove(key);
        }
    }

    // Normalize app start measurements into unified attributes.
    // V1 spans have measurements `app_start_cold`/`app_start_warm` which become
    // attributes with those names after v1→v2 conversion.
    // V2 spans will at some point send `app.vitals.start.value` + `app.vitals.start.type` directly.
    if !attrs.contains_key(APP__VITALS__START__VALUE) {
        if let Some(value) = attrs.get_value("app_start_cold").and_then(|v| v.as_f64())
            && value <= MAX_DURATION_MOBILE_MS
        {
            attrs.insert(APP__VITALS__START__VALUE, value);
            attrs.insert_if_missing(APP__VITALS__START__TYPE, || "cold".to_owned());
        } else if let Some(value) = attrs.get_value("app_start_warm").and_then(|v| v.as_f64())
            && value <= MAX_DURATION_MOBILE_MS
        {
            attrs.insert(APP__VITALS__START__VALUE, value);
            attrs.insert_if_missing(APP__VITALS__START__TYPE, || "warm".to_owned());
        }
    }

    // Derive device.class from device attributes if not already set.
    if !attrs.contains_key(DEVICE__CLASS)
        && let Some(device_class) = DeviceClass::from_attributes(attrs)
    {
        attrs.insert(DEVICE__CLASS, device_class.to_string());
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::assert_annotated_snapshot;

    use super::*;

    macro_rules! attributes {
        ($($key:expr => $value:expr),* $(,)?) => {
            Attributes::from([
                $(($key.into(), Annotated::new($value.into())),)*
            ])
        };
    }

    macro_rules! mobile_sdk_test {
        ($name:ident, $sdk:expr) => {
            #[test]
            fn $name() {
                let mut attributes = Annotated::new(attributes! {
                    SENTRY__SDK__NAME => $sdk,
                });
                normalize_mobile_attributes(&mut attributes);
                assert_annotated_snapshot!(attributes);
            }
        };
    }

    mobile_sdk_test!(test_mobile_tag_cocoa, "sentry.cocoa");
    mobile_sdk_test!(test_mobile_tag_flutter, "sentry.dart.flutter");
    mobile_sdk_test!(test_mobile_tag_android, "sentry.java.android");
    mobile_sdk_test!(
        test_mobile_tag_react_native,
        "sentry.javascript.react-native"
    );

    #[test]
    fn test_mobile_tag_not_mobile_sdk() {
        let mut attributes = Annotated::new(attributes! {
            SENTRY__SDK__NAME => "sentry.python",
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "sentry.sdk.name": {
            "type": "string",
            "value": "sentry.python"
          }
        }
        "#);
    }

    #[test]
    fn test_main_thread_tag_mobile_sdk() {
        let mut attributes = Annotated::new(attributes! {
            SENTRY__SDK__NAME => "sentry.cocoa",
            THREAD__NAME => "main",
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "sentry.main_thread": {
            "type": "string",
            "value": "true"
          },
          "sentry.mobile": {
            "type": "string",
            "value": "true"
          },
          "sentry.sdk.name": {
            "type": "string",
            "value": "sentry.cocoa"
          },
          "thread.name": {
            "type": "string",
            "value": "main"
          }
        }
        "#);
    }

    #[test]
    fn test_main_thread_tag_not_main() {
        let mut attributes = Annotated::new(attributes! {
            SENTRY__SDK__NAME => "sentry.cocoa",
            THREAD__NAME => "background",
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "sentry.mobile": {
            "type": "string",
            "value": "true"
          },
          "sentry.sdk.name": {
            "type": "string",
            "value": "sentry.cocoa"
          },
          "thread.name": {
            "type": "string",
            "value": "background"
          }
        }
        "#);
    }

    #[test]
    fn test_main_thread_tag_not_set_for_non_mobile_sdk() {
        let mut attributes = Annotated::new(attributes! {
            SENTRY__SDK__NAME => "sentry.python",
            THREAD__NAME => "main",
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "sentry.sdk.name": {
            "type": "string",
            "value": "sentry.python"
          },
          "thread.name": {
            "type": "string",
            "value": "main"
          }
        }
        "#);
    }

    macro_rules! outlier_test {
        ($name:ident, $key:expr, $value:expr) => {
            #[test]
            fn $name() {
                let mut attributes = Annotated::new(attributes! {
                    $key => $value,
                });
                normalize_mobile_attributes(&mut attributes);
                assert_annotated_snapshot!(attributes);
            }
        };
    }

    outlier_test!(
        test_outlier_removes_start_cold,
        APP__VITALS__START__COLD__VALUE,
        200_000.0
    );
    outlier_test!(
        test_outlier_removes_start_warm,
        APP__VITALS__START__WARM__VALUE,
        200_000.0
    );
    outlier_test!(
        test_outlier_removes_start_value,
        APP__VITALS__START__VALUE,
        200_000.0
    );
    outlier_test!(
        test_outlier_removes_ttid,
        APP__VITALS__TTID__VALUE,
        200_000.0
    );
    outlier_test!(
        test_outlier_removes_ttfd,
        APP__VITALS__TTFD__VALUE,
        200_000.0
    );

    outlier_test!(
        test_outlier_keeps_start_cold,
        APP__VITALS__START__COLD__VALUE,
        5000.0
    );
    outlier_test!(
        test_outlier_keeps_start_warm,
        APP__VITALS__START__WARM__VALUE,
        5000.0
    );
    outlier_test!(
        test_outlier_keeps_start_value,
        APP__VITALS__START__VALUE,
        5000.0
    );
    outlier_test!(test_outlier_keeps_ttid, APP__VITALS__TTID__VALUE, 5000.0);
    outlier_test!(test_outlier_keeps_ttfd, APP__VITALS__TTFD__VALUE, 5000.0);

    #[test]
    fn test_app_start_cold_normalized() {
        let mut attributes = Annotated::new(attributes! {
            "app_start_cold" => 1234.0,
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "app.vitals.start.type": {
            "type": "string",
            "value": "cold"
          },
          "app.vitals.start.value": {
            "type": "double",
            "value": 1234.0
          },
          "app_start_cold": {
            "type": "double",
            "value": 1234.0
          }
        }
        "#);
    }

    #[test]
    fn test_app_start_warm_normalized() {
        let mut attributes = Annotated::new(attributes! {
            "app_start_warm" => 567.0,
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "app.vitals.start.type": {
            "type": "string",
            "value": "warm"
          },
          "app.vitals.start.value": {
            "type": "double",
            "value": 567.0
          },
          "app_start_warm": {
            "type": "double",
            "value": 567.0
          }
        }
        "#);
    }

    #[test]
    fn test_app_start_v2_not_overwritten() {
        let mut attributes = Annotated::new(attributes! {
            APP__VITALS__START__VALUE => 999.0,
            APP__VITALS__START__TYPE => "warm",
            "app_start_cold" => 1234.0,
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "app.vitals.start.type": {
            "type": "string",
            "value": "warm"
          },
          "app.vitals.start.value": {
            "type": "double",
            "value": 999.0
          },
          "app_start_cold": {
            "type": "double",
            "value": 1234.0
          }
        }
        "#);
    }

    #[test]
    fn test_device_class_iphone() {
        let mut attributes = Annotated::new(attributes! {
            DEVICE__FAMILY => "iPhone",
            DEVICE__MODEL => "iPhone17,5",
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "device.class": {
            "type": "string",
            "value": "3"
          },
          "device.family": {
            "type": "string",
            "value": "iPhone"
          },
          "device.model": {
            "type": "string",
            "value": "iPhone17,5"
          }
        }
        "#);
    }

    #[test]
    fn test_device_class_android() {
        let mut attributes = Annotated::new(attributes! {
            DEVICE__FAMILY => "Android",
            DEVICE__PROCESSOR_FREQUENCY => 3000.0,
            DEVICE__PROCESSOR_COUNT => 8.0,
            DEVICE__MEMORY_SIZE => 8_589_934_592.0,
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "device.class": {
            "type": "string",
            "value": "3"
          },
          "device.family": {
            "type": "string",
            "value": "Android"
          },
          "device.memory_size": {
            "type": "double",
            "value": 8589934592.0
          },
          "device.processor_count": {
            "type": "double",
            "value": 8.0
          },
          "device.processor_frequency": {
            "type": "double",
            "value": 3000.0
          }
        }
        "#);
    }

    #[test]
    fn test_device_class_missing_attrs() {
        let mut attributes = Annotated::new(attributes! {
            DEVICE__FAMILY => "Android",
        });

        normalize_mobile_attributes(&mut attributes);

        assert_annotated_snapshot!(attributes, @r#"
        {
          "device.family": {
            "type": "string",
            "value": "Android"
          }
        }
        "#);
    }
}
