//! Mobile-specific normalizations for SpanV2 attributes.

use relay_conventions::consts::*;
use relay_event_schema::protocol::{Attributes, DeviceClass};
use relay_protocol::Annotated;

use crate::normalize::utils::{MAIN_THREAD_NAME, MAX_DURATION_MOBILE_MS, MOBILE_SDKS};

pub fn normalize_mobile_attributes(attributes: &mut Annotated<Attributes>) {
    let Some(attrs) = attributes.value_mut() else {
        return;
    };

    if let Some(sdk_name) = attrs.get_value(SENTRY_SDK_NAME).and_then(|v| v.as_str()) {
        if MOBILE_SDKS.contains(&sdk_name) {
            attrs.insert(SENTRY_MOBILE, "true".to_owned());
        }
    }

    if let Some(thread_name) = attrs.get_value(THREAD_NAME).and_then(|v| v.as_str()) {
        if thread_name == MAIN_THREAD_NAME {
            attrs.insert(SENTRY_MAIN_THREAD, "true".to_owned());
        }
    }

    for key in [
        APP_VITALS_START_COLD_VALUE,
        APP_VITALS_START_WARM_VALUE,
        APP_VITALS_TTID_VALUE,
        APP_VITALS_TTFD_VALUE,
    ] {
        if let Some(value) = attrs.get_value(key).and_then(|v| v.as_f64()) {
            if value > MAX_DURATION_MOBILE_MS {
                attrs.remove(key);
            }
        }
    }

    // Normalize app start measurements into unified attributes.
    // V1 spans have measurements `app_start_cold`/`app_start_warm` which become
    // attributes with those names after v1→v2 conversion.
    // V2 spans will at some point send `app.start.value` + `app.start.type` directly.
    if !attrs.contains_key(APP_VITALS_START_VALUE) {
        if let Some(value) = attrs.get_value("app_start_cold").and_then(|v| v.as_f64()) {
            if value <= MAX_DURATION_MOBILE_MS {
                attrs.insert(APP_VITALS_START_VALUE, value);
                attrs.insert_if_missing(APP_VITALS_START_TYPE, || "cold".to_owned());
            }
        } else if let Some(value) = attrs.get_value("app_start_warm").and_then(|v| v.as_f64()) {
            if value <= MAX_DURATION_MOBILE_MS {
                attrs.insert(APP_VITALS_START_VALUE, value);
                attrs.insert_if_missing(APP_VITALS_START_TYPE, || "warm".to_owned());
            }
        }
    }
}

pub fn normalize_device_class(attributes: &mut Annotated<Attributes>) {
    let Some(attrs) = attributes.value_mut() else {
        return;
    };

    if attrs.contains_key(DEVICE_CLASS) {
        return;
    }

    if let Some(device_class) = DeviceClass::from_attributes(attrs) {
        attrs.insert(DEVICE_CLASS, device_class.to_string());
    }
}

#[cfg(test)]
mod tests {
    use relay_protocol::SerializableAnnotated;

    use super::*;

    #[test]
    fn test_mobile_tag_cocoa() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "sentry.sdk.name": {"type": "string", "value": "sentry.cocoa"}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r###"
        {
          "sentry.mobile": {
            "type": "string",
            "value": "true"
          },
          "sentry.sdk.name": {
            "type": "string",
            "value": "sentry.cocoa"
          }
        }
        "###);
    }

    #[test]
    fn test_mobile_tag_not_mobile_sdk() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "sentry.sdk.name": {"type": "string", "value": "sentry.python"}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(SENTRY_MOBILE).is_none());
    }

    #[test]
    fn test_main_thread_tag() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "thread.name": {"type": "string", "value": "main"}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r###"
        {
          "sentry.main_thread": {
            "type": "string",
            "value": "true"
          },
          "thread.name": {
            "type": "string",
            "value": "main"
          }
        }
        "###);
    }

    #[test]
    fn test_main_thread_tag_not_main() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "thread.name": {"type": "string", "value": "background"}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(SENTRY_MAIN_THREAD).is_none());
    }

    #[test]
    fn test_outlier_filtering_removes_excessive() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app.vitals.start.cold.value": {"type": "double", "value": 200000.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(APP_VITALS_START_COLD_VALUE).is_none());
    }

    #[test]
    fn test_outlier_filtering_keeps_valid() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app.vitals.start.cold.value": {"type": "double", "value": 5000.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(APP_VITALS_START_COLD_VALUE).is_some());
    }

    #[test]
    fn test_app_start_cold_normalized() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app_start_cold": {"type": "double", "value": 1234.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert_eq!(
            attrs.get_value(APP_VITALS_START_VALUE).and_then(|v| v.as_f64()),
            Some(1234.0)
        );
        assert_eq!(
            attrs.get_value(APP_VITALS_START_TYPE).and_then(|v| v.as_str()),
            Some("cold")
        );
    }

    #[test]
    fn test_app_start_warm_normalized() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app_start_warm": {"type": "double", "value": 567.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert_eq!(
            attrs.get_value(APP_VITALS_START_VALUE).and_then(|v| v.as_f64()),
            Some(567.0)
        );
        assert_eq!(
            attrs.get_value(APP_VITALS_START_TYPE).and_then(|v| v.as_str()),
            Some("warm")
        );
    }

    #[test]
    fn test_app_start_cold_outlier_not_normalized() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app_start_cold": {"type": "double", "value": 200000.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(APP_VITALS_START_VALUE).is_none());
        assert!(attrs.get_value(APP_VITALS_START_TYPE).is_none());
    }

    #[test]
    fn test_app_start_v2_not_overwritten() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app.vitals.start.value": {"type": "double", "value": 999.0},
                "app.vitals.start.type": {"type": "string", "value": "warm"},
                "app_start_cold": {"type": "double", "value": 1234.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert_eq!(
            attrs.get_value(APP_VITALS_START_VALUE).and_then(|v| v.as_f64()),
            Some(999.0)
        );
        assert_eq!(
            attrs.get_value(APP_VITALS_START_TYPE).and_then(|v| v.as_str()),
            Some("warm")
        );
    }

    #[test]
    fn test_device_class_iphone() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "device.family": {"type": "string", "value": "iPhone"},
                "device.model": {"type": "string", "value": "iPhone17,5"}
            }"#,
        )
        .unwrap();

        normalize_device_class(&mut attributes);

        insta::assert_json_snapshot!(SerializableAnnotated(&attributes), @r###"
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
        "###);
    }

    #[test]
    fn test_device_class_android_high() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "device.family": {"type": "string", "value": "Android"},
                "device.processor_frequency": {"type": "double", "value": 3000.0},
                "device.processor_count": {"type": "double", "value": 8.0},
                "device.memory_size": {"type": "double", "value": 8589934592.0}
            }"#,
        )
        .unwrap();

        normalize_device_class(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert_eq!(
            attrs.get_value(DEVICE_CLASS).and_then(|v| v.as_str()),
            Some("3")
        );
    }

    #[test]
    fn test_device_class_android_low() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "device.family": {"type": "string", "value": "Android"},
                "device.processor_frequency": {"type": "double", "value": 1500.0},
                "device.processor_count": {"type": "double", "value": 4.0},
                "device.memory_size": {"type": "double", "value": 2147483648.0}
            }"#,
        )
        .unwrap();

        normalize_device_class(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert_eq!(
            attrs.get_value(DEVICE_CLASS).and_then(|v| v.as_str()),
            Some("1")
        );
    }

    #[test]
    fn test_device_class_missing_attrs() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "device.family": {"type": "string", "value": "Android"}
            }"#,
        )
        .unwrap();

        normalize_device_class(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(DEVICE_CLASS).is_none());
    }
}
