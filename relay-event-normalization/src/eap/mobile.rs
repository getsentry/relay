//! Mobile-specific normalizations for SpanV2 attributes.

use relay_conventions::consts::*;
use relay_event_schema::protocol::{Attributes, DeviceClass};
use relay_protocol::Annotated;

use crate::normalize::utils::{MAIN_THREAD_NAME, MAX_DURATION_MOBILE_MS, MOBILE_SDKS};

/// Normalizes mobile-specific attributes on a span.
///
/// - Sets `sentry.mobile: "true"` if the SDK is a known mobile SDK.
/// - Sets `sentry.main_thread: "true"` if `thread.name` is `"main"`.
/// - Removes mobile measurement attributes that exceed 180 seconds.
pub fn normalize_mobile_attributes(attributes: &mut Annotated<Attributes>) {
    let Some(attrs) = attributes.value_mut() else {
        return;
    };

    // Mobile tag: derive from SDK name.
    if let Some(sdk_name) = attrs.get_value(SENTRY_SDK_NAME).and_then(|v| v.as_str()) {
        if MOBILE_SDKS.contains(&sdk_name) {
            attrs.insert(SENTRY_MOBILE, "true".to_owned());
        }
    }

    // Main thread tag: derive from thread name.
    if let Some(thread_name) = attrs.get_value(THREAD_NAME).and_then(|v| v.as_str()) {
        if thread_name == MAIN_THREAD_NAME {
            attrs.insert(SENTRY_MAIN_THREAD, "true".to_owned());
        }
    }

    // Outlier filtering: remove mobile measurements exceeding 180s.
    for key in [
        APP_START_COLD_VALUE,
        APP_START_WARM_VALUE,
        APP_TTID_VALUE,
        APP_TTFD_VALUE,
    ] {
        if let Some(value) = attrs.get_value(key).and_then(|v| v.as_f64()) {
            if value > MAX_DURATION_MOBILE_MS {
                attrs.remove(key);
            }
        }
    }
}

/// Derives the `device.class` attribute from device attributes.
///
/// Uses the same classification logic as the V1 pipeline:
/// - iOS: lookup table from `device.model` (e.g. `"iPhone17,5"` → HIGH)
/// - Android: thresholds on `device.processor_frequency`, `device.processor_count`,
///   `device.memory_size`
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
                "app.start.cold.value": {"type": "double", "value": 200000.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(APP_START_COLD_VALUE).is_none());
    }

    #[test]
    fn test_outlier_filtering_keeps_valid() {
        let mut attributes = Annotated::<Attributes>::from_json(
            r#"{
                "app.start.cold.value": {"type": "double", "value": 5000.0}
            }"#,
        )
        .unwrap();

        normalize_mobile_attributes(&mut attributes);

        let attrs = attributes.value().unwrap();
        assert!(attrs.get_value(APP_START_COLD_VALUE).is_some());
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
