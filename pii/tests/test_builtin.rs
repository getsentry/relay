use std::collections::BTreeMap;

use relay_general::processor::{process_value, ProcessingState, ValueType};
use relay_general::types::{Annotated, Remark, RemarkType};
use relay_pii::{PiiConfig, PiiProcessor, Vars};

// #[derive(Clone, Debug, PartialEq, Empty, FromValue, ProcessValue, ToValue)]
struct FreeformRoot {
    // #[metastructure(pii = "true")]
    value: Annotated<String>,
}

macro_rules! assert_text_rule {
    (
        rule = $rule:expr,
        input = $input:expr,
        output = $output:expr,
        remarks = $remarks:expr,
    ) => {{
        let config = PiiConfig {
            rules: BTreeMap::new(),
            vars: Vars::default(),
            applications: {
                let mut map = BTreeMap::new();
                map.insert(ValueType::String.into(), vec![$rule.to_string()]);
                map
            },
        };

        let input = $input.to_string();
        let mut processor = PiiProcessor::new(&config);
        let mut root = Annotated::new(FreeformRoot {
            value: Annotated::new(input),
        });

        // process_value(&mut root, &mut processor, ProcessingState::root()).unwrap();

        let root = root.0.unwrap();
        assert_eq!(root.value.value().unwrap(), $output);

        let remarks: Vec<Remark> = $remarks;
        assert_eq!(
            root.value.meta().iter_remarks().collect::<Vec<_>>(),
            remarks.iter().collect::<Vec<_>>()
        );
    }};
}

#[test]
fn test_ipv4() {
    assert_text_rule!(
        rule = "@ip",
        input = "before 127.0.0.1 after",
        output = "before [ip] after",
        remarks = vec![Remark::with_range(RemarkType::Substituted, "@ip", (7, 11))],
    );
    assert_text_rule!(
        rule = "@ip:replace",
        input = "before 127.0.0.1 after",
        output = "before [ip] after",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@ip:replace",
            (7, 11)
        )],
    );
    assert_text_rule!(
        rule = "@ip:hash",
        input = "before 127.0.0.1 after",
        output = "before AE12FE3B5F129B5CC4CDD2B136B7B7947C4D2741 after",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@ip:hash",
            (7, 47)
        )],
    );
}

#[test]
fn test_ipv6() {
    assert_text_rule!(
        rule = "@ip",
        input = "before ::1 after",
        output = "before [ip] after",
        remarks = vec![Remark::with_range(RemarkType::Substituted, "@ip", (7, 11))],
    );
    assert_text_rule!(
        rule = "@ip:replace",
        input = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
        output = "[[ip]]",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@ip:replace",
            (1, 5)
        )],
    );
    assert_text_rule!(
        rule = "@ip:hash",
        input = "before 2001:0db8:85a3:0000:0000:8a2e:0370:7334 after",
        output = "before 8C3DC9BEED9ADE493670547E24E4E45EDE69FF03 after",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@ip:hash",
            (7, 47)
        )],
    );
    assert_text_rule!(
        rule = "@ip",
        input = "foo::1",
        output = "foo::1",
        remarks = vec![],
    );
}

#[test]
fn test_imei() {
    assert_text_rule!(
        rule = "@imei",
        input = "before 356938035643809 after",
        output = "before [imei] after",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@imei",
            (7, 13)
        )],
    );
    assert_text_rule!(
        rule = "@imei:replace",
        input = "before 356938035643809 after",
        output = "before [imei] after",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@imei:replace",
            (7, 13)
        )],
    );
    assert_text_rule!(
        rule = "@imei:hash",
        input = "before 356938035643809 after",
        output = "before 3888108AA99417402969D0B47A2CA4ECD2A1AAD3 after",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@imei:hash",
            (7, 47)
        )],
    );
}

#[test]
fn test_mac() {
    assert_text_rule!(
        rule = "@mac",
        input = "ether 4a:00:04:10:9b:50",
        output = "ether 4a:00:04:**:**:**",
        remarks = vec![Remark::with_range(RemarkType::Masked, "@mac", (6, 23))],
    );
    assert_text_rule!(
        rule = "@mac:mask",
        input = "ether 4a:00:04:10:9b:50",
        output = "ether 4a:00:04:**:**:**",
        remarks = vec![Remark::with_range(RemarkType::Masked, "@mac:mask", (6, 23))],
    );
    assert_text_rule!(
        rule = "@mac:replace",
        input = "ether 4a:00:04:10:9b:50",
        output = "ether [mac]",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@mac:replace",
            (6, 11)
        )],
    );
    assert_text_rule!(
        rule = "@mac:hash",
        input = "ether 4a:00:04:10:9b:50",
        output = "ether 6220F3EE59BF56B32C98323D7DE43286AAF1F8F1",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@mac:hash",
            (6, 46)
        )],
    );
}

#[test]
fn test_uuid() {
    assert_text_rule!(
        rule = "@uuid",
        input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1",
        output = "user id ********-****-****-****-************",
        remarks = vec![Remark::with_range(RemarkType::Masked, "@uuid", (8, 44))],
    );
    assert_text_rule!(
        rule = "@uuid:mask",
        input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1",
        output = "user id ********-****-****-****-************",
        remarks = vec![Remark::with_range(
            RemarkType::Masked,
            "@uuid:mask",
            (8, 44)
        )],
    );
    assert_text_rule!(
        rule = "@uuid:hash",
        input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1",
        output = "user id EBFA4DBDAA4A619F10B6DE2ABB630DE0122F5CDB",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@uuid:hash",
            (8, 48)
        )],
    );
    assert_text_rule!(
        rule = "@uuid:replace",
        input = "user id ceee0822-ed8f-4622-b2a3-789e73e75cd1",
        output = "user id [uuid]",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@uuid:replace",
            (8, 14)
        )],
    );
}

#[test]
fn test_email() {
    assert_text_rule!(
        rule = "@email",
        input = "John Appleseed <john@appleseed.com>",
        output = "John Appleseed <[email]>",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@email",
            (16, 23)
        )],
    );
    assert_text_rule!(
        rule = "@email:replace",
        input = "John Appleseed <john@appleseed.com>",
        output = "John Appleseed <[email]>",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@email:replace",
            (16, 23)
        )],
    );
    assert_text_rule!(
        rule = "@email:mask",
        input = "John Appleseed <john@appleseed.com>",
        output = "John Appleseed <****@*********.***>",
        remarks = vec![Remark::with_range(
            RemarkType::Masked,
            "@email:mask",
            (16, 34)
        )],
    );
    assert_text_rule!(
        rule = "@email:hash",
        input = "John Appleseed <john@appleseed.com>",
        output = "John Appleseed <33835528AC0FFF1B46D167C35FEAAA6F08FD3F46>",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@email:hash",
            (16, 56)
        )],
    );
}

#[test]
fn test_creditcard() {
    assert_text_rule!(
        rule = "@creditcard",
        input = "John Appleseed 4571234567890111!",
        output = "John Appleseed [creditcard]!",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@creditcard",
            (15, 27)
        )],
    );
    assert_text_rule!(
        rule = "@creditcard",
        input = "John Appleseed 4571 2345 6789 0111!",
        output = "John Appleseed [creditcard]!",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@creditcard",
            (15, 27)
        )],
    );
    assert_text_rule!(
        rule = "@creditcard",
        input = "John Appleseed 4571-2345-6789-0111!",
        output = "John Appleseed [creditcard]!",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@creditcard",
            (15, 27)
        )],
    );
    assert_text_rule!(
        rule = "@creditcard:mask",
        input = "John Appleseed 4571234567890111!",
        output = "John Appleseed ************0111!",
        remarks = vec![Remark::with_range(
            RemarkType::Masked,
            "@creditcard:mask",
            (15, 31)
        )],
    );
    assert_text_rule!(
        rule = "@creditcard:replace",
        input = "John Appleseed 4571234567890111!",
        output = "John Appleseed [creditcard]!",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@creditcard:replace",
            (15, 27)
        )],
    );
    assert_text_rule!(
        rule = "@creditcard:hash",
        input = "John Appleseed 4571234567890111!",
        output = "John Appleseed 68C796A9ED3FB51BF850A11140FCADD8E2D88466!",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@creditcard:hash",
            (15, 55)
        )],
    );
}

#[test]
fn test_pemkey_replace() {
    let input = "This is a comment I left on my key
            -----BEGIN EC PRIVATE KEY-----
MIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL
fojr3jkJIxFS07AoLQpsawOFZipxSQHieaAHBgUrgQQAI6GBiQOBhgAEAdaBHsBo
KBuPVQL2F6z57Xb034TRlpaarne/XGWaCsohtl3YYcFll3A7rV+wHudcKFSvrgjp
soH5cUhpnhOL9ujuAM7Ldk1G+11Mf7EZ5sjrLe81fSB8S16D2vjtxkf/+mmwwhlM
HdmUCGvfKiF2CodxyLon1XkK8pX+Ap86MbJhluqK
-----END EC PRIVATE KEY-----";

    let output = "This is a comment I left on my ke,

            -----BEGIN EC PRIVATE KEY-----
[pemkey]
-----END EC PRIVATE KEY-----";

    assert_text_rule!(
        rule = "@pemkey",
        input = input,
        output = output,
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@pemkey",
            (79, 87)
        )],
    );
}

#[test]
fn test_pemkey_hash() {
    let input = "This is a comment I left on my key

            -----BEGIN EC PRIVATE KEY-----
MIHbAgEBBEFbLvIaAaez3q0u6BQYMHZ28B7iSdMPPaODUMGkdorl3ShgTbYmzqGL
fojr3jkJIxFS07AoLQpsawOFZipxSQHieaAHBgUrgQQAI6GBiQOBhgAEAdaBHsBo
KBuPVQL2F6z57Xb034TRlpaarne/XGWaCsohtl3YYcFll3A7rV+wHudcKFSvrgjp
soH5cUhpnhOL9ujuAM7Ldk1G+11Mf7EZ5sjrLe81fSB8S16D2vjtxkf/+mmwwhlM
HdmUCGvfKiF2CodxyLon1XkK8pX+Ap86MbJhluqK
-----END EC PRIVATE KEY-----";

    let output = "This is a comment I left on my ke,

            -----BEGIN EC PRIVATE KEY-----
291134FEC77C62B11E2DC6D89910BB43157294BC
-----END EC PRIVATE KEY-----";

    assert_text_rule!(
        rule = "@pemkey:hash",
        input = input,
        output = output,
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@pemkey:hash",
            (79, 119)
        )],
    );
}

#[test]
fn test_urlauth() {
    assert_text_rule!(
        rule = "@urlauth",
        input = "https://username:password@example.com/",
        output = "https://[auth]@example.com/",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@urlauth",
            (8, 14)
        )],
    );
    assert_text_rule!(
        rule = "@urlauth:replace",
        input = "https://username:password@example.com/",
        output = "https://[auth]@example.com/",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@urlauth:replace",
            (8, 14)
        )],
    );
    assert_text_rule!(
        rule = "@urlauth:hash",
        input = "https://username:password@example.com/",
        output = "https://2F82AB8A5E3FAD655B5F81E3BEA30D2A14FEF2AC@example.com/",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@urlauth:hash",
            (8, 48)
        )],
    );
}

#[test]
fn test_usssn() {
    assert_text_rule!(
        rule = "@usssn",
        input = "Hi I'm Hilda and my SSN is 078-05-1120",
        output = "Hi I'm Hilda and my SSN is ***-**-****",
        remarks = vec![Remark::with_range(RemarkType::Masked, "@usssn", (27, 38))],
    );
    assert_text_rule!(
        rule = "@usssn:mask",
        input = "Hi I'm Hilda and my SSN is 078-05-1120",
        output = "Hi I'm Hilda and my SSN is ***-**-****",
        remarks = vec![Remark::with_range(
            RemarkType::Masked,
            "@usssn:mask",
            (27, 38)
        )],
    );
    assert_text_rule!(
        rule = "@usssn:replace",
        input = "Hi I'm Hilda and my SSN is 078-05-1120",
        output = "Hi I'm Hilda and my SSN is [us-ssn]",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@usssn:replace",
            (27, 35)
        )],
    );
    assert_text_rule!(
        rule = "@usssn:hash",
        input = "Hi I'm Hilda and my SSN is 078-05-1120",
        output = "Hi I'm Hilda and my SSN is 01328BB9C55B35F354FBC7B60CADAC789DC2035C",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@usssn:hash",
            (27, 67)
        )],
    );
}

#[test]
fn test_userpath() {
    assert_text_rule!(
        rule = "@userpath",
        input = "C:\\Users\\mitsuhiko\\Desktop",
        output = "C:\\Users\\[user]\\Desktop",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@userpath",
            (9, 15)
        )],
    );
    assert_text_rule!(
        rule = "@userpath:replace",
        input = "File in /Users/mitsuhiko/Development/sentry-stripping",
        output = "File in /Users/[user]/Development/sentry-stripping",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@userpath:replace",
            (15, 21)
        )],
    );
    assert_text_rule!(
        rule = "@userpath:replace",
        input = "C:\\Windows\\Profiles\\Armin\\Temp",
        output = "C:\\Windows\\Profiles\\[user]\\Temp",
        remarks = vec![Remark::with_range(
            RemarkType::Substituted,
            "@userpath:replace",
            (20, 26)
        )],
    );
    assert_text_rule!(
        rule = "@userpath:hash",
        input = "File in /Users/mitsuhiko/Development/sentry-stripping",
        output =
            "File in /Users/A8791A1A8D11583E0200CC1B9AB971B4D78B8A69/Development/sentry-stripping",
        remarks = vec![Remark::with_range(
            RemarkType::Pseudonymized,
            "@userpath:hash",
            (15, 55)
        ),],
    );
}
