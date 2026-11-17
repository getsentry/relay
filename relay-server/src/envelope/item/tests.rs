use crate::managed::Counted;
use relay_quotas::DataCategory;
#[cfg(feature = "processing")]
use uuid::Uuid;

#[test]
fn misplaced_attachment_headers_do_not_change_item_classification() {
    use super::*;
    for ty in ["event", "user_report", "profile"] {
        for attachment_type in ["event.minidump", "future_attachment"] {
            let header = serde_json::json!({
                "type": ty, "attachment_type": attachment_type,
                "filename": "preserved.bin", "length": 2,
            });
            let (item, _) = Item::parse(Bytes::from(format!("{header}\n{{}}\n"))).unwrap();
            assert!(!matches!(item, Item::Attachment(_)));
            assert!(!item.is_unknown());
            assert_eq!(serde_json::to_value(item.wire_headers()).unwrap(), header);
        }
    }
    let (attachment, _) = Item::parse(Bytes::from_static(
        b"{\"type\":\"attachment\",\"attachment_type\":\"future_attachment\"}\nbody\n",
    ))
    .unwrap();
    assert!(attachment.is_unknown());
}

use crate::integrations::LogsIntegration;
use crate::integrations::OtelFormat;

use super::*;

#[test]
fn payload_classification_and_routing_are_separate_facts() {
    // These are the pre-refactor routing/classification contracts. ReplayEvent's
    // routing is intentionally preserved even though its quota is independent.
    for (ty, payload, routed) in [
        (ItemType::Event, true, true),
        (ItemType::Transaction, true, true),
        (ItemType::Security, true, true),
        (ItemType::RawSecurity, true, true),
        (ItemType::UnrealReport, true, true),
        (ItemType::UserReportV2, true, true),
        (ItemType::Attachment, false, true),
        (ItemType::FormData, false, true),
        (ItemType::UserReport, false, true),
        (ItemType::Profile, false, true),
        (ItemType::ReplayEvent, false, true),
        (ItemType::ReplayRecording, false, false),
        (ItemType::ReplayVideo, false, false),
        (ItemType::ProfileChunk, false, false),
        (ItemType::Span, false, false),
        (ItemType::Session, false, false),
        (ItemType::Sessions, false, false),
        (ItemType::MetricBuckets, false, false),
        (ItemType::ClientReport, false, false),
        (ItemType::CheckIn, false, false),
        (ItemType::Log, false, false),
        (ItemType::TraceMetric, false, false),
        (ItemType::Integration, false, false),
        (ItemType::Unknown("future".into()), false, false),
    ] {
        let item = Item::new(ty);
        assert_eq!(item.carries_event_payload(), payload, "{:?}", item.ty());
        assert_eq!(item.routes_with_event(), routed, "{:?}", item.ty());
    }
    for parent in [
        AttachmentParentType::Event,
        AttachmentParentType::Span,
        AttachmentParentType::Trace,
    ] {
        let mut dump = AttachmentItem::new();
        dump.set_attachment_type(AttachmentType::Minidump);
        if !matches!(parent, AttachmentParentType::Event) {
            dump.data_mut()
                .set_payload(ContentType::TraceAttachment, "{}");
            if matches!(parent, AttachmentParentType::Span) {
                dump.set_parent_id(Some(ParentId::SpanId(None)));
            }
        }
        assert_eq!(
            dump.carries_event_payload(),
            matches!(parent, AttachmentParentType::Event)
        );
    }
}

#[test]
fn typed_items_preserve_wire_headers_and_variant_identity() {
    // Preserve unknown tags and misplaced or invalid headers for forwarding.
    for tag in ["transaction", "profile", "attachment", "future_item"] {
        let header = serde_json::json!({
            "type": tag, "length": 2, "span_count": "invalid", "future_header": [1, 2],
        });
        let wire = format!("{header}\n{{}}\n");
        let (item, consumed) = Item::parse(Bytes::from(wire.clone())).unwrap();
        assert_eq!(consumed, wire.len());
        assert_eq!(item.ty().as_str(), tag);
        assert_eq!(serde_json::to_value(item.wire_headers()).unwrap(), header);
        assert_eq!(item.data().payload(), "{}");
        match item {
            Item::Transaction(transaction) => assert_eq!(transaction.span_count(), None),
            Item::Profile(profile) => assert_eq!(profile.profile_type(), None),
            Item::Attachment(attachment) => assert_eq!(attachment.attachment_body_size(), 2),
            Item::Unknown(_) => {}
            _ => unreachable!(),
        }
    }
    // Swapping common storage cannot turn a ProfileItem into a TransactionItem.
    let mut transaction = TransactionItem::new();
    transaction.set_span_count(Some(3));
    let mut profile = ProfileItem::new();
    std::mem::swap(transaction.data_mut(), profile.data_mut());
    assert_eq!(transaction.span_count(), None);
    let transaction = Item::from(transaction);
    assert_eq!(
        serde_json::to_value(transaction.wire_headers()).unwrap()["type"],
        "transaction"
    );
    let profile = Item::from(profile);
    assert_eq!(
        serde_json::to_value(profile.wire_headers()).unwrap()["type"],
        "profile"
    );
}

#[test]
fn item_count_headers_are_only_read_by_supported_variants() {
    for (tag, supported) in [
        ("transaction", false),
        ("event", false),
        ("integration", false),
        ("session", true),
        ("profile", true),
        ("log", true),
        ("future_item", true),
    ] {
        for (header, count) in [
            ("null", None),
            ("0", Some(0)),
            ("3", Some(3)),
            ("\"invalid\"", None),
        ] {
            let wire = format!("{{\"type\":\"{tag}\",\"item_count\":{header}}}\n{{}}\n");
            let (item, _) = Item::parse(Bytes::from(wire)).unwrap();
            assert_eq!(
                item.item_count(),
                if supported { count } else { None },
                "{tag}: {header}"
            );
        }
    }
}

#[test]
fn concrete_accounting_preserves_container_counts() {
    let (profile, _) = Item::parse(Bytes::from_static(
        b"{\"type\":\"profile\",\"platform\":\"python\",\"item_count\":3}\n{}\n",
    ))
    .unwrap();
    let Item::Profile(profile) = profile else {
        panic!("expected a profile");
    };
    assert_eq!(
        profile.quantities().as_slice(),
        [
            (DataCategory::Profile, 3),
            (DataCategory::ProfileIndexed, 3),
            (DataCategory::ProfileBackend, 3),
        ]
    );
    let mut transaction = TransactionItem::new();
    // Transactions never support containers: `1` still means one transaction.
    transaction.data_mut().set_payload(ContentType::Json, "{}");
    transaction
        .data_mut()
        .set_test_header(ItemHeaderKey::ItemCount, Some(99u32));
    transaction.set_span_count(Some(2));
    assert_eq!(
        transaction.quantities().as_slice(),
        [
            (DataCategory::Transaction, 1),
            (DataCategory::TransactionIndexed, 1),
            (DataCategory::Span, 3),
            (DataCategory::SpanIndexed, 3),
        ]
    );
}

#[test]
fn test_item_quantities_preserve_order_and_zero_counts() {
    let mut span = SpanItem::new();
    span.data_mut().set_payload(ContentType::Json, "{}");
    span.data_mut()
        .set_test_header(ItemHeaderKey::ItemCount, Some(3u32));
    let mut log = LogItem::new();
    log.data_mut().set_payload(ContentType::Json, "");
    log.data_mut()
        .set_test_header(ItemHeaderKey::ItemCount, Some(0u32));
    let mut profile = ProfileItem::new();
    profile.set_platform("python".into());
    let mut transaction = TransactionItem::new();
    transaction.set_span_count(Some(2));
    let mut attachment = AttachmentItem::new();
    attachment.data_mut().set_rate_limited(true);
    let mut minidump = AttachmentItem::new();
    minidump.set_attachment_type(AttachmentType::Minidump);
    minidump
        .data_mut()
        .set_payload(ContentType::OctetStream, "MDMP");
    let mut trace_attachment = AttachmentItem::new();
    trace_attachment
        .data_mut()
        .set_payload(ContentType::TraceAttachment, "{}body");
    trace_attachment.set_meta_length(2);
    let mut nintendo = AttachmentItem::new();
    nintendo.set_attachment_type(AttachmentType::NintendoSwitchDyingMessage);
    nintendo
        .data_mut()
        .set_payload(ContentType::OctetStream, "dying message");
    let mut misplaced = UserReportItem::new();
    misplaced.data_mut().set_test_header(
        crate::envelope::ItemHeaderKey::AttachmentType,
        Some(AttachmentType::NintendoSwitchDyingMessage),
    );
    let cases: [(Item, Vec<_>); 11] = [
        (nintendo.into(), vec![]),
        (misplaced.into(), vec![(DataCategory::UserReportV2, 1)]),
        (UnknownItem::new("future".into()).into(), vec![]),
        (attachment.into(), vec![]),
        // Implicit events belong to envelope accounting, not each crash attachment.
        (
            minidump.into(),
            vec![
                (DataCategory::Attachment, 4),
                (DataCategory::AttachmentItem, 1),
            ],
        ),
        (
            trace_attachment.into(),
            vec![
                (DataCategory::Attachment, 4),
                (DataCategory::AttachmentItem, 1),
            ],
        ),
        (
            UserReportItem::new().into(),
            vec![(DataCategory::UserReportV2, 1)],
        ),
        (
            span.into(),
            vec![(DataCategory::Span, 3), (DataCategory::SpanIndexed, 3)],
        ),
        (
            log.into(),
            vec![(DataCategory::LogItem, 0), (DataCategory::LogByte, 1)],
        ),
        (
            profile.into(),
            vec![
                (DataCategory::Profile, 1),
                (DataCategory::ProfileIndexed, 1),
                (DataCategory::ProfileBackend, 1),
            ],
        ),
        (
            transaction.into(),
            vec![
                (DataCategory::Transaction, 1),
                (DataCategory::TransactionIndexed, 1),
                (DataCategory::Span, 3),
                (DataCategory::SpanIndexed, 3),
            ],
        ),
    ];
    for (item, expected) in cases {
        assert_eq!(item.quantities().as_slice(), expected.as_slice());
    }
}

#[test]
fn test_item_is() {
    let item = Item::from(TransactionItem::new());
    assert!(item.is::<TransactionItem>());
    assert!(!item.is::<EventItem>());
    assert!(!item.is::<UnknownItem>());

    let unknown = Item::from(UnknownItem::new("future".into()));
    assert!(unknown.is::<UnknownItem>());
    assert!(!unknown.is::<TransactionItem>());
}

#[test]
fn test_item_downcast_ref() {
    let mut transaction = TransactionItem::new();
    transaction.set_span_count(Some(3));
    let item = Item::from(transaction);

    let borrowed = item.downcast_ref::<TransactionItem>().unwrap();
    assert_eq!(borrowed.span_count(), Some(3));
    let Item::Transaction(original) = &item else {
        unreachable!();
    };
    assert!(std::ptr::eq(borrowed, original));
    assert!(item.downcast_ref::<AttachmentItem>().is_none());

    let unknown = Item::from(UnknownItem::new("future".into()));
    assert!(unknown.downcast_ref::<UnknownItem>().is_some());
    assert!(unknown.downcast_ref::<TransactionItem>().is_none());
}

#[test]
fn test_item_empty() {
    let item = Item::Attachment(AttachmentItem::new());

    assert_eq!(item.data().payload(), Bytes::new());
    assert_eq!(item.data().len(), 0);
    assert!(item.data().is_empty());

    assert_eq!(item.data().content_type(), None);
}

#[test]
fn test_item_set_payload() {
    let mut item = EventItem::new();

    let payload = Bytes::from(&br#"{"event_id":"3adcb99a1be84a5d8057f2eb9a0161ce"}"#[..]);
    item.data_mut()
        .set_payload(ContentType::Json, payload.clone());

    // Payload
    assert_eq!(item.data().payload(), payload);
    assert_eq!(item.data().len(), payload.len());
    assert!(!item.data().is_empty());

    // Meta data
    assert_eq!(item.data().content_type(), Some(ContentType::Json));
}

#[test]
fn test_item_set_content_type() {
    let mut item = Item::new(ItemType::Attachment);

    let payload = Bytes::from_static(b"payload");
    item.data_mut()
        .set_payload(ContentType::Json, payload.clone());
    item.data_mut().set_content_type(ContentType::OctetStream);

    assert_eq!(item.data().content_type(), Some(ContentType::OctetStream));
    assert_eq!(item.data().payload(), payload);
    assert_eq!(
        item.data().headers().get::<usize>(ItemHeaderKey::Length),
        Some(payload.len())
    );
}

#[test]
#[cfg(feature = "processing")]
fn test_item_set_routing_hint() {
    let uuid = Uuid::parse_str("8a4ab00f-fba2-4f7b-a164-b58199d55c95").unwrap();

    let mut item = EventItem::new();
    item.data_mut().set_routing_hint(uuid);

    assert_eq!(item.data().routing_hint(), Some(uuid));
}

#[test]
fn test_item_source_quantities() {
    let mut item = MetricBucketsItem::new();
    assert!(item.data().source_quantities().is_none());

    let source_quantities = SourceQuantities {
        transactions: 12,
        ..Default::default()
    };
    item.data_mut().set_source_quantities(source_quantities);

    assert_eq!(item.data().source_quantities(), Some(source_quantities));
}

#[test]
fn test_internal_content_type_does_parse() {
    let (item, _) = Item::parse(Bytes::from_static(concat!(
            r#"{"type":"attachment","content_type":"application/vnd.sentry.integration.otel.logs+json","length":5}"#,
            "\n",
            "12345"
        ).as_bytes()))
        .unwrap();

    assert_eq!(
        item.data().content_type(),
        Some(ContentType::Integration(Integration::Logs(
            LogsIntegration::OtelV1 {
                format: OtelFormat::Json
            }
        )))
    );
    assert!(!matches!(item, Item::Integration(_)));
}

#[test]
fn test_item_parse_length_overflow_does_not_panic() {
    let header = format!(r#"{{"type":"attachment","length":{}}}"#, usize::MAX)
        .into_bytes()
        .into();

    let err = Item::parse(header).unwrap_err();

    std::assert_matches!(err, EnvelopeError::UnexpectedEof);
}

#[test]
fn test_item_type_names() {
    assert_eq!(ItemType::Span.name(), "span");
    assert_eq!(ItemType::Unknown("test".to_owned()).name(), "unknown");
    assert_eq!(ItemType::Span.as_str(), "span");
    assert_eq!(ItemType::Unknown("test".to_owned()).as_str(), "test");
    assert_eq!(&ItemType::Span.to_string(), "span");
    assert_eq!(&ItemType::Unknown("test".to_owned()).to_string(), "test");
}

#[test]
fn test_item_headers() {
    let mut headers: ItemHeaders = serde_json::from_str(
        r#"{
            "type":"attachment",
            "length":42,
            "content_type": "application/json",
            "filename":"test.txt"
        }"#,
    )
    .unwrap();

    assert_eq!(headers.ty(), &ItemType::Attachment);
    assert!(headers.contains(ItemHeaderKey::Length));
    assert_eq!(headers.get(ItemHeaderKey::Length), Some(42u32));
    assert_eq!(headers.get(ItemHeaderKey::Filename), Some("test.txt"));
    assert_eq!(
        headers.get(ItemHeaderKey::ContentType),
        Some("application/json")
    );

    headers.set_or_remove(ItemHeaderKey::Length, Some(1337u32));
    assert_eq!(headers.get(ItemHeaderKey::Length), Some(1337));
    headers.set_or_remove(ItemHeaderKey::Length, None::<u32>);
    assert!(!headers.contains(ItemHeaderKey::Length));

    let s = serde_json::to_string(&headers).unwrap();
    insta::assert_snapshot!(s, @r#"{"type":"attachment","content_type":"application/json","filename":"test.txt"}"#);
}

#[test]
fn test_item_headers_unknown() {
    let (item, _) = Item::parse(Bytes::from_static(
        concat!(
            r#"{"type":"attachment","unknown1":"foo","unknown2":"bar","length":5}"#,
            "\n",
            "12345"
        )
        .as_bytes(),
    ))
    .unwrap();

    insta::assert_debug_snapshot!(&item.wire_headers(), @r#"
        {
            "type": Attachment,
            Length: 5,
            "unknown1": "foo",
            "unknown2": "bar",
        }
        "#);

    // Round trip -> serializes the headers again.
    let s = serde_json::to_string(&item.wire_headers()).unwrap();
    insta::assert_snapshot!(s, @r#"{"type":"attachment","length":5,"unknown1":"foo","unknown2":"bar"}"#);
}

#[test]
fn attachment_type_defaults_preserve_wire_headers() {
    use super::*;
    use serde_json::json;

    for (declared, expected, unknown) in [
        (None, AttachmentType::Attachment, false),
        (
            Some(json!("event.attachment")),
            AttachmentType::Attachment,
            false,
        ),
        (
            Some(json!("event.minidump")),
            AttachmentType::Minidump,
            false,
        ),
        (
            Some(json!("future.attachment")),
            AttachmentType::Attachment,
            true,
        ),
        (Some(json!(42)), AttachmentType::Attachment, true),
        (Some(json!(null)), AttachmentType::Attachment, true),
    ] {
        let mut header = json!({"type": "attachment", "length": 4});
        if let Some(declared) = declared {
            header["attachment_type"] = declared;
        }
        let (item, _) = Item::parse(Bytes::from(format!("{header}\nbody\n"))).unwrap();
        let attachment = item.downcast_ref::<AttachmentItem>().unwrap();
        assert_eq!(attachment.attachment_type(), expected, "{header}");
        assert_eq!(item.is_unknown(), unknown, "{header}");
        assert_eq!(serde_json::to_value(item.wire_headers()).unwrap(), header);
    }
}

#[test]
fn attachment_type_defaults_preserve_nintendo_detection() {
    use super::*;
    use crate::constants::{NNSWITCH_DYING_MESSAGE_FILENAME, NNSWITCH_SENTRY_MAGIC};
    use serde_json::json;

    for (declared, expected, unknown) in [
        (None, AttachmentType::NintendoSwitchDyingMessage, false),
        (
            Some(json!("future.attachment")),
            AttachmentType::NintendoSwitchDyingMessage,
            true,
        ),
        (
            Some(json!("event.attachment")),
            AttachmentType::Attachment,
            false,
        ),
    ] {
        let mut header = json!({
            "type": "attachment", "filename": NNSWITCH_DYING_MESSAGE_FILENAME,
            "length": NNSWITCH_SENTRY_MAGIC.len(),
        });
        if let Some(declared) = declared {
            header["attachment_type"] = declared;
        }
        let mut wire = format!("{header}\n").into_bytes();
        wire.extend_from_slice(NNSWITCH_SENTRY_MAGIC);
        wire.push(b'\n');
        let (item, _) = Item::parse(Bytes::from(wire)).unwrap();
        assert_eq!(
            item.downcast_ref::<AttachmentItem>()
                .unwrap()
                .attachment_type(),
            expected
        );
        assert_eq!(item.is_unknown(), unknown);
        assert_eq!(serde_json::to_value(item.wire_headers()).unwrap(), header);
    }

    let mut attachment = AttachmentItem::new();
    attachment.set_filename(NNSWITCH_DYING_MESSAGE_FILENAME);
    attachment
        .data_mut()
        .set_payload_without_content_type("not a dying message");
    assert_eq!(attachment.attachment_type(), AttachmentType::Attachment);
    attachment.set_filename("ordinary.dat");
    attachment
        .data_mut()
        .set_payload_without_content_type(NNSWITCH_SENTRY_MAGIC);
    assert_eq!(attachment.attachment_type(), AttachmentType::Attachment);
}
