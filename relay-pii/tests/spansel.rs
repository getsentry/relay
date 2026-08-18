//! Scratch: does `encrypt` redaction work on span attributes?
use relay_event_schema::processor::ValueType;
use relay_event_schema::protocol::SpanV2;
use relay_pii::{PiiConfig, eap};
use relay_protocol::Annotated;

fn span() -> Annotated<SpanV2> {
    Annotated::<SpanV2>::from_json(r#"{
      "trace_id": "ff62a8b040f340bda5d830223def1d81",
      "span_id": "b0429c44b67a3eb1", "name": "GET /users", "status": "ok",
      "start_timestamp": 1700000000.0, "end_timestamp": 1700000001.0,
      "attributes": {"user.email": {"type":"string","value":"ivy@example.com"}}
    }"#).unwrap()
}

fn show(method: serde_json::Value) {
    let cfg: PiiConfig = serde_json::from_value(serde_json::json!({
        "vars": {"publicKey": "d9KbvrQ9LZm0PMF2fC2sFbdO3E4KL35k2g1QVxdgeko="},
        "rules": {"r": {"type": "anything", "redaction": method.clone()}},
        "applications": {"'user.email'.value": ["r"]}
    })).unwrap();
    let mut s = span();
    eap::scrub(ValueType::Span, &mut s, Some(&cfg), None).unwrap();
    let v: serde_json::Value = serde_json::from_str(&s.to_json().unwrap()).unwrap();
    println!("  {:<42} -> {}", method.to_string(), v["attributes"]["user.email"]["value"]);
}

#[test]
fn probe() {
    show(serde_json::json!({"method": "replace", "text": "[X]"}));
    show(serde_json::json!({"method": "mask"}));
    show(serde_json::json!({"method": "remove"}));
    show(serde_json::json!({"method": "encrypt"}));
}
