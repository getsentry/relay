use std::collections::HashMap;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

use relay_base_schema::events::EventType;
use relay_base_schema::project::ProjectId;
use std::process::Stdio;

struct Proxy {
    pub listening_url: String,
    _process: BackgroundProcess,
}

impl Proxy {
    pub fn new(listening_address: &str) -> Self {
        // Define the path to the proxy executable
        let executable_path = "/Users/tor/prog/rust/proxy/target/debug/proxy";
        let _process = BackgroundProcess::new(executable_path, &[listening_address]);

        let mut child = Command::new(executable_path)
            .args([listening_address])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        std::thread::sleep(std::time::Duration::from_secs(1));
        let stdout = child.stdout.take().unwrap();

        let reader = BufReader::new(stdout);
        let line = dbg!(reader.lines().next()).unwrap().unwrap();

        let (_, address) = line.split_once("http").unwrap();
        let listening_address = format!("http{}", address.trim());

        let _process = BackgroundProcess { child: Some(child) };

        Proxy {
            listening_url: listening_address,
            _process,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::net::TcpStream;

    use relay_base_schema::project::{ProjectId, ProjectKey};
    use relay_server::envelope::ItemType;

    use relay_server::envelope::AttachmentType;
    use serde_json::json;

    use crate::mini_sentry::MiniSentry;
    use crate::relay::Relay;
    use crate::test_envelopy::generate_transaction_item;
    use crate::test_envelopy::Proxy;
    use crate::test_envelopy::{RawEnvelope, RawItem};
    use crate::{create_error_envelope, EnvelopeBuilder, StateBuilder};
    use relay_server::actors::project::ProjectState;

    /// Same as test_envelope but we first go through the new proxy
    #[test]
    fn test_proxy() {
        let project_state = StateBuilder::new();
        let x: ProjectState = StateBuilder::new().into();
        dbg!(x);
        return;
        let sentry = MiniSentry::new().add_project_state(project_state);
        let inner_relay = Relay::new(&sentry);
        let outer_relay = Relay::new(&inner_relay);
        let proxy = Proxy::new(&outer_relay.envelope_url(ProjectId(42)));

        for _ in 0..15 {
            let envelope = RawEnvelope::new(sentry.dsn_public_key())
                .add_item_from_json(json!({"message": "hello world"}), ItemType::Event);

            outer_relay.send_envelope_to_url(envelope, &proxy.listening_url);
        }

        sentry
            .captured_envelopes()
            .wait_for_envelope(20)
            .assert_item_qty(1)
            .assert_all_item_types(ItemType::Event)
            .assert_logentries("hello world");
    }

    #[test]
    fn test_envelope() {
        let project_state = StateBuilder::new();
        let sentry = MiniSentry::new().add_project_state(project_state);
        let inner_relay = Relay::new(&sentry);
        let outer_relay = Relay::new(&inner_relay);

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
            .add_item_from_json(json!({"message": "hello world"}), ItemType::Event);

        outer_relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(5)
            .assert_item_qty(1)
            .assert_all_item_types(ItemType::Event)
            .assert_logentries("hello world");
    }

    #[test]
    fn test_envelope_close_connection() {
        let project_id = ProjectId(42);
        let sentry = MiniSentry::new().add_project_state(StateBuilder::new());
        let dsn_key = sentry.get_dsn_public_key(project_id);
        let relay = Relay::builder(&sentry).build();
        let server_address = relay.server_address();

        let body_template = |dsn_key: ProjectKey| {
            format!(
                r#"{{"event_id": "9ec79c33ec9942ab8353589fcb2e04dc","dsn": "https://{}:@sentry.io/42"}}
{{"type":"attachment","length":11,"content_type":"text/plain","filename":"hello.txt"}}
Hello World
"#,
                dsn_key
            )
        };

        let num_iterations = 10;

        for _ in 0..num_iterations {
            let mut stream =
                TcpStream::connect(server_address).expect("Failed to connect to server");
            let body_template = body_template(dsn_key);
            let req = format!(
                "POST /api/42/envelope/ HTTP/1.1\r\nContent-Length: {}\r\n\r\n{}",
                body_template.len(),
                body_template
            );

            stream.write_all(req.as_bytes()).unwrap();
            std::thread::sleep(std::time::Duration::from_micros(10));
            drop(stream);
        }
        std::thread::sleep(std::time::Duration::from_millis(500));
        sentry
            .captured_envelopes()
            .assert_envelope_qty(num_iterations);
    }

    #[test]
    fn test_envelope_empty() {
        let sentry = MiniSentry::new().add_basic_project_state();
        let relay = Relay::builder(&sentry).build();
        let envelope = RawEnvelope::new(sentry.dsn_public_key());
        relay.send_envelope(envelope);
        sentry.captured_envelopes().wait(2).assert_empty();
    }

    #[test]
    fn test_envelope_without_header() {
        let sentry = MiniSentry::new().add_basic_project_state();
        let relay = Relay::builder(&sentry).build();
        let dsn = relay.get_dsn(sentry.public_key());

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
            .add_http_header("X-Sentry-Auth", "")
            .add_header("dsn", &dsn)
            .add_event(r#""message":"Hello, World!""#);

        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_envelope(20)
            .assert_logentries("Hello, World!");
    }

    #[test]
    fn test_unknown_item() {
        let sentry = MiniSentry::new().add_basic_project_state();
        let relay = Relay::new(&sentry);

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
            .add_item("something", ItemType::Unknown("invalid_unknown".into()))
            .add_attachment(
                "something",
                AttachmentType::Unknown("attachment_unknown".into()),
            );

        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait_for_n_envelope(2, 3)
            .assert_envelope_qty(2)
            .assert_contains_item_headers(&[
                ("type", Some("invalid_unknown")),
                ("attachment_type", None),
            ])
            .assert_contains_item_headers(&[
                ("type", "attachment"),
                ("attachment_type", "attachment_unknown"),
            ]);
    }

    #[test]
    fn test_drop_unknown_item() {
        let sentry = MiniSentry::new().add_basic_project_state();
        let relay = Relay::builder(&sentry)
            .set_accept_unknown_items(false)
            .build();

        let envelope = RawEnvelope::new(sentry.dsn_public_key())
            .add_attachment("something", None)
            .add_attachment(
                "something",
                AttachmentType::Unknown("attachment_unknown".into()),
            )
            .add_item("something", ItemType::Unknown("invalid_unknown".into()));

        relay.send_envelope(envelope);

        sentry
            .captured_envelopes()
            .wait(2)
            .assert_envelope_qty(1)
            .assert_all_item_types(ItemType::Attachment);
    }

    #[test]
    fn test_normalize_measurement_interface() {
        todo!("need to figure out consumers first")
    }

    #[test]
    fn test_empty_measurement_interface() {
        let sentry = MiniSentry::new().add_basic_project_state();
        let inner_relay = Relay::new(&sentry);
        let outer_relay = Relay::new(&inner_relay);

        let mut transaction_item = generate_transaction_item();
        transaction_item
            .as_object_mut()
            .unwrap()
            .insert("mesaurements".to_string(), json!({}));

        let envelope = RawEnvelope::new(sentry.dsn_public_key()).add_transaction(transaction_item);
        outer_relay.send_envelope(envelope);
        sentry
            .captured_envelopes()
            .wait_for_envelope(5)
            .assert_item_qty(1)
            .assert_contains_transaction_value("/organizations/:orgId/performance/:eventSlug/");
    }

    #[test]
    fn test_strip_measurement_interface() {
        todo!("consumer stuff")
    }

    #[test]
    fn test_sample_rates() {
        let sentry = MiniSentry::new().add_basic_project_state();
        let inner_relay = Relay::new(&sentry);
        let outer_relay = Relay::new(&inner_relay);
        let sample_rates = json!([
            {"id": "client_sampler", "rate": 0.01},
            {"id": "dynamic_user", "rate": 0.05},
        ]);

        let item = RawItem::from_json(json!({"message": "hello world!"}))
            .add_header_from_json("sample_rates", sample_rates.clone())
            .set_type(ItemType::Event);

        let envelope = RawEnvelope::new(sentry.dsn_public_key()).add_raw_item(item);
        outer_relay.send_envelope(envelope);
        sentry
            .captured_envelopes()
            .wait_for_envelope(5)
            .assert_item_qty(1)
            .assert_contains_item_header_value("sample_rates", sample_rates);
    }
}

fn generate_transaction_item() -> serde_json::Value {
    json!({
        "event_id": "d2132d31b39445f1938d7e21b6bf0ec4",
        "type": "transaction",
        "transaction": "/organizations/:orgId/performance/:eventSlug/",
        "transaction_info": {"source": "route"},
        "start_timestamp": 1597976392.6542819,
        "timestamp": 1597976400.6189718,
        "contexts": {
            "trace": {
                "trace_id": "4C79F60C11214EB38604F4AE0781BFB2",
                "span_id": "FA90FDEAD5F74052",
                "type": "trace",
            }
        },
        "spans": [
            {
                "description": "<OrganizationContext>",
                "op": "react.mount",
                "parent_span_id": "8f5a2b8768cafb4e",
                "span_id": "bd429c44b67a3eb4",
                "start_timestamp": 1597976393.4619668,
                "timestamp": 1597976393.4718769,
                "trace_id": "ff62a8b040f340bda5d830223def1d81",
            }
        ],
    })
}

#[derive(Debug)]
enum PayLoad {
    Json(Value),
    Bytes(Vec<u8>),
}

#[derive(Debug)]
pub struct RawItem {
    headers: HashMap<String, Value>,
    payload: PayLoad,
}

impl RawItem {
    pub fn from_bytes(payload: impl Into<Vec<u8>>) -> Self {
        Self {
            headers: HashMap::default(),
            payload: PayLoad::Bytes(payload.into()),
        }
    }

    pub fn from_json(payload: Value) -> Self {
        Self {
            headers: HashMap::default(),
            payload: PayLoad::Json(payload),
        }
    }

    pub fn payload_string(&self) -> String {
        match &self.payload {
            PayLoad::Json(json) => json.to_string(),
            PayLoad::Bytes(bytes) => String::from_utf8(bytes.clone()).unwrap(),
        }
    }

    pub fn inferred_content_type(&self) -> &'static str {
        match self.payload {
            PayLoad::Json(_) => "application/json",
            PayLoad::Bytes(_) => "application/octet-stream",
        }
    }

    pub fn add_header_from_json(mut self, key: &str, val: Value) -> Self {
        self.headers.insert(key.into(), val);
        self
    }

    pub fn add_header(mut self, key: &str, val: &str) -> Self {
        self.headers.insert(key.into(), val.into());
        self
    }

    pub fn set_type(mut self, ty: ItemType) -> Self {
        let ty: String = ty.to_string();
        self.headers.insert("type".into(), ty.into());
        self
    }

    // Serialize the RawItem for inclusion in RawEnvelope's serialization
    pub fn serialize(&self) -> String {
        let mut headers = self.headers.clone();
        // Assuming payload length is desired in bytes, considering UTF-8 encoding
        let payload = self.payload_string();
        headers.insert(
            "length".to_owned(),
            serde_json::Value::Number(payload.len().into()),
        );

        headers.insert(
            "content_type".to_owned(),
            self.inferred_content_type().into(),
        );

        // Serialize headers to JSON string
        dbg!(&headers);
        let headers_json = serde_json::to_string(&headers).unwrap();
        dbg!(&headers_json);
        let serialized = format!("{}\n{}\n", headers_json, payload);
        dbg!(&serialized);

        let x = serialized.replace("\"25\"", "25");
        x
    }
}

#[derive(Debug)]
pub struct RawEnvelope {
    pub http_headers: HashMap<String, String>,
    pub project_id: ProjectId,
    pub dsn_public_key: ProjectKey,
    pub headers: HashMap<String, Value>,
    pub items: Vec<RawItem>,
}

impl RawEnvelope {
    pub fn new(dsn: ProjectKey) -> Self {
        Self {
            project_id: ProjectId(42),
            dsn_public_key: dsn,
            http_headers: Default::default(),
            headers: Default::default(),
            items: Default::default(),
        }
    }

    pub fn add_transaction_and_trace_info(
        self,
        public_key: ProjectKey,
        transaction: Option<&str>,
    ) -> Self {
        let (item, trace_id, _) = x_create_transaction_item(transaction);

        self.add_raw_item(item)
            .add_trace_info_simple(trace_id, public_key)
    }

    pub fn add_transaction_and_trace_info_not_simple(
        self,
        public_key: ProjectKey,
        transaction: Option<&str>,
        client_sample_rate: Option<f32>,
    ) -> Self {
        let (item, trace_id, _) = x_create_transaction_item(transaction);

        self.add_raw_item(item).add_trace_info(
            trace_id,
            public_key,
            None,
            None,
            client_sample_rate,
            transaction,
        )
    }

    pub fn add_trace_info_simple(mut self, trace_id: Uuid, public_key: ProjectKey) -> Self {
        let trace_info = json!({
            "trace_id": dbg!(trace_id.simple().to_string()),
            "public_key": public_key,
        });

        self.headers.insert("trace".into(), trace_info);
        self
    }

    pub fn add_error_event_with_trace_info(mut self, public_key: ProjectKey) -> Self {
        let (item, trace_id, event_id) = create_error_item(public_key);

        self.add_raw_item(item)
            .set_event_id(event_id)
            .add_trace_info(
                trace_id,
                public_key,
                Some(1.0),
                Some(true),
                Some(1.0),
                Some("/transaction"),
            )
    }

    pub fn add_trace_info(
        mut self,
        trace_id: Uuid,
        public_key: ProjectKey,
        release: Option<f32>,
        sampled: Option<bool>,
        client_sample_rate: Option<f32>,
        transaction: Option<&str>,
    ) -> Self {
        let mut x = json!({
            "trace_id": dbg!(trace_id.simple().to_string()),
            "public_key": public_key,
        });

        let trace_info = x.as_object_mut().unwrap();

        if let Some(release) = release {
            let release = format!("{:.1}", release);
            trace_info.insert("release".to_string(), release.into());
        }

        if let Some(sample_rate) = client_sample_rate {
            let sample_rate = format!("{:.5}", sample_rate);
            trace_info.insert("sample_rate".to_string(), sample_rate.into());
        }

        if let Some(transaction) = transaction {
            trace_info.insert("transaction".to_string(), transaction.into());
        }

        if let Some(sampled) = sampled {
            trace_info.insert("sampled".to_string(), sampled.into());
        }

        dbg!(&trace_info);

        self.headers.insert(
            "trace".into(),
            serde_json::Value::Object(trace_info.to_owned()),
        );
        self
    }

    pub fn set_project_id(mut self, id: ProjectId) -> Self {
        self.project_id = id;
        self
    }

    pub fn set_event_id(mut self, id: Uuid) -> Self {
        self.add_header("event_id", &id.to_string())
    }

    pub fn add_header(mut self, key: &str, val: &str) -> Self {
        self.headers.insert(key.into(), val.into());
        self
    }

    pub fn add_http_header(mut self, key: &str, val: &str) -> Self {
        self.http_headers.insert(key.into(), val.into());
        self
    }

    pub fn add_raw_item(mut self, item: RawItem) -> Self {
        self.items.push(item);
        self
    }

    pub fn add_item_from_json(mut self, payload: Value, ty: ItemType) -> Self {
        let item = RawItem::from_json(payload).set_type(ty);
        self.items.push(item);
        self
    }

    pub fn add_item(mut self, payload: &str, ty: ItemType) -> Self {
        let item = RawItem::from_json(payload.into()).set_type(ty);
        self.items.push(item);
        self
    }

    pub fn add_attachment(mut self, payload: &str, ty: impl Into<Option<AttachmentType>>) -> Self {
        let ty = ty.into();
        let mut item = RawItem::from_json(payload.into()).set_type(ItemType::Attachment);

        if let Some(ty) = ty {
            item = item.add_header("attachment_type", &ty.to_string());
        };

        self.items.push(item);
        self
    }

    pub fn add_transaction(mut self, payload: Value) -> Self {
        let item = RawItem::from_json(payload).set_type(ItemType::Event);
        self.items.push(item);
        self
    }

    pub fn add_event(mut self, payload: &str) -> Self {
        let item = RawItem::from_bytes(payload.to_string()).set_type(ItemType::Event);
        self.items.push(item);
        self
    }

    pub fn serialize(&self) -> String {
        let mut serialized = String::new();

        // Serialize envelope-level headers as JSON
        let headers_json = serde_json::to_string(&self.headers).unwrap();
        serialized.push_str(&format!("{}\n", headers_json));

        // Serialize items, which are already adjusted to include JSON headers
        for item in &self.items {
            serialized.push_str(item.serialize().as_str());
        }

        serialized
    }
}

use relay_base_schema::project::ProjectKey;
use relay_event_schema::protocol::{Event, LogEntry};
use relay_server::envelope::AttachmentType;
use relay_server::envelope::Item;
use relay_server::envelope::ItemType;
use serde_json::json;
use serde_json::Value;
use uuid::Uuid;

use crate::into_item;
use crate::x_create_transaction_item;
use crate::BackgroundProcess;

pub fn create_error_item(public_key: ProjectKey) -> (RawItem, Uuid, Uuid) {
    let trace_id = Uuid::new_v4();
    let event_id = Uuid::new_v4();
    dbg!(event_id);
    dbg!(trace_id);
    let error_event = json!({
        "event_id": dbg!(event_id.simple()),
        "message": "This is an error.",
        "extra": {"msg_text": "This is an error", "id": event_id.simple()},
        "type": "error",
        "environment": "production",
        "release": "foo@1.2.3",
    });

    let item = RawItem::from_json(error_event).set_type(ItemType::Event);
    (item, trace_id, event_id)
}
