use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::Duration;

use axum::http::HeaderMap;
use hyper::http::HeaderName;
use reqwest::{self, Response};
use serde_json::{json, Map, Value};
use tokio::runtime::Runtime;
use uuid::Uuid;

use relay_auth::PublicKey;
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_config::{Config, Credentials};

use crate::{outcomes_enabled_config, processing_config, random_port, Envelope, TempDir, Upstream};

pub struct Relay<'a, U: Upstream> {
    server_address: SocketAddr,
    _process: BackgroundProcess,
    upstream_dsn: String,
    upstream: &'a U,
}

impl<'a, U: Upstream> Relay<'a, U> {
    pub fn server_address(&self) -> SocketAddr {
        self.server_address
    }

    pub fn get_dsn(&self, public_key: ProjectKey) -> String {
        let x = self.server_address();
        let host = x.ip();
        let port = x.port();

        format!("http://{public_key}:@{host}:{port}/42")
    }

    pub fn new(upstream: &'a U) -> Relay<'a, U> {
        Self::builder(upstream).build()
    }

    pub fn builder(upstream: &U) -> RelayBuilder<U> {
        let host = "127.0.0.1".into();
        let port = random_port();
        let url = upstream.url();
        let internal_error_dsn = upstream.internal_error_dsn();

        let config = default_relay_config(url, internal_error_dsn, port, host);

        RelayBuilder { config, upstream }
    }

    fn url(&self) -> String {
        format!(
            "http://{}:{}",
            self.server_address.ip(),
            self.server_address.port()
        )
    }

    pub fn get_auth_header(&self, dsn_key: ProjectKey) -> String {
        format!(
            "Sentry sentry_version=5, sentry_timestamp=1535376240291, sentry_client=rust-node/2.6.3, sentry_key={}",
            dsn_key
        )
    }

    pub fn envelope_url(&self, project_id: ProjectId) -> String {
        let endpoint = format!("/api/{}/envelope/", project_id);
        format!("{}{}", self.url(), endpoint)
    }

    pub fn send_envelope_to_url(&self, envelope: Envelope, url: &str) -> Response {
        use reqwest::header::HeaderValue;

        let mut headers = HeaderMap::new();
        headers.insert(
            "Content-Type",
            HeaderValue::from_static("application/x-sentry-envelope"),
        );

        let dsn_key = self.public_dsn_key(envelope.project_id);
        headers.insert(
            "X-Sentry-Auth",
            HeaderValue::from_str(&self.get_auth_header(dsn_key)).unwrap(),
        );

        let data = envelope.serialize();

        // Add additional headers from envelope if necessary
        for (key, value) in envelope.http_headers.iter() {
            headers.insert(
                HeaderName::from_bytes(key.as_bytes()).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
        }

        Runtime::new().unwrap().block_on(async {
            let client = reqwest::Client::new();
            let max_attempts = 15;
            let mut attempts = 0;

            loop {
                let res = client
                    .post(url)
                    .headers(headers.clone())
                    .body(data.clone())
                    .send()
                    .await;

                match res {
                    Ok(response) if response.status().is_success() => return response,
                    Ok(_) | Err(_) => {
                        attempts += 1;
                        if attempts > max_attempts {
                            return res.unwrap();
                        } else {
                            std::thread::sleep(Duration::from_millis(100));
                        }
                    }
                }
            }
        })
    }

    pub fn send_envelope(&self, envelope: Envelope) {
        let url = self.envelope_url(envelope.project_id);
        self.send_envelope_to_url(envelope, &url);
    }
}

impl<U: Upstream> Upstream for Relay<'_, U> {
    fn url(&self) -> String {
        self.url()
    }

    fn internal_error_dsn(&self) -> String {
        self.upstream_dsn.clone()
    }

    fn insert_known_relay(&self, relay_id: Uuid, public_key: PublicKey) {
        self.upstream.insert_known_relay(relay_id, public_key);
    }

    fn public_dsn_key(&self, id: ProjectId) -> ProjectKey {
        self.upstream.public_dsn_key(id)
    }
}

fn default_relay_config(
    url: String,
    internal_error_dsn: String,
    port: u16,
    host: String,
) -> Map<String, Value> {
    json!({
        "relay": {
            "upstream": url,
            "host": host,
            "port": port,
            "tls_port": null,
            "tls_private_key": null,
            "tls_cert": null,
        },
        "sentry": {
            "dsn": internal_error_dsn,
            "enabled": true,
        },
        "limits": {
            "max_api_file_upload_size": "1MiB",
        },
        "cache": {
            "batch_interval": 0,
        },
        "logging": {
            "level": "trace",
        },
        "http": {
            "timeout": 3,
        },
        "processing": {
            "enabled": false,
            "kafka_config": [],
            "redis": "",
        },
        "outcomes": {
            "aggregator": {
                "bucket_interval": 1,
                "flush_interval": 0,
            },
        },
    })
    .as_object()
    .unwrap()
    .clone()
}

pub struct RelayBuilder<'a, U: Upstream> {
    config: Map<String, Value>,
    upstream: &'a U,
}

impl<'a, U: Upstream> RelayBuilder<'a, U> {
    pub fn enable_processing(mut self) -> Self {
        let proc = processing_config();
        self.config.insert("processing".to_string(), proc);
        self
    }

    pub fn enable_outcomes(mut self) -> Self {
        let outcome = outcomes_enabled_config();
        self.config.insert("outcomes".to_string(), outcome);
        self
    }

    pub fn build(self) -> Relay<'a, U> {
        let config = Config::from_json_value(serde_json::Value::Object(self.config)).unwrap();
        let relay_bin = get_relay_binary().unwrap();

        let mut dir = TempDir::default();
        let dir = dir.create_subdir("relay");

        let credentials = load_credentials(&config, &dir);

        self.upstream
            .insert_known_relay(credentials.id, credentials.public_key);

        let process = BackgroundProcess::new(
            relay_bin.as_path().to_str().unwrap(),
            &["-c", dir.as_path().to_str().unwrap(), "run"],
        );

        let server_address = config.listen_addr();

        Relay {
            _process: process,
            server_address,
            upstream_dsn: self.upstream.internal_error_dsn(),
            upstream: self.upstream,
        }
    }
}

fn _get_relay_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(std::env::var("RELAY_BIN")
        .map_or_else(|_| "../target/debug/relay".into(), PathBuf::from)
        .canonicalize()
        .expect("Failed to get absolute path"))
}

fn get_relay_binary() -> Result<PathBuf, Box<dyn Error>> {
    let path = env::var("RELAY_BIN").unwrap_or_else(|_| "../target/debug/relay".to_string());
    let path_buf = PathBuf::from(path);

    dbg!(&path_buf);

    // Check if the path exists before canonicalizing
    if !path_buf.exists() {
        return Err(format!("Path does not exist: {:?}", path_buf).into());
    }

    path_buf.canonicalize().map_err(|e| e.into())
}

fn load_credentials(config: &Config, relay_dir: &Path) -> Credentials {
    let relay_bin = get_relay_binary().unwrap();
    let config_path = relay_dir.join("config.yml");

    std::fs::write(config_path.as_path(), config.to_yaml_string().unwrap()).unwrap();

    let output = std::process::Command::new(relay_bin.as_path())
        .arg("-c")
        .arg(config_path.parent().unwrap())
        .arg("credentials")
        .arg("generate")
        .output()
        .unwrap();

    if !output.status.success() {
        let error_message = format!(
            "failed to load credentials: {:?}\nstderr: {:?}\nstdout: {:?}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr),
            String::from_utf8_lossy(&output.stdout)
        );
        panic!("{}", error_message);
    }
    let credentials_path = relay_dir.join("credentials.json");

    let credentials_str = std::fs::read_to_string(credentials_path).unwrap();
    serde_json::from_str(&credentials_str).expect("Failed to parse JSON")
}

pub struct BackgroundProcess {
    pub child: Option<Child>,
}

impl BackgroundProcess {
    pub fn new(command: &str, args: &[&str]) -> Self {
        let child = std::process::Command::new(command)
            .args(args)
            .spawn()
            .expect("Failed to start process");

        BackgroundProcess { child: Some(child) }
    }
}

impl Drop for BackgroundProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
