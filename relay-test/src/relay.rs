use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};

use hyper::http::HeaderName;
use std::time::Duration;

use axum::http::HeaderMap;
use relay_auth::{PublicKey, SecretKey};
use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_config::Config;
use relay_config::Credentials;
use relay_config::RelayInfo;
use reqwest::{self, Response};
use serde_json::{json, Value};
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use tokio::runtime::Runtime;
use uuid::Uuid;

use crate::mini_sentry::MiniSentry;
use crate::{
    merge, outcomes_enabled_config, processing_config, random_port, BackgroundProcess, RawEnvelope,
    TempDir,
};

pub trait Upstream {
    fn url(&self) -> String;
    fn internal_error_dsn(&self) -> String;
    fn insert_known_relay(&self, relay_id: Uuid, public_key: PublicKey);
    fn public_dsn_key(&self, id: ProjectId) -> ProjectKey;
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

impl Upstream for MiniSentry {
    fn url(&self) -> String {
        self.inner.lock().unwrap().url()
    }

    fn internal_error_dsn(&self) -> String {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .internal_error_dsn()
    }

    fn insert_known_relay(&self, relay_id: Uuid, public_key: PublicKey) {
        self.inner.lock().unwrap().known_relays.insert(
            relay_id,
            RelayInfo {
                public_key,
                internal: true,
            },
        );
    }

    fn public_dsn_key(&self, id: ProjectId) -> ProjectKey {
        self.get_dsn_public_key_configs(id).unwrap().public_key
    }
}

fn default_opts(url: String, internal_error_dsn: String, port: u16, host: String) -> Value {
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
}

pub struct RelayBuilder<'a, U: Upstream> {
    pub config: serde_json::Value,
    upstream: &'a U,
}

impl<'a, U: Upstream> RelayBuilder<'a, U> {
    pub fn enable_processing(mut self) -> Self {
        let proc = processing_config();

        self.config = merge(self.config, proc, vec![]);
        self
    }

    pub fn merge_config(mut self, value: serde_json::Value) -> Self {
        self.config = merge(self.config, value, vec![]);
        self
    }

    pub fn enable_outcomes(mut self) -> Self {
        self.config = merge(self.config, outcomes_enabled_config(), vec![]);
        self
    }

    pub fn build(self) -> Relay<'a, U> {
        let config = Config::from_json_value(self.config).unwrap();
        let relay_bin = get_relay_binary().unwrap();

        let mut dir = TempDir::default();
        let dir = dir.create("relay");

        let credentials = load_credentials(&config, &dir);

        self.upstream
            .insert_known_relay(credentials.id, credentials.public_key);

        let process = BackgroundProcess::new(
            relay_bin.as_path().to_str().unwrap(),
            &["-c", dir.as_path().to_str().unwrap(), "run"],
        );

        let server_address = config.listen_addr();

        // We need this delay before we start sending to relay.
        std::thread::sleep(Duration::from_millis(500));

        Relay {
            _process: process,
            server_address,
            _config: Arc::new(config),
            _client: reqwest::Client::new(),
            upstream_dsn: self.upstream.internal_error_dsn(),
            upstream: self.upstream,
        }
    }
}

pub struct Relay<'a, U: Upstream> {
    server_address: SocketAddr,
    _process: BackgroundProcess,
    _config: Arc<Config>,
    _client: reqwest::Client,
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

        let config = default_opts(url, internal_error_dsn, port, host);

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

    pub fn send_envelope_to_url(&self, envelope: RawEnvelope, url: &str) -> Response {
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
            reqwest::Client::new()
                .post(url)
                .headers(headers)
                .body(data)
                .send()
                .await
                .unwrap()
        })
    }

    pub fn send_envelope(&self, envelope: RawEnvelope) {
        let url = self.envelope_url(envelope.project_id);
        self.send_envelope_to_url(envelope, &url);
    }
}

fn get_relay_binary() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let version = "latest";
    if version == "latest" {
        return Ok(std::env::var("RELAY_BIN")
            .map_or_else(|_| "../target/debug/relay".into(), PathBuf::from)
            .canonicalize()
            .expect("Failed to get absolute path"));
    };

    let filename = match env::consts::OS {
        "linux" => "relay-Linux-x86_64",
        "macos" => "relay-Darwin-x86_64",
        "windows" => "relay-Windows-x86_64.exe",
        _ => panic!("Unsupported OS"),
    };

    let download_path = PathBuf::from(format!(
        "target/relay_releases_cache/{}_{}",
        filename, version
    ));

    if !Path::new(&download_path).exists() {
        let download_url = format!(
            "https://github.com/getsentry/relay/releases/download/{}/{}",
            version, filename
        );

        let client = reqwest::blocking::Client::new();
        let mut request = client.get(download_url);

        if let Ok(token) = env::var("GITHUB_TOKEN") {
            request = request.bearer_auth(token);
        }

        let response = request.send()?.error_for_status()?;

        // Adjusted part: Read the entire response body at once.
        let content = response.bytes()?;

        fs::create_dir_all(Path::new(&download_path).parent().unwrap())?;
        let mut file = File::create(&download_path)?;

        // Write the entire content to the file.
        file.write_all(&content)?;

        let mut perms = fs::metadata(&download_path)?.permissions();
        perms.set_mode(0o700); // UNIX-specific; for Windows, you'll need a different approach
        fs::set_permissions(&download_path, perms)?;
    }

    Ok(download_path)
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
        dbg!(&output);
        panic!("Command execution failed");
    }
    let credentials_path = relay_dir.join("credentials.json");

    let credentials_str = std::fs::read_to_string(credentials_path).unwrap();
    serde_json::from_str(&credentials_str).expect("Failed to parse JSON")
}
