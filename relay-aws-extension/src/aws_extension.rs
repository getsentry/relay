use actix::prelude::*;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;

const EXTENSION_NAME: &str = "sentry-lambda-extension";
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";
// const SHUTDOWN_TIMEOUT: u64 = 2;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct RegisterResponse {
    pub function_name: String,
    pub function_version: String,
    pub handler: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct Tracing {
    pub r#type: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
#[serde(rename_all = "UPPERCASE", tag = "eventType")]
enum NextEventResponse {
    #[serde(rename_all = "camelCase")]
    Invoke {
        deadline_ms: u64,
        request_id: String,
        invoked_function_arn: String,
        tracing: Tracing,
    },
    #[serde(rename_all = "camelCase")]
    Shutdown {
        shutdown_reason: String,
        deadline_ms: u64,
    },
}

pub struct AwsExtension {
    base_url: String,
    reqwest_client: reqwest::blocking::Client,
    extension_id: Option<String>,
}

impl AwsExtension {
    pub fn new() -> Self {
        let base_url = format!(
            "http://{}/2020-01-01/extension",
            env::var("AWS_LAMBDA_RUNTIME_API").unwrap()
        );

        let reqwest_client = reqwest::blocking::Client::builder()
            .timeout(None)
            .build()
            .unwrap();

        AwsExtension {
            base_url,
            reqwest_client,
            extension_id: None,
        }
    }

    fn register(&mut self) {
        let url = format!("{}/register", self.base_url);
        let map = HashMap::from([("events", ["INVOKE", "SHUTDOWN"])]);

        let res = self
            .reqwest_client
            .post(&url)
            .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
            .json(&map)
            .send()
            .unwrap();

        // let register_response: RegisterResponse = res.json().unwrap();

        let extension_id = res
            .headers()
            .get(EXTENSION_ID_HEADER)
            .unwrap()
            .to_str()
            .unwrap();

        self.extension_id = Some(extension_id.to_string());
        relay_log::info!("AWS extension successfully registered");
    }

    fn next_event(&self) -> NextEventResponse {
        let url = format!("{}/event/next", self.base_url);
        let extension_id = self.extension_id.as_ref().unwrap();

        self.reqwest_client
            .get(&url)
            .header(EXTENSION_ID_HEADER, extension_id)
            .send()
            .unwrap()
            .json()
            .unwrap()
    }
}

impl Actor for AwsExtension {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        relay_log::info!("AWS extension started");
        self.register();
        context.notify(NextEvent);
    }

    fn stopped(&mut self, _context: &mut Self::Context) {
        relay_log::info!("AWS extension stopped");
    }
}

impl Default for AwsExtension {
    fn default() -> Self {
        unimplemented!("register with the SystemRegistry instead")
    }
}

impl Supervised for AwsExtension {}

impl SystemService for AwsExtension {}

pub struct NextEvent;

impl Message for NextEvent {
    type Result = ();
}

impl Handler<NextEvent> for AwsExtension {
    type Result = ();

    fn handle(&mut self, _message: NextEvent, context: &mut Self::Context) -> Self::Result {
        match self.next_event() {
            NextEventResponse::Invoke { request_id, .. } => {
                relay_log::debug!("Received INVOKE: request_id {}", request_id);
                context.notify(NextEvent);
            }
            NextEventResponse::Shutdown {
                shutdown_reason, ..
            } => {
                relay_log::debug!("Received SHUTDOWN: reason {}", shutdown_reason);
                //TODO-neel send shutdown to system
            }
        }
    }
}
