use actix::prelude::*;
use std::collections::HashMap;
use std::env;
use std::thread;
use std::time;

const EXTENSION_NAME: &str = "sentry-lambda-extension";
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";
// const SHUTDOWN_TIMEOUT: u64 = 2;

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

    fn set_extension_id(&mut self, extension_id: String) {
        self.extension_id = Some(extension_id);
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

        let extension_id = res
            .headers()
            .get(EXTENSION_ID_HEADER)
            .unwrap()
            .to_str()
            .unwrap();

        self.set_extension_id(extension_id.to_string());
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
        relay_log::info!("Handling next event");
        thread::sleep(time::Duration::from_secs(1));
        context.notify(NextEvent);
    }
}
