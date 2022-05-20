use std::collections::HashMap;

use actix::actors::signal::{Signal, SignalType};
use actix::prelude::*;
use failure::Fail;
use reqwest::blocking::Client;
use reqwest::StatusCode;
use serde::Deserialize;

use relay_system::Controller;

const EXTENSION_NAME: &str = "sentry-lambda-extension";
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

/// Response received from the register API
///
/// Example response body
/// ```json
/// {
///    "functionName": "helloWorld",
///    "functionVersion": "$LATEST",
///    "handler": "lambda_function.lambda_handler"
/// }
/// ```
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterResponse {
    /// the name of the lambda function
    pub function_name: String,
    /// the version of the lambda function
    pub function_version: String,
    /// the handler that the labmda function invokes
    pub handler: String,
}

/// Tracing headers from an INVOKE response
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tracing {
    /// type of tracing header
    #[serde(rename = "type")]
    pub ty: String,
    /// tracing header value
    pub value: String,
}

/// Response received from the next event API on an INVOKE event
///
/// Example response body
/// ```json
/// {
///     "eventType": "INVOKE",
///     "deadlineMs": 676051,
///     "requestId": "3da1f2dc-3222-475e-9205-e2e6c6318895",
///     "invokedFunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:ExtensionTest",
///     "tracing": {
///         "type": "X-Amzn-Trace-Id",
///         "value": "Root=1-5f35ae12-0c0fec141ab77a00bc047aa2;Parent=2be948a625588e32;Sampled=1"
///     }
/// }
/// ```
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvokeResponse {
    /// the time till the lambda function times out in Unix milliseconds
    pub deadline_ms: u64,
    /// unique request identifier
    pub request_id: String,
    /// the invoked lambda function's ARN (Amazon Resource Name)
    pub invoked_function_arn: String,
    /// tracing headers
    pub tracing: Tracing,
}

/// Response received from the next event API on an SHUTDOWN event
///
/// Example response body
///
/// ```json
/// {
///   "eventType": "SHUTDOWN",
///   "shutdownReason": "TIMEOUT",
///   "deadlineMs": 42069
/// }
/// ```
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutdownResponse {
    /// the reason for the shutdown
    pub shutdown_reason: String,
    /// the time till the lambda function times out in Unix milliseconds
    pub deadline_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE", tag = "eventType")]
enum NextEventResponse {
    Invoke(InvokeResponse),
    Shutdown(ShutdownResponse),
}

#[derive(Debug, Fail)]
#[fail(display = "aws extension error")]
/// Generic error in an AWS extension context
pub struct AwsExtensionError;

/// Actor implementing an AWS extension
///
/// Spawns an actor that
/// * registers with the AWS extensions API
/// * sends blocking NextEvent calls to the extensions API to get the next invocation
///
/// Each finished invocation immediately polls for the next event. Note that AWS might
/// freeze the container indefinitely for unused lambdas and this request will also wait
/// till things are active again.
pub struct AwsExtension {
    /// The base url for the AWS Extensions API
    base_url: String,
    /// The reqwest client needs to be blocking and with 0 timeout
    /// because the container might get frozen if lambda is unused
    reqwest_client: Client,
    /// The extension id that will be retrieved on register and used
    /// for subsequent requests
    extension_id: Option<String>,
}

impl AwsExtension {
    /// Creates a new `AwsExtension` instance.
    pub fn new(aws_runtime_api: String) -> Result<Self, AwsExtensionError> {
        let base_url = format!("http://{}/2020-01-01/extension", aws_runtime_api);

        let reqwest_client = reqwest::blocking::Client::builder()
            .timeout(None)
            .build()
            .map_err(|_| AwsExtensionError)?;

        Ok(AwsExtension {
            base_url,
            reqwest_client,
            extension_id: None,
        })
    }

    fn register(&mut self) -> Result<(), AwsExtensionError> {
        let url = format!("{}/register", self.base_url);
        let map = HashMap::from([("events", ["INVOKE", "SHUTDOWN"])]);

        let res = self
            .reqwest_client
            .post(&url)
            .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
            .json(&map)
            .send()
            .map_err(|_| AwsExtensionError)?;

        if res.status() != StatusCode::OK {
            return Err(AwsExtensionError);
        }

        // let register_response: RegisterResponse = res.json().unwrap();

        let extension_id = res
            .headers()
            .get(EXTENSION_ID_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(|h| h.to_string())
            .ok_or(AwsExtensionError)?;

        self.extension_id = Some(extension_id);
        relay_log::info!("AWS extension successfully registered");
        Ok(())
    }

    fn next_event(&self) -> Result<NextEventResponse, AwsExtensionError> {
        match self.extension_id.as_ref() {
            Some(extension_id) => {
                let url = format!("{}/event/next", self.base_url);

                self.reqwest_client
                    .get(&url)
                    .header(EXTENSION_ID_HEADER, extension_id)
                    .send()
                    .and_then(|r| r.json())
                    .map_err(|_| AwsExtensionError)
            }
            None => Err(AwsExtensionError),
        }
    }
}

impl Actor for AwsExtension {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        relay_log::info!("AWS extension started");

        match self.register() {
            Ok(_) => context.notify(NextEvent),
            Err(_) => relay_log::info!("AWS extension registration failed"),
        }
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

struct NextEvent;

impl Message for NextEvent {
    type Result = ();
}

impl Handler<NextEvent> for AwsExtension {
    type Result = ();

    fn handle(&mut self, _message: NextEvent, context: &mut Self::Context) -> Self::Result {
        match self.next_event() {
            Ok(NextEventResponse::Invoke(invoke_response)) => {
                relay_log::debug!("Received INVOKE: request_id {}", invoke_response.request_id);
                context.notify(NextEvent);
            }
            Ok(NextEventResponse::Shutdown(shutdown_response)) => {
                relay_log::debug!(
                    "Received SHUTDOWN: reason {}",
                    shutdown_response.shutdown_reason
                );
                // need to kill the whole system here, not sure if this is the right way
                Controller::from_registry().do_send(Signal(SignalType::Term));
            }
            _ => {
                relay_log::debug!("Next event request failed");
                context.notify(NextEvent);
            }
        }
    }
}
