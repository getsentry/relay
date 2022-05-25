use std::collections::HashMap;

use actix::actors::signal::{Signal, SignalType};
use actix::fut;
use actix::prelude::*;
use failure::Fail;
use futures::{prelude::*, sync::oneshot};
use reqwest::{Client, ClientBuilder, StatusCode};
use serde::Deserialize;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

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

/// Generic error in an AWS extension context
#[derive(Debug, Fail)]
#[fail(display = "aws extension error")]
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
#[derive(Debug)]
pub struct AwsExtension {
    /// The base url for the AWS Extensions API
    base_url: String,
    /// The extension id that will be retrieved on register and used
    /// for subsequent requests
    extension_id: Option<String>,
    /// The reqwest client to make Extensions API requests with.
    /// Note that all timeouts need to be 0 because the container might
    /// get frozen if lambda is idle.
    reqwest_client: Client,
    /// The tokio runtime used to spawn reqwest futures
    reqwest_runtime: Runtime,
}

impl AwsExtension {
    /// Creates a new `AwsExtension` instance.
    pub fn new(aws_runtime_api: String) -> Result<Self, AwsExtensionError> {
        let base_url = format!("http://{}/2020-01-01/extension", aws_runtime_api);

        let reqwest_runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|_| AwsExtensionError)?;

        let reqwest_client = ClientBuilder::new()
            .pool_idle_timeout(None)
            .build()
            .map_err(|_| AwsExtensionError)?;

        Ok(AwsExtension {
            base_url,
            reqwest_client,
            reqwest_runtime,
            extension_id: None,
        })
    }

    fn register(&mut self, context: &mut Context<Self>) {
        let url = format!("{}/register", self.base_url);
        let body = HashMap::from([("events", ["INVOKE", "SHUTDOWN"])]);

        let request = self
            .reqwest_client
            .post(&url)
            .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
            .json(&body);

        let (tx, rx) = oneshot::channel();

        self.reqwest_runtime.spawn(async move {
            let res = request.send().await;

            let extension_id = res.map_err(|_| AwsExtensionError).and_then(|res| {
                if res.status() != StatusCode::OK {
                    return Err(AwsExtensionError);
                }

                res.headers()
                    .get(EXTENSION_ID_HEADER)
                    .and_then(|h| h.to_str().ok())
                    .map(|h| h.to_string())
                    .ok_or(AwsExtensionError)
            });

            tx.send(extension_id)
        });

        rx.map_err(|_| AwsExtensionError)
            .flatten()
            .map_err(|_| relay_log::info!("AWS extension registration failed"))
            .into_actor(self)
            .and_then(|extension_id, slf, ctx| {
                slf.extension_id = Some(extension_id);
                relay_log::info!("AWS extension successfully registered");
                ctx.notify(NextEvent);
                fut::ok(())
            })
            .drop_err()
            .spawn(context)
    }

    fn next_event(&self, context: &mut Context<Self>) {
        let extension_id = self.extension_id.as_ref().unwrap();
        let url = format!("{}/event/next", self.base_url);

        let request = self
            .reqwest_client
            .get(&url)
            .header(EXTENSION_ID_HEADER, extension_id);

        let (tx, rx) = oneshot::channel();

        self.reqwest_runtime.spawn(async move {
            let res = request.send().await;

            match res {
                Ok(res) => {
                    let json = res
                        .json::<NextEventResponse>()
                        .await
                        .map_err(|_| AwsExtensionError);

                    tx.send(json)
                }
                Err(_) => tx.send(Err(AwsExtensionError)),
            }
        });

        rx.map_err(|_| AwsExtensionError)
            .flatten()
            .into_actor(self)
            .and_then(|next_event, _slf, ctx| {
                match next_event {
                    NextEventResponse::Invoke(invoke_response) => {
                        relay_log::debug!(
                            "Received INVOKE: request_id {}",
                            invoke_response.request_id
                        );
                        ctx.notify(NextEvent);
                    }
                    NextEventResponse::Shutdown(shutdown_response) => {
                        relay_log::debug!(
                            "Received SHUTDOWN: reason {}",
                            shutdown_response.shutdown_reason
                        );

                        Controller::from_registry().do_send(Signal(SignalType::Term));
                    }
                }
                fut::ok(())
            })
            .drop_err()
            .spawn(context)
    }
}

impl Actor for AwsExtension {
    type Context = Context<Self>;

    fn started(&mut self, context: &mut Self::Context) {
        relay_log::info!("AWS extension started");
        self.register(context);
    }

    fn stopped(&mut self, _context: &mut Self::Context) {
        relay_log::info!("AWS extension stopped");
    }
}

struct NextEvent;

impl Message for NextEvent {
    type Result = ();
}

impl Handler<NextEvent> for AwsExtension {
    type Result = ();

    fn handle(&mut self, _message: NextEvent, context: &mut Self::Context) -> Self::Result {
        self.next_event(context);
    }
}
