use std::collections::HashMap;
use std::fmt;
use std::ops::ControlFlow;

use reqwest::{Client, ClientBuilder, StatusCode, Url};
use serde::Deserialize;

use relay_system::{Controller, Service, Signal, SignalType};

const EXTENSION_NAME: &str = "sentry-lambda-extension";
const EXTENSION_NAME_HEADER: &str = "Lambda-Extension-Name";
const EXTENSION_ID_HEADER: &str = "Lambda-Extension-Identifier";

/// Response received from the register API.
///
/// # Example
///
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
    /// The name of the lambda function.
    pub function_name: String,
    /// The version of the lambda function.
    pub function_version: String,
    /// The handler that the labmda function invokes.
    pub handler: String,
}

/// Tracing headers from an [`InvokeResponse`].
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tracing {
    /// type of tracing header
    #[serde(rename = "type")]
    pub ty: String,
    /// tracing header value
    pub value: String,
}

/// Response received from the next event API on an `INVOKE` event.
///
/// See [Invoke Phase] for more information.
///
/// # Example
///
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
///
/// [invoke phase]: https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html#runtimes-lifecycle-invoke
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvokeResponse {
    /// The time and date when the lambda function times out in Unix time milliseconds.
    pub deadline_ms: u64,
    /// Unique request identifier.
    pub request_id: String,
    /// The invoked lambda function's ARN (Amazon Resource Name).
    pub invoked_function_arn: String,
    /// Tracing headers.
    pub tracing: Tracing,
}

/// Response received from the next event API on a `SHUTDOWN` event.
///
/// See [Shutdown Phase] for more information.
///
/// # Example
///
/// ```json
/// {
///   "eventType": "SHUTDOWN",
///   "shutdownReason": "TIMEOUT",
///   "deadlineMs": 42069
/// }
/// ```
///
/// [shutdown phase]: https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html#runtimes-lifecycle-shutdown
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShutdownResponse {
    /// The reason for the shutdown.
    pub shutdown_reason: String,
    /// The time and date when the lambda function times out in Unix time milliseconds.
    pub deadline_ms: u64,
}

/// All possible next event responses.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE", tag = "eventType")]
pub enum NextEventResponse {
    /// `INVOKE` response.
    Invoke(InvokeResponse),
    /// `SHUTDOWN` response.
    Shutdown(ShutdownResponse),
}

/// Generic error in an AWS extension context.
#[derive(Debug)]
pub struct AwsExtensionError(());

impl fmt::Display for AwsExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "aws extension error")
    }
}

impl std::error::Error for AwsExtensionError {}

/// Actor implementing an AWS extension.
///
/// Spawns an actor that:
///
/// * registers with the AWS extensions API
/// * sends blocking (0 timeout) NextEvent calls to the extensions API to get the next invocation
///
/// Each finished invocation immediately polls for the next event. Note that AWS might freeze the
/// container indefinitely for unused lambdas and this request will also wait until things are
/// active again.
///
/// The actual requests are done in a separate tokio runtime with one worker thread with a oneshot
/// channel being used for communicating the necessary responses.
#[derive(Debug)]
pub struct AwsExtension {
    /// The base url for the AWS Extensions API.
    base_url: Url,
    /// The reqwest client to make Extensions API requests with.
    ///
    /// Note that all timeouts need to be 0 because the container might get frozen if lambda is
    /// idle.
    reqwest_client: Client,
}

impl AwsExtension {
    /// Creates a new `AwsExtension` instance.
    pub fn new(aws_runtime_api: &str) -> Result<Self, AwsExtensionError> {
        let base_url = format!("http://{}/2020-01-01/extension", aws_runtime_api)
            .parse()
            .map_err(|_| AwsExtensionError(()))?;

        let reqwest_client = ClientBuilder::new()
            .pool_idle_timeout(None)
            .build()
            .map_err(|_| AwsExtensionError(()))?;

        Ok(AwsExtension {
            base_url,
            reqwest_client,
        })
    }

    async fn register(&mut self) -> Result<String, AwsExtensionError> {
        relay_log::info!("Registering AWS extension on {}", self.base_url);
        let url = format!("{}/register", self.base_url);
        let body = HashMap::from([("events", ["INVOKE", "SHUTDOWN"])]);

        let request = self
            .reqwest_client
            .post(&url)
            .header(EXTENSION_NAME_HEADER, EXTENSION_NAME)
            .json(&body);

        let response = request.send().await.map_err(|_| AwsExtensionError(()))?;

        if response.status() != StatusCode::OK {
            return Err(AwsExtensionError(()));
        }

        let extension_id = response
            .headers()
            .get(EXTENSION_ID_HEADER)
            .and_then(|h| h.to_str().ok())
            .map(|h| h.to_string())
            .ok_or(AwsExtensionError(()))?;

        Ok(extension_id)
    }

    async fn next_event(&self, extension_id: &str) -> Result<ControlFlow<()>, AwsExtensionError> {
        let url = format!("{}/event/next", self.base_url);

        let request = self
            .reqwest_client
            .get(&url)
            .header(EXTENSION_ID_HEADER, extension_id);

        let res = request.send().await.map_err(|_| AwsExtensionError(()))?;
        let next_event = res.json().await.map_err(|_| AwsExtensionError(()))?;

        match next_event {
            NextEventResponse::Invoke(response) => {
                relay_log::debug!("Received INVOKE: request_id {}", response.request_id);
                Ok(ControlFlow::Continue(()))
            }
            NextEventResponse::Shutdown(response) => {
                relay_log::debug!("Received SHUTDOWN: reason {}", response.shutdown_reason);
                Controller::from_registry().do_send(Signal(SignalType::Term));
                Ok(ControlFlow::Break(()))
            }
        }
    }
}

impl Service for AwsExtension {
    type Interface = ();

    fn spawn_handler(mut self, _rx: relay_system::Receiver<Self::Interface>) {
        tokio::spawn(async move {
            relay_log::info!("AWS extension started");

            let extension_id = match self.register().await {
                Ok(extension_id) => {
                    relay_log::info!("AWS extension successfully registered");
                    extension_id
                }
                Err(_) => {
                    relay_log::info!("AWS extension registration failed");
                    return;
                }
            };

            while let Ok(control_flow) = self.next_event(&extension_id).await {
                if control_flow.is_break() {
                    break;
                }
            }

            relay_log::info!("AWS extension stopped");
        });
    }
}
