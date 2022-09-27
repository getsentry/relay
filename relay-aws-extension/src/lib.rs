//! AWS extension implementation for Sentry's AWS Lambda layer.
//!
//! An AWS extension is a binary that is bundled along with the SDK and its dependencies in an AWS
//! Layer that is added to a user's Lambda function. The Lambda Runtime will start the extension
//! binary as a concurrent process that runs independently of the actual function invocation and
//! continues running across multiple function invocations.
//!
//! Sentry's extension is basically existing Relay running in proxy mode with an additional
//! [`AwsExtension`] service. The service takes care of the extension lifecycle, namely registering
//! the extension and continuously polling for next events. Note that the interval between two next
//! event calls adds to the billing time of the lambda invocation.
//!
//! The main advantage we get currently from the extension is the lambda function not having to wait
//! for the event being sent to Sentry by the SDK. The actual sending happens in a concurrent Relay
//! process so the main function can return sooner and reduce response time of the user's function.
//! In the future, we might use some of [`RegisterResponse`] and [`NextEventResponse`]'s fields in
//! the event payload but that requires intercepting the envelope and modifying it before sending to
//! upstream.
//!
//! See the official [Lambda Extensions API
//! documentation](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-extensions-api.html) for
//! further details.
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod aws_extension;
pub use aws_extension::*;
