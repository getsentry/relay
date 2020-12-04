use failure::Fail;
use sentry::integrations::backtrace::parse_stacktrace;
use sentry::parse_type_from_debug;
use sentry::protocol::Exception;
use sentry::{ClientOptions, Integration};

/// The Sentry Failure Integration.
#[derive(Debug, Default)]
pub struct FailureIntegration;

impl FailureIntegration {
    /// Creates a new Failure Integration.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Integration for FailureIntegration {
    fn name(&self) -> &'static str {
        "failure"
    }

    fn setup(&self, cfg: &mut ClientOptions) {
        cfg.in_app_exclude.push("failure::");
        cfg.extra_border_frames.extend_from_slice(&[
            "failure::error_message::err_msg",
            "failure::backtrace::Backtrace::new",
            "failure::backtrace::internal::InternalBacktrace::new",
            "failure::Fail::context",
        ]);
    }
}

/// This converts a single fail instance into an exception.
///
/// This is typically not very useful as the `event_from_error` and
/// `event_from_fail` methods will assemble an entire event with all the
/// causes of a failure, however for certain more complex situations where
/// fails are contained within a non fail error type that might also carry
/// useful information it can be useful to call this method instead.
pub fn exception_from_single_fail<F: Fail + ?Sized>(
    f: &F,
    bt: Option<&failure::Backtrace>,
) -> Exception {
    let dbg = format!("{:?}", f);
    Exception {
        ty: parse_type_from_debug(&dbg).to_owned(),
        value: Some(f.to_string()),
        stacktrace: bt
            // format the stack trace with alternate debug to get addresses
            .map(|bt| format!("{:#?}", bt))
            .and_then(|x| parse_stacktrace(&x)),
        ..Default::default()
    }
}
