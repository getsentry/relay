use crate::processor::{ProcessingState, Processor};
use crate::protocol::{ClientSdkInfo, DebugMeta, Exception, Request, Stacktrace, User, Values};
use crate::types::Annotated;

fn collect_errors<T>(value: &Annotated<T>, name: &str) -> Option<String> {
    if value.is_valid() {
        return None;
    }

    let mut collected = format!("invalid {}: ", name);
    for (index, error) in value.meta().iter_errors().enumerate() {
        if index > 0 {
            collected.push_str(", ");
        }

        collected.push_str(error);
    }

    Some(collected)
}

/// Processor that escalates certain errors up to top-level attributes.
///
/// This processor is meant as a temporary shim to provide compatibility with how Sentry deals with
/// interface validation. It emulates the behavior of `raise InterfaceValidationError` in nested
/// interfaces. Note that this assumes that `StoreProcessor` has already attached errors to certain
/// interfaces.
///
/// The following errors are escalated transitively:
///
///  - `Values.values[]` -> `Values`
///  - `Values.values` -> `Values`
///  - `Stacktrace.frames[]` -> `Stacktrace`
///  - `Stacktrace.frames` -> `Stacktrace`
///  - `Exception.stacktrace` -> `Exception`
///  - `Exception.mechanism` -> `Exception`
///  - `Request.method` -> `Request`
///  - `DebugMeta.images[]` -> `DebugMeta`
///  - `User.email` -> `User`
///  - `User.ip_address` -> `User`
///  - `ClientSdk.integrations` -> `ClientSdk`
///  - `ClientSdk.packages` -> `ClientSdk`
pub struct EscalateErrorsProcessor;

impl Processor for EscalateErrorsProcessor {
    fn process_values<T>(
        &mut self,
        mut values: Annotated<Values<T>>,
        _state: ProcessingState,
    ) -> Annotated<Values<T>> {
        // `Values.values[]` -> `Values`
        let invalid_values = values
            .value()
            .and_then(|v| v.values.value())
            .into_iter()
            .flatten()
            .filter(|value| !value.is_valid())
            .count();

        if invalid_values > 0 {
            let error = format!("interface contains {} invalid values", invalid_values);
            values.meta_mut().add_error(error, None);
        }

        // `Values.values` -> `Values`
        if let Some(error) = values
            .value()
            .and_then(|e| collect_errors(&e.values, "values"))
        {
            values.meta_mut().add_error(error, None);
        }

        values
    }

    fn process_exception(
        &mut self,
        mut exception: Annotated<Exception>,
        _state: ProcessingState,
    ) -> Annotated<Exception> {
        // `Exception.stacktrace` -> `Exception`
        if let Some(error) = exception
            .value()
            .and_then(|e| collect_errors(&e.stacktrace, "stacktrace"))
        {
            exception.meta_mut().add_error(error, None);
        }

        // `Exception.mechanism` -> `Exception`
        if let Some(error) = exception
            .value()
            .and_then(|e| collect_errors(&e.mechanism, "mechanism"))
        {
            exception.meta_mut().add_error(error, None);
        }

        exception
    }

    fn process_stacktrace(
        &mut self,
        mut stacktrace: Annotated<Stacktrace>,
        _state: ProcessingState,
    ) -> Annotated<Stacktrace> {
        // `Stacktrace.frames[]` -> `Stacktrace`
        let invalid_frames = stacktrace
            .value()
            .and_then(|stacktrace| stacktrace.frames.value())
            .into_iter()
            .flatten()
            .filter(|frame| !frame.is_valid())
            .count();

        if invalid_frames > 0 {
            let error = format!("stacktrace contains {} invalid frames", invalid_frames);
            stacktrace.meta_mut().add_error(error, None);
        }

        // `Stacktrace.frames` -> `Stacktrace`
        if let Some(error) = stacktrace
            .value()
            .and_then(|s| collect_errors(&s.frames, "frames"))
        {
            stacktrace.meta_mut().add_error(error, None);
        }

        stacktrace
    }

    fn process_request(
        &mut self,
        mut request: Annotated<Request>,
        _state: ProcessingState,
    ) -> Annotated<Request> {
        // `Request.method` -> `Request`
        if let Some(error) = request
            .value()
            .and_then(|r| collect_errors(&r.method, "method"))
        {
            request.meta_mut().add_error(error, None);
        }

        request
    }

    fn process_user(
        &mut self,
        mut user: Annotated<User>,
        _state: ProcessingState,
    ) -> Annotated<User> {
        // `User.email` -> `User`
        if let Some(error) = user.value().and_then(|u| collect_errors(&u.email, "email")) {
            user.meta_mut().add_error(error, None);
        }

        // `User.ip_address` -> `User`
        if let Some(error) = user
            .value()
            .and_then(|u| collect_errors(&u.ip_address, "email"))
        {
            user.meta_mut().add_error(error, None);
        }

        user
    }

    fn process_client_sdk_info(
        &mut self,
        mut client_sdk_info: Annotated<ClientSdkInfo>,
        _state: ProcessingState,
    ) -> Annotated<ClientSdkInfo> {
        // `ClientSdk.integrations` -> `ClientSdk`
        if let Some(error) = client_sdk_info
            .value()
            .and_then(|c| collect_errors(&c.integrations, "integrations"))
        {
            client_sdk_info.meta_mut().add_error(error, None);
        }

        // `ClientSdk.packages` -> `ClientSdk`
        if let Some(error) = client_sdk_info
            .value()
            .and_then(|c| collect_errors(&c.packages, "packages"))
        {
            client_sdk_info.meta_mut().add_error(error, None);
        }

        client_sdk_info
    }

    fn process_debug_meta(
        &mut self,
        mut debug_meta: Annotated<DebugMeta>,
        _state: ProcessingState,
    ) -> Annotated<DebugMeta> {
        // `DebugMeta.images[]` -> `DebugMeta`
        let invalid_images = debug_meta
            .value()
            .and_then(|debug_meta| debug_meta.images.value())
            .into_iter()
            .flatten()
            .filter(|image| !image.is_valid())
            .count();

        if invalid_images > 0 {
            let error = format!("debug meta contains {} invalid images", invalid_images);
            debug_meta.meta_mut().add_error(error, None);
        }

        debug_meta
    }
}
