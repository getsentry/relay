#![allow(deprecated)]

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use actix_web::error::Error;
use actix_web::middleware::{Finished, Middleware, Response, Started};
use actix_web::{http::header, Body, HttpMessage, HttpRequest, HttpResponse};
use failure::Fail;
use futures::prelude::*;

use relay_common::metric;
use relay_log::_sentry::{types::Uuid, Hub, Level, ScopeGuard};
use relay_log::protocol::{ClientSdkPackage, Event, Request};

use crate::constants::SERVER;
use crate::metrics::{RelayCounters, RelayTimers};
use crate::utils::ApiErrorResponse;

/// Basic metrics
pub struct Metrics;

/// The time at which the request started.
#[derive(Clone, Copy, Debug)]
pub struct StartTime(Instant);

impl StartTime {
    /// Returns the `Instant` of this start time.
    #[inline]
    pub fn into_inner(self) -> Instant {
        self.0
    }
}

impl<S> Middleware<S> for Metrics {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started, Error> {
        req.extensions_mut().insert(StartTime(Instant::now()));
        metric!(
            counter(RelayCounters::Requests) += 1,
            route = req.resource().name(),
            method = req.method().as_str()
        );
        Ok(Started::Done)
    }

    fn finish(&self, req: &HttpRequest<S>, resp: &HttpResponse) -> Finished {
        let start_time = req.extensions().get::<StartTime>().unwrap().0;

        metric!(
            timer(RelayTimers::RequestsDuration) = start_time.elapsed(),
            route = req.resource().name(),
            method = req.method().as_str()
        );
        metric!(
            counter(RelayCounters::ResponsesStatusCodes) += 1,
            status_code = &resp.status().as_str(),
            route = req.resource().name(),
            method = req.method().as_str()
        );

        Finished::Done
    }
}

/// Adds the common relay headers.
pub struct AddCommonHeaders;

impl<S> Middleware<S> for AddCommonHeaders {
    fn response(
        &self,
        _request: &HttpRequest<S>,
        mut response: HttpResponse,
    ) -> Result<Response, Error> {
        response
            .headers_mut()
            .insert(header::SERVER, header::HeaderValue::from_static(SERVER));

        Ok(Response::Done(response))
    }
}

/// Registers the default error handlers.
pub struct ErrorHandlers;

impl<S> Middleware<S> for ErrorHandlers {
    fn response(&self, _: &HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
        if (resp.status().is_server_error() || resp.status().is_client_error())
            && resp.body() == &Body::Empty
        {
            let reason = resp.status().canonical_reason().unwrap_or("unknown error");
            Ok(Response::Done(
                resp.into_builder()
                    .json(ApiErrorResponse::with_detail(reason)),
            ))
        } else {
            Ok(Response::Done(resp))
        }
    }
}

/// Read request before returning response. This is not required for RFC compliance, however:
///
/// * a lot of clients are not able to deal with unread request payloads
/// * in HTTP over unix domain sockets, not reading the request payload might cause the client's
///   write() to block forever due to a filled up socket buffer
pub struct ReadRequestMiddleware;

impl<S> Middleware<S> for ReadRequestMiddleware {
    fn response(&self, req: &HttpRequest<S>, resp: HttpResponse) -> Result<Response, Error> {
        let future = req
            .payload()
            .for_each(|_| Ok(()))
            .map(|_| resp)
            .map_err(Error::from);

        Ok(Response::Future(Box::new(future)))
    }
}

/// Reports certain failures to sentry.
pub struct SentryMiddleware {
    emit_header: bool,
    capture_server_errors: bool,
}

struct HubWrapper {
    hub: Arc<Hub>,
    root_scope: RefCell<Option<ScopeGuard>>,
}

impl SentryMiddleware {
    /// Creates a new sentry middleware.
    pub fn new() -> SentryMiddleware {
        SentryMiddleware {
            emit_header: false,
            capture_server_errors: true,
        }
    }

    fn new_hub(&self) -> Arc<Hub> {
        Arc::new(Hub::new_from_top(Hub::main()))
    }
}

impl Default for SentryMiddleware {
    fn default() -> Self {
        SentryMiddleware::new()
    }
}

fn extract_request<S: 'static>(req: &HttpRequest<S>, with_pii: bool) -> (Option<String>, Request) {
    let resource = req.resource();
    let transaction = if let Some(rdef) = resource.rdef() {
        Some(rdef.pattern().to_string())
    } else if resource.name() != "" {
        Some(resource.name().to_string())
    } else {
        None
    };
    let mut sentry_req = Request {
        url: format!(
            "{}://{}{}",
            req.connection_info().scheme(),
            req.connection_info().host(),
            req.uri()
        )
        .parse()
        .ok(),
        method: Some(req.method().to_string()),
        headers: req
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().into(), v.to_str().unwrap_or("").into()))
            .collect(),
        ..Default::default()
    };

    if with_pii {
        if let Some(remote) = req.connection_info().remote() {
            sentry_req.env.insert("REMOTE_ADDR".into(), remote.into());
        }
    };

    (transaction, sentry_req)
}

impl<S: 'static> Middleware<S> for SentryMiddleware {
    fn start(&self, req: &HttpRequest<S>) -> Result<Started, Error> {
        let hub = self.new_hub();
        let outer_req = req;
        let req = outer_req.clone();
        let client = hub.client();

        let req = fragile::SemiSticky::new(req);
        let cached_data = Arc::new(Mutex::new(None));

        let root_scope = hub.push_scope();
        hub.configure_scope(move |scope| {
            scope.add_event_processor(Box::new(move |mut event| {
                let mut cached_data = cached_data.lock().unwrap();
                if cached_data.is_none() && req.is_valid() {
                    let with_pii = client
                        .as_ref()
                        .map_or(false, |x| x.options().send_default_pii);
                    *cached_data = Some(extract_request(&req.get(), with_pii));
                }

                if let Some((ref transaction, ref req)) = *cached_data {
                    if event.transaction.is_none() {
                        event.transaction = transaction.clone();
                    }
                    if event.request.is_none() {
                        event.request = Some(req.clone());
                    }
                }

                if let Some(sdk) = event.sdk.take() {
                    let mut sdk = sdk.into_owned();
                    sdk.packages.push(ClientSdkPackage {
                        name: "sentry-actix".into(),
                        version: env!("CARGO_PKG_VERSION").into(),
                    });
                    event.sdk = Some(Cow::Owned(sdk));
                }

                Some(event)
            }));
        });

        outer_req.extensions_mut().insert(HubWrapper {
            hub,
            root_scope: RefCell::new(Some(root_scope)),
        });
        Ok(Started::Done)
    }

    fn response(&self, req: &HttpRequest<S>, mut resp: HttpResponse) -> Result<Response, Error> {
        if self.capture_server_errors && resp.status().is_server_error() {
            let event_id = if let Some(error) = resp.error() {
                Some(Hub::from_request(req).capture_actix_error(error))
            } else {
                None
            };
            match event_id {
                Some(event_id) if self.emit_header => {
                    resp.headers_mut().insert(
                        "x-sentry-event",
                        event_id.to_simple_ref().to_string().parse().unwrap(),
                    );
                }
                _ => {}
            }
        }
        Ok(Response::Done(resp))
    }

    fn finish(&self, req: &HttpRequest<S>, _resp: &HttpResponse) -> Finished {
        // if we make it to the end of the request we want to first drop the root
        // scope before we drop the entire hub.  This will first drop the closures
        // on the scope which in turn will release the circular dependency we have
        // with the hub via the request.
        if let Some(hub_wrapper) = req.extensions().get::<HubWrapper>() {
            if let Ok(mut guard) = hub_wrapper.root_scope.try_borrow_mut() {
                guard.take();
            }
        }
        Finished::Done
    }
}

/// Hub extensions for actix.
pub trait ActixWebHubExt {
    /// Returns the hub from a given http request.
    ///
    /// This requires that the `SentryMiddleware` middleware has been enabled or the
    /// call will panic.
    fn from_request<S>(req: &HttpRequest<S>) -> Arc<Hub>;
    /// Captures an actix error on the given hub.
    fn capture_actix_error(&self, err: &Error) -> Uuid;
}

impl ActixWebHubExt for Hub {
    fn from_request<S>(req: &HttpRequest<S>) -> Arc<Hub> {
        req.extensions()
            .get::<HubWrapper>()
            .expect("SentryMiddleware middleware was not registered")
            .hub
            .clone()
    }

    fn capture_actix_error(&self, err: &Error) -> Uuid {
        let mut exceptions = vec![];
        let mut ptr: Option<&dyn Fail> = Some(err.as_fail());
        let mut idx = 0;
        while let Some(fail) = ptr {
            // Check whether the failure::Fail held by err is a failure::Error wrapped in Compat
            // If that's the case, we should be logging that error and its fail instead of the wrapper's construction in actix_web
            // This wouldn't be necessary if failure::Compat<failure::Error>'s Fail::backtrace() impl was not "|| None",
            // that is however impossible to do as of now because it conflicts with the generic implementation of Fail also provided in failure.
            // Waiting for update that allows overlap, (https://github.com/rust-lang/rfcs/issues/1053), but chances are by then failure/std::error will be refactored anyway
            let compat: Option<&failure::Compat<failure::Error>> = fail.downcast_ref();
            let failure_err = compat.map(failure::Compat::get_ref);
            let fail = failure_err.map_or(fail, |x| x.as_fail());
            exceptions.push(relay_log::exception_from_single_fail(
                fail,
                if idx == 0 {
                    Some(failure_err.map_or_else(|| err.backtrace(), |err| err.backtrace()))
                } else {
                    fail.backtrace()
                },
            ));
            ptr = fail.cause();
            idx += 1;
        }
        exceptions.reverse();
        self.capture_event(Event {
            exception: exceptions.into(),
            level: Level::Error,
            ..Default::default()
        })
    }
}
