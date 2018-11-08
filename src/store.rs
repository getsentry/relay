use chrono::Utc;

use itertools::Itertools;

use std::mem;

use crate::processor::{ProcessingState, Processor};
use crate::protocol::{self, *};

fn parse_client_as_sdk(auth: &StoreAuth) -> Option<ClientSdkInfo> {
    auth.client
        .splitn(1, '/')
        .collect_tuple()
        .or_else(|| auth.client.splitn(1, ' ').collect_tuple())
        .map(|(name, version)| ClientSdkInfo {
            name: Annotated::new(name.to_owned()),
            version: Annotated::new(version.to_owned()),
            ..Default::default()
        })
}

/// Simplified version of sentry.coreapi.Auth
pub struct StoreAuth {
    client: String,
    is_public: bool,
}

pub struct StoreNormalizeProcessor {
    project_id: Option<u64>,
    client_ip: Option<String>,
    auth: Option<StoreAuth>,
    key_id: Option<String>,
    version: String,
}

impl Processor for StoreNormalizeProcessor {
    fn process_string(
        &self,
        mut value: Annotated<String>,
        state: ProcessingState,
    ) -> Annotated<String> {
        if let Some(cap_size) = state.attrs().cap_size {
            value = value.trim_string(cap_size);
        }
        value
    }

    // TODO: Reduce cyclomatic complexity of this function
    #[cfg_attr(feature = "cargo-clippy", allow(cyclomatic_complexity))]
    fn process_event(
        &self,
        mut event: Annotated<Event>,
        _state: ProcessingState,
    ) -> Annotated<Event> {
        if let Some(ref mut event) = event.0 {
            if let Some(project_id) = self.project_id {
                event.project.0 = Some(project_id);
            }

            if let Some(ref key_id) = self.key_id {
                event.key_id.0 = Some(key_id.clone());
            }

            event.errors.0.get_or_insert_with(Vec::new);

            // TODO: Interfaces

            event.level.0.get_or_insert(Level::Error);

            if event.dist.0.is_some() && event.release.0.is_none() {
                event.dist.0 = None;
            }

            event.timestamp.0.get_or_insert_with(Utc::now);
            event.received.0 = Some(Utc::now());
            event.logger.0.get_or_insert_with(String::new);

            event.extra.0.get_or_insert_with(Object::new);

            {
                let mut tags = event.tags.0.get_or_insert_with(|| Tags(Default::default()));

                // Fix case where legacy apps pass environment as a tag instead of a top level key
                // TODO (alex) save() just reinserts the environment into the tags
                if event.environment.0.is_none() {
                    let mut new_tags = vec![];
                    for tuple in tags.0.drain(..) {
                        if let Annotated(
                            Some((Annotated(Some(ref k), _), Annotated(Some(ref v), _))),
                            _,
                        ) = tuple
                        {
                            if k == "environment" {
                                event.environment.0 = Some(v.clone());
                                continue;
                            }
                        }

                        new_tags.push(tuple);
                    }

                    tags.0 = new_tags;
                }
            }

            // port of src/sentry/eventtypes
            //
            // the SDKs currently do not describe event types, and we must infer
            // them from available attributes
            let has_exceptions = event
                .exceptions
                .0
                .as_ref()
                .and_then(|exceptions| exceptions.values.0.as_ref())
                .filter(|values| !values.is_empty())
                .is_some();

            event.ty = Annotated::new(if has_exceptions {
                EventType::Error
            } else if event.csp.0.is_some() {
                EventType::Csp
            } else if event.hpkp.0.is_some() {
                EventType::Hpkp
            } else if event.expectct.0.is_some() {
                EventType::ExpectCT
            } else if event.expectstaple.0.is_some() {
                EventType::ExpectStaple
            } else {
                EventType::Default
            });

            event.version = Annotated::new(self.version.clone());

            let os_hint = OsHint::from_event(&event);

            if let Some(ref mut exception_values) = event.exceptions.0 {
                if let Some(ref mut exceptions) = exception_values.values.0 {
                    if exceptions.len() == 1 && event.stacktrace.0.is_some() {
                        if let Some(ref mut exception) =
                            exceptions.get_mut(0).and_then(|x| x.0.as_mut())
                        {
                            mem::swap(&mut exception.stacktrace, &mut event.stacktrace);
                            event.stacktrace = Annotated::empty();
                        }
                    }

                    // Exception mechanism needs SDK information to resolve proper names in
                    // exception meta (such as signal names). "SDK Information" really means
                    // the operating system version the event was generated on. Some
                    // normalization still works without sdk_info, such as mach_exception
                    // names (they can only occur on macOS).
                    for mut exception in exceptions.iter_mut() {
                        if let Some(ref mut exception) = exception.0 {
                            if let Some(ref mut mechanism) = exception.mechanism.0 {
                                protocol::normalize_mechanism_meta(mechanism, os_hint);
                            }
                        }
                    }
                }
            }

            let is_public = match self.auth {
                Some(ref auth) => auth.is_public,
                None => false,
            };

            let http_ip = event
                .request
                .0
                .as_ref()
                .and_then(|request| request.env.0.as_ref())
                .and_then(|env| env.get("REMOTE_ADDR"))
                .and_then(|addr| addr.0.as_ref());

            // If there is no User ip_address, update it either from the Http interface
            // or the client_ip of the request.
            if let Some(Value::String(http_ip)) = http_ip {
                let mut user = event.user.0.get_or_insert_with(Default::default);
                if user.ip_address.0.is_none() {
                    user.ip_address = Annotated::new(http_ip.clone());
                }
            } else if let Some(ref client_ip) = self.client_ip {
                let should_use_client_ip =
                    is_public || match event.platform.0.as_ref().map(|x| &**x) {
                        Some("javascript") | Some("cocoa") | Some("objc") => true,
                        _ => false,
                    };

                if should_use_client_ip {
                    let mut user = event.user.0.get_or_insert_with(Default::default);
                    if user.ip_address.0.is_none() {
                        user.ip_address = Annotated::new(client_ip.clone());
                    }
                }
            }
        }
        event
    }

    fn process_breadcrumb(
        &self,
        mut breadcrumb: Annotated<Breadcrumb>,
        _state: ProcessingState,
    ) -> Annotated<Breadcrumb> {
        if let Some(ref mut breadcrumb) = breadcrumb.0 {
            breadcrumb.ty.get_or_insert_with(|| "default".to_string());
            breadcrumb.level.get_or_insert_with(|| Level::Info);
        }
        breadcrumb
    }

    fn process_request(
        &self,
        mut request: Annotated<Request>,
        _state: ProcessingState,
    ) -> Annotated<Request> {
        // Fill in ip addresses marked as {{auto}}
        if let Some(ref mut request) = request.0 {
            if let Some(ref client_ip) = self.client_ip {
                if let Some(ref mut env) = request.env.0 {
                    if env.get("REMOTE_ADDR").and_then(|x| x.0.as_ref())
                        == Some(&Value::String("{{auto}}".to_string()))
                    {
                        env.insert(
                            "REMOTE_ADDR".to_string(),
                            Annotated::new(Value::String(client_ip.clone())),
                        );
                    }
                }
            }
        }

        request
    }

    fn process_user(&self, mut user: Annotated<User>, _state: ProcessingState) -> Annotated<User> {
        // Fill in ip addresses marked as {{auto}}
        if let Some(ref mut user) = user.0 {
            if let Some(ref client_ip) = self.client_ip {
                if user.ip_address.0.as_ref().map(|x| &**x) == Some("{{auto}}") {
                    user.ip_address.0 = Some(client_ip.clone());
                }
            }
        }
        user
    }

    fn process_client_sdk_info(
        &self,
        mut info: Annotated<ClientSdkInfo>,
        _state: ProcessingState,
    ) -> Annotated<ClientSdkInfo> {
        if info.0.is_none() {
            if let Some(ref auth) = self.auth {
                info.0 = parse_client_as_sdk(&auth);
            }
        }

        info
    }

    fn process_exception(
        &self,
        exception: Annotated<Exception>,
        state: ProcessingState,
    ) -> Annotated<Exception> {
        let _state = state;
        exception
    }
}
