use chrono::Utc;

use itertools::Itertools;

use crate::meta::{Annotated, Value};
use crate::processor::{ProcessingState, Processor};
use crate::protocol::{ClientSdkInfo, Event, Request, Tags, User};
use crate::types::{EventType, Level};

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

pub struct StoreProcessor {
    project_id: Option<u64>,
    client_ip: Option<String>,
    auth: Option<StoreAuth>,
    key_id: Option<String>,
}

impl Processor for StoreProcessor {
    fn process_event(
        &self,
        mut event: Annotated<Event>,
        _state: ProcessingState,
    ) -> Annotated<Event> {
        if let Some(ref mut event) = event.0 {
            if let Some(ref project_id) = self.project_id {
                event.project.0 = Some(project_id.clone());
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

            // port of src/sentry/eventtypes
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

            // TODO: Finish port of EventManager.normalize
        }
        event
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
}
