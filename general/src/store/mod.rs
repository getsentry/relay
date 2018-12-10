//! Utility code for sentry's internal store.
use std::collections::BTreeSet;
use std::mem;

use chrono::{Duration, Utc};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{
    Breadcrumb, ClientSdkInfo, Event, EventId, EventType, Exception, Frame, IpAddr, Level, Request,
    Stacktrace, TagEntry, Tags, User,
};
use crate::types::{Annotated, Error, ErrorKind, Meta, Object, ValueAction};

mod geo;
mod mechanism;
mod remove_other;
mod request;
mod schema;
mod stacktrace;
mod trimming;

pub use crate::store::geo::GeoIpLookup;

/// The config for store.
#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(default)]
pub struct StoreConfig {
    pub project_id: Option<u64>,
    pub client_ip: Option<String>,
    pub client: Option<String>,
    pub is_public_auth: bool,
    pub key_id: Option<String>,
    pub protocol_version: Option<String>,
    pub stacktrace_frames_hard_limit: Option<usize>,
    pub valid_platforms: BTreeSet<String>,
    pub max_secs_in_future: Option<i64>,
    pub max_secs_in_past: Option<i64>,
}

impl StoreConfig {
    /// Returns the SDK info.
    fn get_sdk_info(&self) -> Option<ClientSdkInfo> {
        self.client.as_ref().and_then(|client| {
            client
                .splitn(1, '/')
                .collect_tuple()
                .or_else(|| client.splitn(1, ' ').collect_tuple())
                .map(|(name, version)| ClientSdkInfo {
                    name: Annotated::new(name.to_owned()),
                    version: Annotated::new(version.to_owned()),
                    ..Default::default()
                })
        })
    }
}

/// The processor that normalizes events for store.
pub struct StoreNormalizeProcessor<'a> {
    config: StoreConfig,
    geoip_lookup: Option<&'a GeoIpLookup>,
    trimming_processor: trimming::TrimmingProcessor,
}

impl<'a> StoreNormalizeProcessor<'a> {
    /// Creates a new normalization processor.
    pub fn new(
        config: StoreConfig,
        geoip_lookup: Option<&'a GeoIpLookup>,
    ) -> StoreNormalizeProcessor<'a> {
        StoreNormalizeProcessor {
            config,
            geoip_lookup,
            trimming_processor: trimming::TrimmingProcessor::new(),
        }
    }

    /// Returns a reference to the config.
    pub fn config(&self) -> &StoreConfig {
        &self.config
    }

    fn normalize_timestamps(&self, event: &mut Event) {
        let current_timestamp = Utc::now();
        if event.received.value().is_none() {
            event.received.set_value(Some(current_timestamp));
        }

        event.timestamp.apply(|timestamp, meta| {
            if let Some(secs) = self.config.max_secs_in_future {
                if *timestamp > current_timestamp + Duration::seconds(secs) {
                    meta.add_error(ErrorKind::FutureTimestamp);
                    return ValueAction::DeleteSoft;
                }
            }

            if let Some(secs) = self.config.max_secs_in_past {
                if *timestamp < current_timestamp - Duration::seconds(secs) {
                    meta.add_error(ErrorKind::PastTimestamp);
                    return ValueAction::DeleteSoft;
                }
            }

            ValueAction::Keep
        });

        if event.timestamp.value().is_none() {
            event.timestamp.set_value(Some(current_timestamp));
        }
    }

    fn normalize_event_tags(&self, event: &mut Event) {
        let tags = &mut event.tags.value_mut().get_or_insert_with(Tags::default).0;
        let environment = &mut event.environment;

        tags.retain(|tag| {
            let tag = match tag.value() {
                Some(x) => x,
                None => return true, // ToValue will decide if we should skip serializing Annotated::empty()
            };

            match tag.key() {
                // These tags are special and are used in pairing with `sentry:{}`. They should not be allowed
                // to be set via data ingest due to ambiguity.
                Some("release") | Some("dist") | Some("filename") | Some("function") => false,

                // Fix case where legacy apps pass environment as a tag instead of a top level key
                Some("environment") => {
                    if let Some(ref value) = tag.value() {
                        environment.get_or_insert_with(|| value.to_string());
                    }

                    false
                }
                _ => true,
            }
        });

        if event.server_name.value().is_some() {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("server_name".to_string()),
                std::mem::replace(&mut event.server_name, Annotated::empty()),
            )));
        }

        if event.site.value().is_some() {
            tags.push(Annotated::new(TagEntry(
                Annotated::new("site".to_string()),
                std::mem::replace(&mut event.site, Annotated::empty()),
            )));
        }
    }

    fn infer_event_type(&self, event: &Event) -> EventType {
        // port of src/sentry/eventtypes
        //
        // the SDKs currently do not describe event types, and we must infer
        // them from available attributes

        let has_exceptions = event
            .exceptions
            .value()
            .and_then(|exceptions| exceptions.values.value())
            .filter(|values| !values.is_empty())
            .is_some();

        if has_exceptions {
            EventType::Error
        } else if event.csp.value().is_some() {
            EventType::Csp
        } else if event.hpkp.value().is_some() {
            EventType::Hpkp
        } else if event.expectct.value().is_some() {
            EventType::ExpectCT
        } else if event.expectstaple.value().is_some() {
            EventType::ExpectStaple
        } else {
            EventType::Default
        }
    }

    fn normalize_user_ip(&self, event: &mut Event) {
        let http_ip = event
            .request
            .value()
            .and_then(|request| request.env.value())
            .and_then(|env| env.get("REMOTE_ADDR"))
            .and_then(|addr| addr.as_str());

        // If there is no User ip_address, update it either from the Http interface
        // or the client_ip of the request.
        if let Some(http_ip) = http_ip {
            self.set_user_ip(&mut event.user, http_ip);
        } else if let Some(ref client_ip) = self.config.client_ip {
            let should_use_client_ip = self.config.is_public_auth
                || match event.platform.as_str() {
                    Some("javascript") | Some("cocoa") | Some("objc") => true,
                    _ => false,
                };

            if should_use_client_ip {
                self.set_user_ip(&mut event.user, client_ip.as_str());
            }
        }
    }

    fn set_user_ip(&self, user: &mut Annotated<User>, ip_address: &str) {
        let user = user.value_mut().get_or_insert_with(User::default);
        if user.ip_address.value().is_none() {
            user.ip_address = Annotated::new(IpAddr(ip_address.to_string()));
        }
    }

    fn normalize_exceptions(&self, event: &mut Event) {
        let os_hint = mechanism::OsHint::from_event(&event);

        if let Some(exception_values) = event.exceptions.value_mut() {
            if let Some(exceptions) = exception_values.values.value_mut() {
                if exceptions.len() == 1 && event.stacktrace.value().is_some() {
                    if let Some(exception) = exceptions.get_mut(0) {
                        if let Some(exception) = exception.value_mut() {
                            mem::swap(&mut exception.stacktrace, &mut event.stacktrace);
                            event.stacktrace = Annotated::empty();
                        }
                    }
                }

                // Exception mechanism needs SDK information to resolve proper names in
                // exception meta (such as signal names). "SDK Information" really means
                // the operating system version the event was generated on. Some
                // normalization still works without sdk_info, such as mach_exception
                // names (they can only occur on macOS).
                for exception in exceptions {
                    if let Some(exception) = exception.value_mut() {
                        if let Some(mechanism) = exception.mechanism.value_mut() {
                            mechanism::normalize_mechanism_meta(mechanism, os_hint);
                        }
                    }
                }
            }
        }
    }
}

impl<'a> Processor for StoreNormalizeProcessor<'a> {
    // TODO: Reduce cyclomatic complexity of this function
    #[allow(clippy::cyclomatic_complexity)]
    fn process_event(
        &mut self,
        event: &mut Event,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        schema::SchemaProcessor.process_event(event, meta, state);
        event.process_child_values(self, state);

        // Override the project_id, even if it was set in the payload
        if let Some(project_id) = self.config.project_id {
            event.project.set_value(Some(project_id));
        }

        // Override the key_id, even if it was set in the payload
        if let Some(ref key_id) = self.config.key_id {
            event.key_id.set_value(Some(key_id.clone()));
        }

        if event.errors.value().is_none() {
            event.errors.set_value(Some(Vec::new()));
        }

        if event.client_sdk.value().is_none() {
            event.client_sdk.set_value(self.config.get_sdk_info());
        }

        event
            .stacktrace
            .apply(stacktrace::process_non_raw_stacktrace);

        if event.level.value().is_none() {
            event.level.set_value(Some(Level::Error));
        }

        if event.id.value().is_none() {
            event.id.set_value(Some(EventId(Uuid::new_v4())));
        }

        if event.logger.value().is_none() {
            event.logger.set_value(Some(String::new()));
        }

        if event.dist.value().is_some() && event.release.value().is_none() {
            event.dist.set_value(None);
        }

        if let Some(dist) = event.dist.value_mut() {
            if dist.trim() != dist {
                *dist = dist.trim().to_string();
            }
        }

        self.normalize_timestamps(event);

        event.platform.apply(|platform, _| {
            // TODO: keep original value?
            self.config.valid_platforms.contains(platform)
        });

        if event.platform.value().is_none() {
            event.platform.set_value(Some("other".to_string()));
        }

        if event.extra.value().is_none() {
            event.extra.set_value(Some(Object::new()));
        }

        self.normalize_event_tags(event);

        event.ty = Annotated::new(self.infer_event_type(event));

        if let Some(ref version) = self.config.protocol_version {
            event.version = Annotated::new(version.clone());
        }

        self.normalize_exceptions(event);
        self.normalize_user_ip(event);

        remove_other::RemoveOtherProcessor.process_event(event, meta, state);

        // Trimming should happen at end to ensure derived tags are the right length.
        self.trimming_processor.process_event(event, meta, state);

        ValueAction::Keep
    }

    fn process_breadcrumb(
        &mut self,
        breadcrumb: &mut Breadcrumb,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        breadcrumb.process_child_values(self, state);

        if breadcrumb.ty.value().is_none() {
            breadcrumb.ty.set_value(Some("default".to_string()));
        }

        if breadcrumb.level.value().is_none() {
            breadcrumb.level.set_value(Some(Level::Info));
        }

        ValueAction::Keep
    }

    fn process_request(
        &mut self,
        request: &mut Request,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        request.process_child_values(self, state);

        let client_ip = self.config.client_ip.as_ref().map(String::as_str);
        request::normalize_request(request, client_ip);

        ValueAction::Keep
    }

    fn process_user(
        &mut self,
        user: &mut User,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        if !user.other.is_empty() {
            let data = &mut user.data.0.get_or_insert_with(Object::new);
            data.extend(std::mem::replace(&mut user.other, Object::new()).into_iter());
        }

        user.process_child_values(self, state);

        // Fill in ip addresses marked as {{auto}}
        if let Some(ref mut ip_address) = user.ip_address.value_mut() {
            if ip_address.is_auto() {
                if let Some(ref client_ip) = self.config.client_ip {
                    *ip_address = IpAddr(client_ip.clone());
                }
            }
        }

        // Infer user.geo from user.ip_address
        if user.geo.value().is_none() {
            if let Some(ref geoip_lookup) = self.geoip_lookup {
                if let Some(ip_address) = user.ip_address.value() {
                    if let Ok(Some(geo)) = geoip_lookup.lookup(ip_address.as_str()) {
                        user.geo.set_value(Some(geo));
                    }
                }
            }
        }

        ValueAction::Keep
    }

    fn process_exception(
        &mut self,
        exception: &mut Exception,
        meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        exception.process_child_values(self, state);

        exception
            .stacktrace
            .apply(stacktrace::process_non_raw_stacktrace);

        lazy_static! {
            static ref TYPE_VALUE_RE: Regex = Regex::new(r"^(\w+):(.*)$").unwrap();
        }

        if exception.ty.value().is_none() {
            if let Some(value_str) = exception.value.value_mut() {
                let new_values = TYPE_VALUE_RE
                    .captures(value_str)
                    .map(|cap| (cap[1].to_string(), cap[2].trim().to_string().into()));

                if let Some((new_type, new_value)) = new_values {
                    exception.ty.set_value(Some(new_type));
                    *value_str = new_value;
                }
            }
        }

        if exception.ty.value().is_none() && exception.value.value().is_none() {
            meta.add_error(Error::with(ErrorKind::MissingAttribute, |error| {
                error.insert("attribute", "type or value");
            }));
            return ValueAction::DeleteSoft;
        }

        ValueAction::Keep
    }

    fn process_frame(
        &mut self,
        frame: &mut Frame,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        frame.process_child_values(self, state);

        if frame.function.as_str() == Some("?") {
            frame.function.set_value(None);
        }

        if frame.symbol.as_str() == Some("?") {
            frame.symbol.set_value(None);
        }

        if let Some(lines) = frame.pre_lines.value_mut() {
            for line in lines {
                if line.value().is_none() {
                    line.set_value(Some("".to_string()));
                }
            }
        }

        if let Some(lines) = frame.post_lines.value_mut() {
            for line in lines {
                if line.value().is_none() {
                    line.set_value(Some("".to_string()));
                }
            }
        }

        ValueAction::Keep
    }

    fn process_stacktrace(
        &mut self,
        stacktrace: &mut Stacktrace,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        if let Some(limit) = self.config.stacktrace_frames_hard_limit {
            stacktrace
                .frames
                .apply(|frames, meta| stacktrace::enforce_frame_hard_limit(frames, meta, limit));
        }

        stacktrace.process_child_values(self, state);

        // TODO: port slim_frame_data and call it here (needs to run after process_frame because of
        // `in_app`)

        ValueAction::Keep
    }
}

#[cfg(test)]
use {crate::processor::process_value, crate::types::Value};

#[test]
fn test_handles_type_in_value() {
    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);

    let mut exception = Annotated::new(Exception {
        value: Annotated::new("ValueError: unauthorized".to_string().into()),
        ..Default::default()
    });

    process_value(&mut exception, &mut processor, Default::default());
    let exception = exception.value().unwrap();
    assert_eq_dbg!(exception.value.as_str(), Some("unauthorized"));
    assert_eq_dbg!(exception.ty.as_str(), Some("ValueError"));

    let mut exception = Annotated::new(Exception {
        value: Annotated::new("ValueError:unauthorized".to_string().into()),
        ..Default::default()
    });

    process_value(&mut exception, &mut processor, Default::default());
    let exception = exception.value().unwrap();
    assert_eq_dbg!(exception.value.as_str(), Some("unauthorized"));
    assert_eq_dbg!(exception.ty.as_str(), Some("ValueError"));
}

#[test]
fn test_json_value() {
    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);

    let mut exception = Annotated::new(Exception {
        value: Annotated::new(r#"{"unauthorized":true}"#.to_string().into()),
        ..Default::default()
    });
    process_value(&mut exception, &mut processor, Default::default());
    let exception = exception.value().unwrap();

    // Don't split a json-serialized value on the colon
    assert_eq_dbg!(exception.value.as_str(), Some(r#"{"unauthorized":true}"#));
    assert_eq_dbg!(exception.ty.value(), None);
}

#[test]
fn test_exception_invalid() {
    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);

    let mut exception = Annotated::new(Exception::default());
    process_value(&mut exception, &mut processor, Default::default());

    let expected = Error::with(ErrorKind::MissingAttribute, |error| {
        error.insert("attribute", "type or value");
    });

    assert_eq_dbg!(
        exception.meta().iter_errors().collect_tuple(),
        Some((&expected,))
    );
}

#[test]
fn test_geo_from_ip_address() {
    use crate::protocol::Geo;

    let lookup = GeoIpLookup::open("../GeoLite2-City.mmdb").unwrap();
    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), Some(&lookup));

    let mut user = Annotated::new(User {
        ip_address: Annotated::new(IpAddr("213.47.147.207".to_string())),
        ..Default::default()
    });

    process_value(&mut user, &mut processor, Default::default());

    let expected = Annotated::new(Geo {
        country_code: Annotated::new("AT".to_string()),
        city: Annotated::new("Vienna".to_string()),
        region: Annotated::new("Austria".to_string()),
        ..Default::default()
    });
    assert_eq_dbg!(user.value().unwrap().geo, expected)
}

#[test]
fn test_schema_processor_invoked() {
    use crate::protocol::User;

    let mut event = Annotated::new(Event {
        user: Annotated::new(User {
            email: Annotated::new("bananabread".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    });

    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);
    process_value(&mut event, &mut processor, Default::default());

    assert_eq_dbg!(
        event.value().unwrap().user.value().unwrap().email.value(),
        None
    );
}

#[test]
fn test_environment_tag_is_moved() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(
            vec![Annotated::new(TagEntry(
                Annotated::new("environment".to_string()),
                Annotated::new("despacito".to_string()),
            ))]
            .into(),
        )),
        ..Default::default()
    });

    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);
    process_value(&mut event, &mut processor, Default::default());

    let event = event.0.unwrap();

    assert_eq_dbg!(event.environment.0, Some("despacito".to_string()));

    assert_eq_dbg!(event.tags.0, Some(Tags(vec![].into())));
}

#[test]
fn test_top_level_keys_moved_into_tags() {
    let mut event = Annotated::new(Event {
        server_name: Annotated::new("foo".to_string()),
        site: Annotated::new("foo".to_string()),
        ..Default::default()
    });

    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);
    process_value(&mut event, &mut processor, Default::default());

    let event = event.0.unwrap();

    assert_eq_dbg!(event.site.0, None);
    assert_eq_dbg!(event.server_name.0, None);

    assert_eq_dbg!(
        event.tags.0,
        Some(Tags(
            vec![
                Annotated::new(TagEntry(
                    Annotated::new("server_name".to_string()),
                    Annotated::new("foo".to_string()),
                )),
                Annotated::new(TagEntry(
                    Annotated::new("site".to_string()),
                    Annotated::new("foo".to_string()),
                ))
            ]
            .into()
        ))
    );
}

#[test]
fn test_user_data_moved() {
    let mut user = Annotated::new(User {
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_owned())),
            );
            map
        },
        ..Default::default()
    });

    let mut processor = StoreNormalizeProcessor::new(StoreConfig::default(), None);
    process_value(&mut user, &mut processor, Default::default());

    let user = user.0.unwrap();

    assert_eq_dbg!(user.data, {
        let mut map = Object::new();
        map.insert(
            "other".to_string(),
            Annotated::new(Value::String("value".to_owned())),
        );
        Annotated::new(map)
    });

    assert_eq_dbg!(user.other, Object::new());
}
