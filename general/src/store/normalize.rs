use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::mem;
use std::sync::Arc;

use chrono::{Duration, Utc};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use smallvec::SmallVec;
use uuid::Uuid;

use crate::processor::{MaxChars, ProcessValue, ProcessingState, Processor};
use crate::protocol::{
    AsPair, Breadcrumb, ClientSdkInfo, Context, DebugImage, Event, EventId, EventType, Exception,
    Frame, IpAddr, Level, LogEntry, Request, Stacktrace, Tags, User,
};
use crate::store::{GeoIpLookup, StoreConfig};
use crate::types::{Annotated, Empty, Error, ErrorKind, Meta, Object, ValueAction};

mod contexts;
mod logentry;
mod mechanism;
mod request;
mod stacktrace;

struct DedupCache(SmallVec<[u64; 16]>);

impl DedupCache {
    pub fn new() -> Self {
        DedupCache(Default::default())
    }

    pub fn probe<H: Hash>(&mut self, element: H) -> bool {
        let mut hasher = DefaultHasher::new();
        element.hash(&mut hasher);
        let hash = hasher.finish();

        if self.0.contains(&hash) {
            false
        } else {
            self.0.push(hash);
            true
        }
    }
}

/// The processor that normalizes events for store.
pub struct NormalizeProcessor<'a> {
    config: Arc<StoreConfig>,
    geoip_lookup: Option<&'a GeoIpLookup>,
}

impl<'a> NormalizeProcessor<'a> {
    /// Creates a new normalization processor.
    pub fn new(config: Arc<StoreConfig>, geoip_lookup: Option<&'a GeoIpLookup>) -> Self {
        NormalizeProcessor {
            config,
            geoip_lookup,
        }
    }

    /// Returns the SDK info from the config.
    fn get_sdk_info(&self) -> Option<ClientSdkInfo> {
        self.config.client.as_ref().and_then(|client| {
            client
                .splitn(2, '/')
                .collect_tuple()
                .or_else(|| client.splitn(2, ' ').collect_tuple())
                .map(|(name, version)| ClientSdkInfo {
                    name: Annotated::new(name.to_owned()),
                    version: Annotated::new(version.to_owned()),
                    ..Default::default()
                })
        })
    }

    /// Ensures that the `release` and `dist` fields match up.
    fn normalize_release_dist(&self, event: &mut Event) {
        if event.dist.value().is_some() && event.release.value().is_empty() {
            event.dist.set_value(None);
        }

        if let Some(dist) = event.dist.value_mut() {
            if dist.trim() != dist {
                *dist = dist.trim().to_string();
            }
        }
    }

    /// Validates the timestamp range and sets a default value.
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

    /// Removes internal tags and adds tags for well-known attributes.
    fn normalize_event_tags(&self, event: &mut Event) {
        let tags = &mut event.tags.value_mut().get_or_insert_with(Tags::default).0;
        let environment = &mut event.environment;
        if environment.is_empty() {
            *environment = Annotated::empty();
        }

        // Fix case where legacy apps pass environment as a tag instead of a top level key
        if let Some(tag) = tags.remove("environment").and_then(|tag| tag.into_value()) {
            environment.get_or_insert_with(|| tag);
        }

        // Remove internal tags, that are generated with a `sentry:` prefix when saving the event.
        // They are not allowed to be set by the client due to ambiguity. Also, deduplicate tags.
        let mut tag_cache = DedupCache::new();
        tags.retain(|entry| {
            match entry.value() {
                Some(tag) => match tag.key().unwrap_or_default() {
                    "" | "release" | "dist" | "user" | "filename" | "function" => false,
                    name => tag_cache.probe(name),
                },
                // ToValue will decide if we should skip serializing Annotated::empty()
                None => true,
            }
        });

        for tag in tags.iter_mut() {
            tag.apply(|tag, meta| {
                if let Some(key) = tag.key() {
                    if bytecount::num_chars(key.as_bytes()) > MaxChars::TagKey.limit() {
                        meta.add_error(Error::new(ErrorKind::ValueTooLong));
                        return ValueAction::DeleteHard;
                    }
                }

                if let Some(value) = tag.value() {
                    if value.is_empty() {
                        meta.add_error(Error::nonempty());
                        return ValueAction::DeleteHard;
                    }

                    if bytecount::num_chars(value.as_bytes()) > MaxChars::TagValue.limit() {
                        meta.add_error(Error::new(ErrorKind::ValueTooLong));
                        return ValueAction::DeleteHard;
                    }
                }

                ValueAction::Keep
            });
        }

        let server_name = std::mem::replace(&mut event.server_name, Annotated::empty());
        if server_name.value().is_some() {
            tags.insert("server_name".to_string(), server_name);
        }

        let site = std::mem::replace(&mut event.site, Annotated::empty());
        if site.value().is_some() {
            tags.insert("site".to_string(), site);
        }
    }

    /// Infers the `EventType` from the event's interfaces.
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

    /// Inserts the IP address into the user interface. Creates the user if it doesn't exist.
    fn normalize_user_ip(&self, event: &mut Event) {
        let remote_addr = event
            .request
            .value()
            .and_then(|request| request.env.value())
            .and_then(|env| env.get("REMOTE_ADDR"))
            .and_then(|value| value.as_str())
            .and_then(|ip| IpAddr::parse(ip).ok());

        // If there is no User ip_address, update it either from the Http interface
        // or the client_ip of the request.
        if let Some(ip_address) = remote_addr {
            self.set_user_ip(&mut event.user, ip_address);
        } else if let Some(ref ip_address) = self.config.client_ip {
            self.set_user_ip(&mut event.user, ip_address.clone());
        }
    }

    /// Creates or updates the user's IP address.
    fn set_user_ip(&self, user: &mut Annotated<User>, ip_address: IpAddr) {
        let user = user.value_mut().get_or_insert_with(User::default);
        if user.ip_address.value().is_none() {
            user.ip_address = Annotated::new(ip_address);
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

impl<'a> Processor for NormalizeProcessor<'a> {
    fn process_event(
        &mut self,
        event: &mut Event,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        event.process_child_values(self, state);

        // Override internal attributes, even if they were set in the payload
        event.project = Annotated::from(self.config.project_id);
        event.key_id = Annotated::from(self.config.key_id.clone());
        event.ty = Annotated::from(self.infer_event_type(event));
        event.version = Annotated::from(self.config.protocol_version.clone());

        // Validate basic attributes
        event.platform.apply(|platform, _| {
            if self.config.valid_platforms.contains(platform) {
                ValueAction::Keep
            } else {
                ValueAction::DeleteSoft
            }
        });

        // Default required attributes, even if they have errors
        event.errors.get_or_insert_with(Vec::new);
        event.level.get_or_insert_with(|| Level::Error);
        event.id.get_or_insert_with(|| EventId(Uuid::new_v4()));
        event.platform.get_or_insert_with(|| "other".to_string());
        event.logger.get_or_insert_with(String::new);
        event.extra.get_or_insert_with(Object::new);
        if event.client_sdk.value().is_none() {
            event.client_sdk.set_value(self.get_sdk_info());
        }

        event
            .stacktrace
            .apply(stacktrace::process_non_raw_stacktrace);

        // Normalize connected attributes and interfaces
        self.normalize_release_dist(event);
        self.normalize_timestamps(event);
        self.normalize_event_tags(event);
        self.normalize_exceptions(event);
        self.normalize_user_ip(event);

        ValueAction::Keep
    }

    fn process_breadcrumb(
        &mut self,
        breadcrumb: &mut Breadcrumb,
        _meta: &mut Meta,
        state: &ProcessingState<'_>,
    ) -> ValueAction {
        breadcrumb.process_child_values(self, state);

        if breadcrumb.ty.value().is_empty() {
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

        let client_ip = self.config.client_ip.as_ref();
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
            let data = user.data.value_mut().get_or_insert_with(Object::new);
            data.extend(std::mem::replace(&mut user.other, Object::new()).into_iter());
        }

        user.process_child_values(self, state);

        // Fill in ip addresses marked as {{auto}}
        if let Some(ip_address) = user.ip_address.value_mut() {
            if ip_address.is_auto() {
                if let Some(ref client_ip) = self.config.client_ip {
                    *ip_address = client_ip.clone();
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

    fn process_debug_image(
        &mut self,
        image: &mut DebugImage,
        meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        match image {
            DebugImage::Other(_) => {
                meta.add_error(Error::invalid("unsupported debug image type"));
                ValueAction::DeleteSoft
            }
            _ => ValueAction::Keep,
        }
    }

    fn process_logentry(
        &mut self,
        logentry: &mut LogEntry,
        meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        logentry::normalize_logentry(logentry, meta)
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

        if exception.ty.value().is_empty() {
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

        if exception.ty.value().is_empty() && exception.value.value().is_empty() {
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

        if let Some(lines) = frame.pre_context.value_mut() {
            for line in lines.iter_mut() {
                line.get_or_insert_with(String::new);
            }
        }

        if let Some(lines) = frame.post_context.value_mut() {
            for line in lines.iter_mut() {
                line.get_or_insert_with(String::new);
            }
        }

        if frame.context_line.value().is_none()
            && (!frame.pre_context.is_empty() || !frame.post_context.is_empty())
        {
            frame.context_line.set_value(Some(String::new()));
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

        // needs to run after process_frame because of `in_app`
        if let Some(limit) = self.config.max_stacktrace_frames {
            stacktrace
                .frames
                .apply(|frames, _meta| stacktrace::slim_frame_data(frames, limit));
        }

        ValueAction::Keep
    }

    fn process_context(
        &mut self,
        context: &mut Context,
        _meta: &mut Meta,
        _state: &ProcessingState<'_>,
    ) -> ValueAction {
        contexts::normalize_context(context);
        ValueAction::Keep
    }
}

#[cfg(test)]
use crate::{
    processor::process_value,
    protocol::{PairList, TagEntry},
    types::Value,
};

#[cfg(test)]
impl Default for NormalizeProcessor<'_> {
    fn default() -> Self {
        NormalizeProcessor::new(Arc::new(StoreConfig::default()), None)
    }
}

#[test]
fn test_handles_type_in_value() {
    let mut processor = NormalizeProcessor::default();

    let mut exception = Annotated::new(Exception {
        value: Annotated::new("ValueError: unauthorized".to_string().into()),
        ..Exception::default()
    });

    process_value(&mut exception, &mut processor, ProcessingState::root());
    let exception = exception.value().unwrap();
    assert_eq_dbg!(exception.value.as_str(), Some("unauthorized"));
    assert_eq_dbg!(exception.ty.as_str(), Some("ValueError"));

    let mut exception = Annotated::new(Exception {
        value: Annotated::new("ValueError:unauthorized".to_string().into()),
        ..Exception::default()
    });

    process_value(&mut exception, &mut processor, ProcessingState::root());
    let exception = exception.value().unwrap();
    assert_eq_dbg!(exception.value.as_str(), Some("unauthorized"));
    assert_eq_dbg!(exception.ty.as_str(), Some("ValueError"));
}

#[test]
fn test_rejects_empty_exception_fields() {
    let mut processor = NormalizeProcessor::new(Arc::new(StoreConfig::default()), None);

    let mut exception = Annotated::new(Exception {
        value: Annotated::new("".to_string().into()),
        ty: Annotated::new("".to_string()),
        ..Default::default()
    });

    process_value(&mut exception, &mut processor, ProcessingState::root());
    assert!(exception.value().is_none());
    assert!(exception.meta().has_errors());
}

#[test]
fn test_json_value() {
    let mut processor = NormalizeProcessor::default();

    let mut exception = Annotated::new(Exception {
        value: Annotated::new(r#"{"unauthorized":true}"#.to_string().into()),
        ..Exception::default()
    });
    process_value(&mut exception, &mut processor, ProcessingState::root());
    let exception = exception.value().unwrap();

    // Don't split a json-serialized value on the colon
    assert_eq_dbg!(exception.value.as_str(), Some(r#"{"unauthorized":true}"#));
    assert_eq_dbg!(exception.ty.value(), None);
}

#[test]
fn test_exception_invalid() {
    let mut processor = NormalizeProcessor::default();

    let mut exception = Annotated::new(Exception::default());
    process_value(&mut exception, &mut processor, ProcessingState::root());

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
    let mut processor = NormalizeProcessor::new(Arc::new(StoreConfig::default()), Some(&lookup));

    let mut user = Annotated::new(User {
        ip_address: Annotated::new(IpAddr("213.47.147.207".to_string())),
        ..User::default()
    });

    process_value(&mut user, &mut processor, ProcessingState::root());

    let expected = Annotated::new(Geo {
        country_code: Annotated::new("AT".to_string()),
        city: Annotated::new("Vienna".to_string()),
        region: Annotated::new("Austria".to_string()),
        ..Geo::default()
    });
    assert_eq_dbg!(user.value().unwrap().geo, expected)
}

#[test]
fn test_user_ip_from_remote_addr() {
    let mut event = Annotated::new(Event {
        request: Annotated::from(Request {
            env: Annotated::new({
                let mut map = Object::new();
                map.insert(
                    "REMOTE_ADDR".to_string(),
                    Annotated::new(Value::String("213.47.147.207".to_string())),
                );
                map
            }),
            ..Request::default()
        }),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let user = event
        .value()
        .unwrap()
        .user
        .value()
        .expect("user was not created");

    let ip_addr = user.ip_address.value().expect("ip address was not created");

    assert_eq_dbg!(ip_addr, &IpAddr("213.47.147.207".to_string()));
}

#[test]
fn test_user_ip_from_invalid_remote_addr() {
    let mut event = Annotated::new(Event {
        request: Annotated::from(Request {
            env: Annotated::new({
                let mut map = Object::new();
                map.insert(
                    "REMOTE_ADDR".to_string(),
                    Annotated::new(Value::String("whoops".to_string())),
                );
                map
            }),
            ..Request::default()
        }),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    assert_eq_dbg!(Annotated::empty(), event.value().unwrap().user);
}

#[test]
fn test_user_ip_from_client_ip() {
    let mut event = Annotated::new(Event::default());

    let mut config = StoreConfig::default();
    config.client_ip = Some(IpAddr::parse("213.47.147.207").unwrap());

    let mut processor = NormalizeProcessor::new(Arc::new(config), None);
    process_value(&mut event, &mut processor, ProcessingState::root());

    let user = event
        .value()
        .unwrap()
        .user
        .value()
        .expect("user was not created");

    let ip_addr = user.ip_address.value().expect("ip address was not created");

    assert_eq_dbg!(ip_addr, &IpAddr("213.47.147.207".to_string()));
}

#[test]
fn test_environment_tag_is_moved() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(vec![Annotated::new(TagEntry(
            Annotated::new("environment".to_string()),
            Annotated::new("despacito".to_string()),
        ))]))),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let event = event.value().unwrap();

    assert_eq_dbg!(event.environment.as_str(), Some("despacito"));
    assert_eq_dbg!(event.tags.value(), Some(&Tags(vec![].into())));
}

#[test]
fn test_empty_environment_is_removed_and_overwritten_with_tag() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(vec![Annotated::new(TagEntry(
            Annotated::new("environment".to_string()),
            Annotated::new("despacito".to_string()),
        ))]))),
        environment: Annotated::new("".to_string()),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let event = event.value().unwrap();

    assert_eq_dbg!(event.environment.as_str(), Some("despacito"));
    assert_eq_dbg!(event.tags.value(), Some(&Tags(vec![].into())));
}

#[test]
fn test_empty_environment_is_removed() {
    let mut event = Annotated::new(Event {
        environment: Annotated::new("".to_string()),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let event = event.value().unwrap();
    assert_eq_dbg!(event.environment.value(), None);
}

#[test]
fn test_top_level_keys_moved_into_tags() {
    let mut event = Annotated::new(Event {
        server_name: Annotated::new("foo".to_string()),
        site: Annotated::new("foo".to_string()),
        tags: Annotated::new(Tags(PairList(vec![
            Annotated::new(TagEntry(
                Annotated::new("site".to_string()),
                Annotated::new("old".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("server_name".to_string()),
                Annotated::new("old".to_string()),
            )),
        ]))),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let event = event.value().unwrap();

    assert_eq_dbg!(event.site.value(), None);
    assert_eq_dbg!(event.server_name.value(), None);

    assert_eq_dbg!(
        event.tags.value(),
        Some(&Tags(PairList(vec![
            Annotated::new(TagEntry(
                Annotated::new("site".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("server_name".to_string()),
                Annotated::new("foo".to_string()),
            )),
        ])))
    );
}

#[test]
fn test_internal_tags_removed() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(vec![
            Annotated::new(TagEntry(
                Annotated::new("release".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("dist".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("user".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("filename".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("function".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("something".to_string()),
                Annotated::new("else".to_string()),
            )),
        ]))),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    assert_eq!(event.value().unwrap().tags.value().unwrap().len(), 1);
}

#[test]
fn test_empty_tags_removed() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(vec![
            Annotated::new(TagEntry(
                Annotated::new("".to_string()),
                Annotated::new("foo".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("foo".to_string()),
                Annotated::new("".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("something".to_string()),
                Annotated::new("else".to_string()),
            )),
        ]))),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let tags = event.value().unwrap().tags.value().unwrap();
    assert_eq!(tags.len(), 2);

    assert_eq!(
        tags.get(0).unwrap(),
        &Annotated::from_error(Error::nonempty(), None)
    )
}

#[test]
fn test_tags_deduplicated() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(vec![
            Annotated::new(TagEntry(
                Annotated::new("foo".to_string()),
                Annotated::new("1".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("bar".to_string()),
                Annotated::new("1".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("foo".to_string()),
                Annotated::new("2".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("bar".to_string()),
                Annotated::new("2".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("foo".to_string()),
                Annotated::new("3".to_string()),
            )),
        ]))),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    // should keep the first occurrence of every tag
    assert_eq_dbg!(
        event.value().unwrap().tags.value().unwrap(),
        &Tags(PairList(vec![
            Annotated::new(TagEntry(
                Annotated::new("foo".to_string()),
                Annotated::new("1".to_string()),
            )),
            Annotated::new(TagEntry(
                Annotated::new("bar".to_string()),
                Annotated::new("1".to_string()),
            )),
        ]))
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
        ..User::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut user, &mut processor, ProcessingState::root());

    let user = user.value().unwrap();

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

#[test]
fn test_unknown_debug_image() {
    use crate::protocol::DebugMeta;

    let mut event = Annotated::new(Event {
        debug_meta: Annotated::new(DebugMeta {
            images: Annotated::new(vec![Annotated::new(DebugImage::Other(Default::default()))]),
            ..DebugMeta::default()
        }),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let expected = Annotated::new(DebugMeta {
        images: Annotated::new(vec![Annotated::from_error(
            Error::invalid("unsupported debug image type"),
            Some(Value::Object(Default::default())),
        )]),
        ..DebugMeta::default()
    });

    assert_eq_dbg!(expected, event.value().unwrap().debug_meta);
}

#[test]
fn test_context_line_default() {
    let mut frame = Annotated::new(Frame {
        pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_string())]),
        post_context: Annotated::new(vec![Annotated::new("".to_string()), Annotated::default()]),
        ..Frame::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut frame, &mut processor, ProcessingState::root());

    let frame = frame.value().unwrap();
    assert_eq_dbg!(frame.context_line.as_str(), Some(""));
}

#[test]
fn test_context_line_retain() {
    let mut frame = Annotated::new(Frame {
        pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_string())]),
        post_context: Annotated::new(vec![Annotated::new("".to_string()), Annotated::default()]),
        context_line: Annotated::new("some line".to_string()),
        ..Frame::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut frame, &mut processor, ProcessingState::root());

    let frame = frame.value().unwrap();
    assert_eq_dbg!(frame.context_line.as_str(), Some("some line"));
}

#[test]
fn test_frame_null_context_lines() {
    let mut frame = Annotated::new(Frame {
        pre_context: Annotated::new(vec![Annotated::default(), Annotated::new("".to_string())]),
        post_context: Annotated::new(vec![Annotated::new("".to_string()), Annotated::default()]),
        ..Frame::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut frame, &mut processor, ProcessingState::root());

    let frame = frame.value().unwrap();
    assert_eq_dbg!(
        *frame.pre_context.value().unwrap(),
        vec![
            Annotated::new("".to_string()),
            Annotated::new("".to_string())
        ],
    );
    assert_eq_dbg!(
        *frame.post_context.value().unwrap(),
        vec![
            Annotated::new("".to_string()),
            Annotated::new("".to_string())
        ],
    );
}

#[test]
fn test_too_long_tags() {
    let mut event = Annotated::new(Event {
        tags: Annotated::new(Tags(PairList(
            vec![Annotated::new(TagEntry(
                Annotated::new("foobar".to_string()),
                Annotated::new("...........................................................................................................................................................................................................".to_string()),
            )),Annotated::new(TagEntry(
                Annotated::new("foooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo".to_string()),
                Annotated::new("bar".to_string()),
            ))]),
        )),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let event = event.value().unwrap();

    assert_eq_dbg!(
        event.tags.value(),
        Some(&Tags(PairList(vec![
            Annotated::from_error(Error::new(ErrorKind::ValueTooLong), None),
            Annotated::from_error(Error::new(ErrorKind::ValueTooLong), None)
        ])))
    );
}

#[test]
fn test_regression_backfills_abs_path_even_when_moving_stacktrace() {
    use crate::protocol::Values;
    let mut event = Annotated::new(Event {
        exceptions: Annotated::new(Values::new(vec![Annotated::new(Exception {
            ty: Annotated::new("FooDivisionError".to_string()),
            value: Annotated::new("hi".to_string().into()),
            ..Exception::default()
        })])),
        stacktrace: Annotated::new(Stacktrace {
            frames: Annotated::new(vec![Annotated::new(Frame {
                module: Annotated::new("MyModule".to_string()),
                filename: Annotated::new("MyFilename".to_string()),
                function: Annotated::new("Void FooBar()".to_string()),
                ..Frame::default()
            })]),
            ..Stacktrace::default()
        }),
        ..Event::default()
    });

    let mut processor = NormalizeProcessor::default();
    process_value(&mut event, &mut processor, ProcessingState::root());

    let expected = Annotated::new(Stacktrace {
        frames: Annotated::new(vec![Annotated::new(Frame {
            module: Annotated::new("MyModule".to_string()),
            filename: Annotated::new("MyFilename".to_string()),
            abs_path: Annotated::new("MyFilename".to_string()),
            function: Annotated::new("Void FooBar()".to_string()),
            ..Frame::default()
        })]),
        ..Stacktrace::default()
    });

    assert_eq_dbg!(
        event
            .value()
            .unwrap()
            .exceptions
            .value()
            .unwrap()
            .values
            .value()
            .unwrap()
            .get(0)
            .unwrap()
            .value()
            .unwrap()
            .stacktrace,
        expected
    );
}

#[test]
fn test_parses_sdk_info_from_header() {
    let mut event = Annotated::new(Event::default());
    let mut processor = NormalizeProcessor::new(
        Arc::new(StoreConfig {
            client: Some("_fooBar/0.0.0".to_string()),
            ..StoreConfig::default()
        }),
        None,
    );
    process_value(&mut event, &mut processor, ProcessingState::root());

    assert_eq_dbg!(
        event.value().unwrap().client_sdk,
        Annotated::new(ClientSdkInfo {
            name: Annotated::new("_fooBar".to_string()),
            version: Annotated::new("0.0.0".to_string()),
            ..ClientSdkInfo::default()
        })
    );
}
