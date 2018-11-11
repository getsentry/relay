//! Utility code for sentry's internal store.
use chrono::Utc;

use itertools::Itertools;
use regex::Regex;
use url::Url;

use std::mem;
use std::path::PathBuf;

use crate::processor::{ProcessValue, ProcessingState, Processor};
use crate::protocol::{
    normalize_mechanism_meta, Breadcrumb, ClientSdkInfo, Event, EventType, Exception, Frame, Geo,
    IpAddr, Level, OsHint, Request, Stacktrace, Tags, User,
};
use crate::types::{Annotated, Object, Value};

mod geo;

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

fn process_non_raw_stacktrace(stacktrace: &mut Annotated<Stacktrace>) {
    // this processing should only be done for non raw frames (i.e. not for
    // exception.raw_stacktrace)
    if let Some(ref mut stacktrace) = stacktrace.0 {
        if let Some(ref mut frames) = stacktrace.frames.0 {
            for mut frame in frames.iter_mut() {
                process_non_raw_frame(&mut frame);
            }
        }
    }
}

fn parse_type_and_value(
    ty: Annotated<String>,
    value: Annotated<String>,
) -> (Annotated<String>, Annotated<String>) {
    lazy_static! {
        static ref TYPE_VALUE_RE: Regex = Regex::new(r"^(\w+):(.*)$").unwrap();
    }

    if let (None, Some(value_str)) = (ty.0.as_ref(), value.0.as_ref()) {
        if let Some((ref cap,)) = TYPE_VALUE_RE.captures_iter(value_str).collect_tuple() {
            return (
                Annotated::new(cap[1].to_string()),
                Annotated::new(cap[2].trim().to_string()),
            );
        }
    }

    (ty, value)
}

fn is_url(filename: &str) -> bool {
    filename.starts_with("file:")
        || filename.starts_with("http:")
        || filename.starts_with("https:")
        || filename.starts_with("applewebdata:")
}

fn process_non_raw_frame(frame: &mut Annotated<Frame>) {
    if let Some(ref mut frame) = frame.0 {
        if frame.abs_path.0.is_none() {
            frame.abs_path = mem::replace(&mut frame.filename, Annotated::empty());
        }

        if let (None, Some(abs_path)) = (frame.filename.0.as_ref(), frame.abs_path.0.as_ref()) {
            frame.filename = Annotated::new(abs_path.clone());

            if is_url(&abs_path) {
                if let Ok(url) = Url::parse(&abs_path) {
                    let path = url.path();

                    if !path.is_empty() && path != "/" {
                        frame.filename = Annotated::new(path.to_string());
                    }
                }
            }
        }
    }
}

/// Simplified version of sentry.coreapi.Auth
pub struct StoreAuth {
    client: String,
    is_public: bool,
}

/// The processor that normalizes events for store.
#[derive(Default)]
pub struct StoreNormalizeProcessor {
    project_id: Option<u64>,
    client_ip: Option<String>,
    auth: Option<StoreAuth>,
    key_id: Option<String>,
    geoip_path: Option<PathBuf>,
    version: Option<String>,
}

impl Processor for StoreNormalizeProcessor {
    fn process_string(
        &mut self,
        mut value: Annotated<String>,
        state: ProcessingState,
    ) -> Annotated<String> {
        if let Some(max_chars) = state.attrs().max_chars {
            value = value.trim_string(max_chars);
        }
        value
    }

    // TODO: Reduce cyclomatic complexity of this function
    #[cfg_attr(feature = "cargo-clippy", allow(cyclomatic_complexity))]
    fn process_event(
        &mut self,
        event: Annotated<Event>,
        state: ProcessingState,
    ) -> Annotated<Event> {
        let mut event = ProcessValue::process_child_values(event, self, state);

        if let Some(ref mut event) = event.0 {
            if let Some(project_id) = self.project_id {
                event.project.0 = Some(project_id);
            }

            if let Some(ref key_id) = self.key_id {
                event.key_id.0 = Some(key_id.clone());
            }

            event.errors.0.get_or_insert_with(Vec::new);

            // TODO: Interfaces
            process_non_raw_stacktrace(&mut event.stacktrace);

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

            if let Some(ref version) = self.version {
                event.version = Annotated::new(version.clone());
            }

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
                                normalize_mechanism_meta(mechanism, os_hint);
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
                    user.ip_address = Annotated::new(IpAddr(http_ip.clone()));
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
                        user.ip_address = Annotated::new(IpAddr(client_ip.clone()));
                    }
                }
            }
        }
        event
    }

    fn process_breadcrumb(
        &mut self,
        breadcrumb: Annotated<Breadcrumb>,
        state: ProcessingState,
    ) -> Annotated<Breadcrumb> {
        let mut breadcrumb = ProcessValue::process_child_values(breadcrumb, self, state);

        if let Some(ref mut breadcrumb) = breadcrumb.0 {
            breadcrumb.ty.get_or_insert_with(|| "default".to_string());
            breadcrumb.level.get_or_insert_with(|| Level::Info);
        }
        breadcrumb
    }

    fn process_request(
        &mut self,
        request: Annotated<Request>,
        state: ProcessingState,
    ) -> Annotated<Request> {
        let mut request = ProcessValue::process_child_values(request, self, state);

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

    fn process_user(&mut self, user: Annotated<User>, state: ProcessingState) -> Annotated<User> {
        let mut user = ProcessValue::process_child_values(user, self, state);

        if let Some(ref mut user) = user.0 {
            // Fill in ip addresses marked as {{auto}}
            if let Some(ref client_ip) = self.client_ip {
                if user.ip_address.0.as_ref().map_or(false, |x| x.is_auto()) {
                    user.ip_address.0 = Some(IpAddr(client_ip.clone()));
                }
            }

            // Validate email
            let is_valid_email = user
                .email
                .0
                .as_ref()
                .map(|x| x.contains('@'))
                .unwrap_or(true);
            if !is_valid_email {
                user.email.1.add_error(
                    "invalid email address",
                    user.email.0.take().map(Value::String),
                );
            }

            // Infer user.geo from user.ip_address
            if let (None, Some(geoip_path), Some(ip_address)) = (
                user.geo.0.as_ref(),
                self.geoip_path.as_ref(),
                user.ip_address.0.as_ref(),
            ) {
                if let Some(city_info) = geo::lookup_ip(geoip_path, &ip_address.0) {
                    user.geo = Annotated::new(Geo {
                        country_code: Annotated(city_info.country_code, Default::default()),
                        city: Annotated(city_info.city, Default::default()),
                        region: Annotated(city_info.region, Default::default()),
                        ..Default::default()
                    });
                }
            }
        }
        user
    }

    fn process_client_sdk_info(
        &mut self,
        info: Annotated<ClientSdkInfo>,
        state: ProcessingState,
    ) -> Annotated<ClientSdkInfo> {
        let mut info = ProcessValue::process_child_values(info, self, state);

        if info.0.is_none() {
            if let Some(ref auth) = self.auth {
                info.0 = parse_client_as_sdk(&auth);
            }
        }

        info
    }

    fn process_exception(
        &mut self,
        exception: Annotated<Exception>,
        state: ProcessingState,
    ) -> Annotated<Exception> {
        let mut exception = ProcessValue::process_child_values(exception, self, state);

        if let Annotated(Some(ref mut exception), ref mut meta) = exception {
            process_non_raw_stacktrace(&mut exception.stacktrace);

            let (ty, value) = parse_type_and_value(
                exception.ty.clone(),
                exception.value.clone().map_value(|x| x.0),
            );
            exception.ty = ty;
            exception.value = value.map_value(|x| x.into());

            if exception.ty.0.is_none() && exception.value.0.is_none() && !meta.has_errors() {
                meta.add_error("type or value required", None);
            }
        }

        exception
    }

    fn process_frame(
        &mut self,
        frame: Annotated<Frame>,
        state: ProcessingState,
    ) -> Annotated<Frame> {
        let mut frame = ProcessValue::process_child_values(frame, self, state);

        if let Some(ref mut frame) = frame.0 {
            frame.in_app.0.get_or_insert(false);

            if frame.function.0.as_ref().map(|x| x == "?").unwrap_or(false) {
                frame.function.0 = None;
            }

            if frame.symbol.0.as_ref().map(|x| x == "?").unwrap_or(false) {
                frame.symbol.0 = None;
            }

            for mut lines in &mut [&mut frame.pre_lines, &mut frame.post_lines] {
                if let Some(ref mut lines) = lines.0 {
                    for mut line in lines {
                        line.0.get_or_insert_with(String::new);
                    }
                }
            }
        }
        frame
    }
}

#[test]
fn test_basic_trimming() {
    use crate::processor::CapSize;
    use std::iter::repeat;

    let mut processor = StoreNormalizeProcessor::default();

    let event = Annotated::new(Event {
        culprit: Annotated::new(repeat("x").take(300).collect::<String>()),
        ..Default::default()
    });

    let event = event.process(&mut processor);

    assert_eq_dbg!(
        event.0.unwrap().culprit,
        Annotated::new(repeat("x").take(300).collect::<String>()).trim_string(CapSize::Symbol)
    );
}

#[test]
fn test_handles_type_in_value() {
    let mut processor = StoreNormalizeProcessor::default();

    let exception = Annotated::new(Exception {
        value: Annotated::new("ValueError: unauthorized".to_string().into()),
        ..Default::default()
    });

    let exception = exception.process(&mut processor).0.unwrap();
    assert_eq_dbg!(exception.value.0, Some("unauthorized".to_string().into()));
    assert_eq_dbg!(exception.ty.0, Some("ValueError".to_string()));

    let exception = Annotated::new(Exception {
        value: Annotated::new("ValueError:unauthorized".to_string().into()),
        ..Default::default()
    });

    let exception = exception.process(&mut processor).0.unwrap();
    assert_eq_dbg!(exception.value.0, Some("unauthorized".to_string().into()));
    assert_eq_dbg!(exception.ty.0, Some("ValueError".to_string()));
}

#[test]
fn test_json_value() {
    let mut processor = StoreNormalizeProcessor::default();

    let exception = Annotated::new(Exception {
        value: Annotated::new(r#"{"unauthorized":true}"#.to_string().into()),
        ..Default::default()
    });
    let exception = exception.process(&mut processor).0.unwrap();

    // Don't split a json-serialized value on the colon
    assert_eq_dbg!(
        exception.value.0,
        Some(r#"{"unauthorized":true}"#.to_string().into())
    );
    assert_eq_dbg!(exception.ty.0, None);
}

#[test]
fn test_exception_invalid() {
    let mut processor = StoreNormalizeProcessor::default();

    let exception = Annotated::new(Exception::default());
    let exception = exception.process(&mut processor);

    assert_eq_dbg!(
        exception.1.iter_errors().collect_tuple(),
        Some(("type or value required",))
    );
}

#[test]
fn test_is_url() {
    assert!(is_url("http://example.org/"));
    assert!(is_url("https://example.org/"));
    assert!(is_url("file:///tmp/filename"));
    assert!(is_url(
        "applewebdata://00000000-0000-1000-8080-808080808080"
    ));
    assert!(!is_url("app:///index.bundle")); // react native
    assert!(!is_url("webpack:///./app/index.jsx")); // webpack bundle
    assert!(!is_url("data:,"));
    assert!(!is_url("blob:\x00"));
}

#[test]
fn test_coerces_url_filenames() {
    let mut frame = Annotated::new(Frame {
        line: Annotated::new(1),
        filename: Annotated::new("http://foo.com/foo.js".to_string()),
        ..Default::default()
    });

    process_non_raw_frame(&mut frame);
    let frame = frame.0.unwrap();

    assert_eq!(frame.filename.0, Some("/foo.js".to_string()));
    assert_eq!(frame.abs_path.0, Some("http://foo.com/foo.js".to_string()));
}

#[test]
fn test_does_not_overwrite_filename() {
    let mut frame = Annotated::new(Frame {
        line: Annotated::new(1),
        filename: Annotated::new("foo.js".to_string()),
        abs_path: Annotated::new("http://foo.com/foo.js".to_string()),
        ..Default::default()
    });

    process_non_raw_frame(&mut frame);
    let frame = frame.0.unwrap();

    assert_eq!(frame.filename.0, Some("foo.js".to_string()));
    assert_eq!(frame.abs_path.0, Some("http://foo.com/foo.js".to_string()));
}

#[test]
fn test_ignores_results_with_empty_path() {
    let mut frame = Annotated::new(Frame {
        line: Annotated::new(1),
        abs_path: Annotated::new("http://foo.com".to_string()),
        ..Default::default()
    });

    process_non_raw_frame(&mut frame);
    let frame = frame.0.unwrap();

    assert_eq!(frame.filename.0, Some("http://foo.com".to_string()));
    assert_eq!(frame.abs_path.0, frame.filename.0);
}

#[test]
fn test_geo_from_ip_address() {
    let mut processor = StoreNormalizeProcessor {
        geoip_path: Some(PathBuf::from("GeoLiteCity.dat")),
        ..Default::default()
    };

    let user = Annotated::new(User {
        ip_address: Annotated::new(IpAddr("213.47.147.207".to_string())),
        ..Default::default()
    });

    let user = user.process(&mut processor);

    let expected = Annotated::new(Geo {
        country_code: Annotated::new("AT".to_string()),
        city: Annotated::new("Vienna".to_string()),
        region: Annotated::new("09".to_string()),
        ..Default::default()
    });
    assert_eq_dbg!(user.0.unwrap().geo, expected)
}

#[test]
fn test_invalid_email() {
    let mut processor = StoreNormalizeProcessor::default();

    let user = Annotated::new(User {
        email: Annotated::new("bananabread".to_string()),
        ..Default::default()
    });

    let user = user.process(&mut processor);

    assert_eq_dbg!(
        user,
        Annotated::new(User {
            email: Annotated::from_error(
                "invalid email address",
                Some(Value::String("bananabread".to_string()))
            ),
            ..Default::default()
        })
    );
}
