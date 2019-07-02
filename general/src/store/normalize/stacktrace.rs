use std::mem;

use url::Url;

use crate::protocol::{Frame, RawStacktrace};
use crate::types::{Annotated, Empty, Meta};

fn is_url(filename: &str) -> bool {
    filename.starts_with("file:")
        || filename.starts_with("http:")
        || filename.starts_with("https:")
        || filename.starts_with("applewebdata:")
}

pub fn process_stacktrace(stacktrace: &mut RawStacktrace, _meta: &mut Meta) {
    // This processing is only done for non raw frames (i.e. not for exception.raw_stacktrace).
    if let Some(frames) = stacktrace.frames.value_mut() {
        for frame in frames.iter_mut() {
            frame.apply(process_non_raw_frame);
        }
    }
}

pub fn process_non_raw_frame(frame: &mut Frame, _meta: &mut Meta) {
    if frame.abs_path.value().is_empty() {
        frame.abs_path = mem::replace(&mut frame.filename, Annotated::empty());
    }

    if frame.filename.value().is_empty() {
        if let Some(abs_path) = frame.abs_path.value_mut() {
            frame.filename = Annotated::new(abs_path.clone());

            if is_url(abs_path) {
                if let Ok(url) = Url::parse(abs_path) {
                    let path = url.path();

                    if !path.is_empty() && path != "/" {
                        frame.filename = Annotated::new(path.to_string());
                    }
                }
            }
        }
    }
}

#[test]
fn test_coerces_url_filenames() {
    let mut frame = Annotated::new(Frame {
        lineno: Annotated::new(1),
        filename: Annotated::new("http://foo.com/foo.js".to_string()),
        ..Default::default()
    });

    frame.apply(process_non_raw_frame);
    let frame = frame.value().unwrap();

    assert_eq!(frame.filename.as_str(), Some("/foo.js"));
    assert_eq!(frame.abs_path.as_str(), Some("http://foo.com/foo.js"));
}

#[test]
fn test_does_not_overwrite_filename() {
    let mut frame = Annotated::new(Frame {
        lineno: Annotated::new(1),
        filename: Annotated::new("foo.js".to_string()),
        abs_path: Annotated::new("http://foo.com/foo.js".to_string()),
        ..Default::default()
    });

    frame.apply(process_non_raw_frame);
    let frame = frame.value().unwrap();

    assert_eq!(frame.filename.as_str(), Some("foo.js"));
    assert_eq!(frame.abs_path.as_str(), Some("http://foo.com/foo.js"));
}

#[test]
fn test_ignores_results_with_empty_path() {
    let mut frame = Annotated::new(Frame {
        lineno: Annotated::new(1),
        abs_path: Annotated::new("http://foo.com".to_string()),
        ..Default::default()
    });

    frame.apply(process_non_raw_frame);
    let frame = frame.value().unwrap();

    assert_eq!(frame.filename.as_str(), Some("http://foo.com"));
    assert_eq!(frame.abs_path.as_str(), frame.filename.as_str());
}

#[test]
fn test_ignores_results_with_slash_path() {
    let mut frame = Annotated::new(Frame {
        lineno: Annotated::new(1),
        abs_path: Annotated::new("http://foo.com/".to_string()),
        ..Default::default()
    });

    frame.apply(process_non_raw_frame);
    let frame = frame.value().unwrap();

    assert_eq!(frame.filename.as_str(), Some("http://foo.com/"));
    assert_eq!(frame.abs_path.as_str(), frame.filename.as_str());
}

#[test]
fn test_coerce_empty_filename() {
    let mut frame = Annotated::new(Frame {
        lineno: Annotated::new(1),
        filename: Annotated::new("".to_string()),
        abs_path: Annotated::new("http://foo.com/foo.js".to_string()),
        ..Default::default()
    });

    frame.apply(process_non_raw_frame);
    let frame = frame.value().unwrap();

    assert_eq!(frame.filename.as_str(), Some("/foo.js"));
    assert_eq!(frame.abs_path.as_str(), Some("http://foo.com/foo.js"));
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
