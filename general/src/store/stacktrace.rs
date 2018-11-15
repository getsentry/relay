use std::mem;

use url::Url;

use crate::protocol::{Frame, Stacktrace};
use crate::types::Annotated;

fn is_url(filename: &str) -> bool {
    filename.starts_with("file:")
        || filename.starts_with("http:")
        || filename.starts_with("https:")
        || filename.starts_with("applewebdata:")
}

pub fn process_non_raw_stacktrace(stacktrace: &mut Annotated<Stacktrace>) {
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

pub fn process_non_raw_frame(frame: &mut Annotated<Frame>) {
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

pub fn enforce_frame_hard_limit(stacktrace: &mut Annotated<Stacktrace>, limit: usize) {
    if let Some(ref mut stacktrace) = stacktrace.0 {
        if let Some(ref mut frames) = stacktrace.frames.0 {
            // Trim down the frame list to a hard limit. Leave the last frame in place in case
            // it's useful for debugging.
            if frames.len() >= limit {
                let last_elem = frames.pop();
                frames.truncate(limit - 1);
                if let Some(last_elem) = last_elem {
                    frames.push(last_elem);
                }
            }
        }
    }
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
fn test_frame_hard_limit() {
    fn create_frame(filename: &str) -> Annotated<Frame> {
        Annotated::new(Frame {
            filename: Annotated::new(filename.to_string()),
            ..Default::default()
        })
    }

    let mut stacktrace = Annotated::new(Stacktrace {
        frames: Annotated::new(vec![
            create_frame("foo1.py"),
            create_frame("foo2.py"),
            create_frame("foo3.py"),
            create_frame("foo4.py"),
            create_frame("foo5.py"),
        ]),
        ..Default::default()
    });

    enforce_frame_hard_limit(&mut stacktrace, 3);

    assert_eq_dbg!(
        stacktrace,
        Annotated::new(Stacktrace {
            frames: Annotated::new(vec![
                create_frame("foo1.py"),
                create_frame("foo2.py"),
                create_frame("foo5.py"),
            ]),
            ..Default::default()
        })
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
