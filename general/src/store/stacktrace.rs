use std::mem;

use url::Url;

use crate::processor::apply_value;
use crate::protocol::{Frame, Stacktrace};
use crate::types::{Annotated, Array, Meta};

fn is_url(filename: &str) -> bool {
    filename.starts_with("file:")
        || filename.starts_with("http:")
        || filename.starts_with("https:")
        || filename.starts_with("applewebdata:")
}

pub fn process_non_raw_stacktrace(stacktrace: &mut Stacktrace, _meta: &mut Meta) {
    // XXX: this processing should only be done for non raw frames (i.e. not for
    // exception.raw_stacktrace)
    if let Some(frames) = stacktrace.frames.value_mut() {
        for frame in frames.iter_mut() {
            apply_value(frame, process_non_raw_frame);
        }
    }
}

pub fn process_non_raw_frame(frame: &mut Frame, _meta: &mut Meta) {
    if frame.abs_path.value().is_none() {
        frame.abs_path = mem::replace(&mut frame.filename, Annotated::empty());
    }

    if frame.filename.value().is_none() {
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

pub fn enforce_frame_hard_limit(frames: &mut Array<Frame>, meta: &mut Meta, limit: usize) {
    // Trim down the frame list to a hard limit. Leave the last frame in place in case
    // it's useful for debugging.
    let original_length = frames.len();
    if original_length >= limit {
        meta.set_original_length(Some(original_length));

        let last_frame = frames.pop();
        frames.truncate(limit - 1);
        if let Some(last_frame) = last_frame {
            frames.push(last_frame);
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

    apply_value(&mut frame, process_non_raw_frame);
    let frame = frame.value().unwrap();

    assert_eq!(frame.filename.as_str(), Some("/foo.js"));
    assert_eq!(frame.abs_path.as_str(), Some("http://foo.com/foo.js"));
}

#[test]
fn test_does_not_overwrite_filename() {
    let mut frame = Annotated::new(Frame {
        line: Annotated::new(1),
        filename: Annotated::new("foo.js".to_string()),
        abs_path: Annotated::new("http://foo.com/foo.js".to_string()),
        ..Default::default()
    });

    apply_value(&mut frame, process_non_raw_frame);
    let frame = frame.value().unwrap();

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

    apply_value(&mut frame, process_non_raw_frame);
    let frame = frame.value().unwrap();

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

    let mut frames = Annotated::new(vec![
        create_frame("foo1.py"),
        create_frame("foo2.py"),
        create_frame("foo3.py"),
        create_frame("foo4.py"),
        create_frame("foo5.py"),
    ]);

    apply_value(&mut frames, |f, m| enforce_frame_hard_limit(f, m, 3));

    let mut expected_meta = Meta::default();
    expected_meta.set_original_length(Some(5));

    assert_eq_dbg!(
        frames,
        Annotated(
            Some(vec![
                create_frame("foo1.py"),
                create_frame("foo2.py"),
                create_frame("foo5.py"),
            ]),
            expected_meta
        )
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
