use std::mem;

use url::Url;

use crate::protocol::{Frame, RawStacktrace};
use crate::types::{Annotated, Array, Empty, Meta};

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

/// Remove excess metadata for middle frames which go beyond `frame_allowance`.
///
/// This is supposed to be equivalent to `slim_frame_data` in Sentry.
pub fn slim_frame_data(frames: &mut Array<Frame>, frame_allowance: usize) {
    let frames_len = frames.len();

    if frames_len <= frame_allowance {
        return;
    }

    // Avoid ownership issues by only storing indices
    let mut app_frame_indices = Vec::with_capacity(frames_len);
    let mut system_frame_indices = Vec::with_capacity(frames_len);

    for (i, frame) in frames.iter().enumerate() {
        if let Some(frame) = frame.value() {
            match frame.in_app.value() {
                Some(true) => app_frame_indices.push(i),
                _ => system_frame_indices.push(i),
            }
        }
    }

    let app_count = app_frame_indices.len();
    let system_allowance_half = frame_allowance.saturating_sub(app_count) / 2;
    let system_frames_to_remove = &system_frame_indices
        [system_allowance_half..system_frame_indices.len() - system_allowance_half];

    let remaining = frames_len
        .saturating_sub(frame_allowance)
        .saturating_sub(system_frames_to_remove.len());
    let app_allowance_half = app_count.saturating_sub(remaining) / 2;
    let app_frames_to_remove =
        &app_frame_indices[app_allowance_half..app_frame_indices.len() - app_allowance_half];

    // TODO: Which annotation to set?

    for i in system_frames_to_remove.iter().chain(app_frames_to_remove) {
        if let Some(frame) = frames.get_mut(*i) {
            if let Some(ref mut frame) = frame.value_mut().as_mut() {
                frame.vars = Annotated::empty();
                frame.pre_context = Annotated::empty();
                frame.post_context = Annotated::empty();
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

    frames.apply(|f, m| enforce_frame_hard_limit(f, m, 3));

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

#[test]
fn test_slim_frame_data_under_max() {
    let mut frames = vec![Annotated::new(Frame {
        filename: Annotated::new("foo".to_string()),
        pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
        context_line: Annotated::new("b".to_string()),
        post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
        ..Default::default()
    })];

    let old_frames = frames.clone();
    slim_frame_data(&mut frames, 4);

    assert_eq_dbg!(frames, old_frames);
}

#[test]
fn test_slim_frame_data_over_max() {
    let mut frames = vec![];

    for n in 0..5 {
        frames.push(Annotated::new(Frame {
            filename: Annotated::new(format!("foo {}", n)),
            pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
            context_line: Annotated::new("b".to_string()),
            post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
            ..Default::default()
        }));
    }

    slim_frame_data(&mut frames, 4);

    let expected = vec![
        Annotated::new(Frame {
            filename: Annotated::new("foo 0".to_string()),
            pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
            context_line: Annotated::new("b".to_string()),
            post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
            ..Default::default()
        }),
        Annotated::new(Frame {
            filename: Annotated::new("foo 1".to_string()),
            pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
            context_line: Annotated::new("b".to_string()),
            post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
            ..Default::default()
        }),
        Annotated::new(Frame {
            filename: Annotated::new("foo 2".to_string()),
            context_line: Annotated::new("b".to_string()),
            ..Default::default()
        }),
        Annotated::new(Frame {
            filename: Annotated::new("foo 3".to_string()),
            pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
            context_line: Annotated::new("b".to_string()),
            post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
            ..Default::default()
        }),
        Annotated::new(Frame {
            filename: Annotated::new("foo 4".to_string()),
            pre_context: Annotated::new(vec![Annotated::new("a".to_string())]),
            context_line: Annotated::new("b".to_string()),
            post_context: Annotated::new(vec![Annotated::new("c".to_string())]),
            ..Default::default()
        }),
    ];

    assert_eq_dbg!(frames, expected);
}
