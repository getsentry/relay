use std::mem;

use relay_event_schema::processor;
use relay_event_schema::protocol::{Frame, RawStacktrace};
use relay_protocol::{Annotated, Empty, Meta};
use url::Url;

fn is_url(filename: &str) -> bool {
    filename.starts_with("file:")
        || filename.starts_with("http:")
        || filename.starts_with("https:")
        || filename.starts_with("applewebdata:")
}

pub fn normalize_stacktrace(stacktrace: &mut RawStacktrace, _meta: &mut Meta) {
    // This processing is only done for non raw frames (i.e. not for exception.raw_stacktrace).
    if let Some(frames) = stacktrace.frames.value_mut() {
        for frame in frames.iter_mut() {
            normalize_non_raw_frame(frame);
        }
    }
}

pub fn normalize_non_raw_frame(frame: &mut Annotated<Frame>) {
    let _ = processor::apply(frame, |frame, _meta| {
        if frame.abs_path.value().is_empty() {
            frame.abs_path = mem::replace(&mut frame.filename, Annotated::empty());
        }

        if frame.filename.value().is_empty() {
            if let Some(abs_path) = frame.abs_path.value_mut() {
                frame.filename = Annotated::new(abs_path.clone());

                if is_url(abs_path.as_str()) {
                    if let Ok(url) = Url::parse(abs_path.as_str()) {
                        let path = url.path();

                        if !path.is_empty() && path != "/" {
                            frame.filename = Annotated::new(path.into());
                        }
                    }
                }
            }
        }

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

        Ok(())
    });
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_coerces_url_filenames() {
        let mut frame = Annotated::new(Frame {
            lineno: Annotated::new(1),
            filename: Annotated::new("http://foo.com/foo.js".into()),
            ..Default::default()
        });

        normalize_non_raw_frame(&mut frame);
        let frame = frame.value().unwrap();

        assert_eq!(frame.filename.value().unwrap().as_str(), "/foo.js");
        assert_eq!(
            frame.abs_path.value().unwrap().as_str(),
            "http://foo.com/foo.js"
        );
    }

    #[test]
    fn test_does_not_overwrite_filename() {
        let mut frame = Annotated::new(Frame {
            lineno: Annotated::new(1),
            filename: Annotated::new("foo.js".into()),
            abs_path: Annotated::new("http://foo.com/foo.js".into()),
            ..Default::default()
        });

        normalize_non_raw_frame(&mut frame);
        let frame = frame.value().unwrap();

        assert_eq!(frame.filename.value().unwrap().as_str(), "foo.js");
        assert_eq!(
            frame.abs_path.value().unwrap().as_str(),
            "http://foo.com/foo.js"
        );
    }

    #[test]
    fn test_ignores_results_with_empty_path() {
        let mut frame = Annotated::new(Frame {
            lineno: Annotated::new(1),
            abs_path: Annotated::new("http://foo.com".into()),
            ..Default::default()
        });

        normalize_non_raw_frame(&mut frame);
        let frame = frame.value().unwrap();

        assert_eq!(frame.filename.value().unwrap().as_str(), "http://foo.com");
        assert_eq!(
            frame.abs_path.value().unwrap().as_str(),
            frame.filename.value().unwrap().as_str()
        );
    }

    #[test]
    fn test_ignores_results_with_slash_path() {
        let mut frame = Annotated::new(Frame {
            lineno: Annotated::new(1),
            abs_path: Annotated::new("http://foo.com/".into()),
            ..Default::default()
        });

        normalize_non_raw_frame(&mut frame);
        let frame = frame.value().unwrap();

        assert_eq!(frame.filename.value().unwrap().as_str(), "http://foo.com/");
        assert_eq!(
            frame.abs_path.value().unwrap().as_str(),
            frame.filename.value().unwrap().as_str()
        );
    }

    #[test]
    fn test_coerce_empty_filename() {
        let mut frame = Annotated::new(Frame {
            lineno: Annotated::new(1),
            filename: Annotated::new("".into()),
            abs_path: Annotated::new("http://foo.com/foo.js".into()),
            ..Default::default()
        });

        normalize_non_raw_frame(&mut frame);
        let frame = frame.value().unwrap();

        assert_eq!(frame.filename.value().unwrap().as_str(), "/foo.js");
        assert_eq!(
            frame.abs_path.value().unwrap().as_str(),
            "http://foo.com/foo.js"
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
}
