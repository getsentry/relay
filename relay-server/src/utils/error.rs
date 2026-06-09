use std::error::Error;

/// Find an error source with the given predicate.
///
/// Will become redundant once [`Error::sources`](https://doc.rust-lang.org/std/error/trait.Error.html#method.sources)
/// is stable.
pub fn find_error_source<E, P>(error: &E, predicate: P) -> Option<&dyn Error>
where
    E: Error + ?Sized,
    P: Fn(&(dyn Error + 'static)) -> bool,
{
    let mut source = error.source();
    while let Some(error) = source {
        if predicate(error) {
            return Some(error);
        }
        source = error.source();
    }
    None
}

/// Returns whether an error was raised by [`tower_http::limit::RequestBodyLimit`].
pub fn is_length_limit_error(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<http_body_util::LengthLimitError>()
        .is_some()
}
