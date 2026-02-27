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
    while let Some(s) = source {
        if predicate(s) {
            return Some(s);
        }
        source = s.source();
    }
    None
}
