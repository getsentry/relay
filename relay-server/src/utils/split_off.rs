/// Splits off items from a vector matching a predicate.
///
/// # Examples
///
/// ```
/// let data = vec!["apple", "apple", "orange"];
///
/// let (apples, oranges) = split_off(data, |item| (item != "orange").then_some(item));
/// assert_eq!(apples, vec!["apple", "apple"]);
/// assert_eq!(oranges, vec!["orange"]);
/// ```
pub fn split_off<T, S>(data: Vec<T>, mut f: impl FnMut(T) -> Option<S>) -> (Vec<T>, Vec<S>) {
    let mut split = Vec::new();

    let data = data
        .into_iter()
        .filter_map(|item| match f(item) {
            Some(p) => {
                split.push(p);
                None
            }
            None => Some(item),
        })
        .collect();

    (data, split)
}
