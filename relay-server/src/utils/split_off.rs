use itertools::Either;

/// Splits off items from a vector matching a predicate.
pub fn split_off<T>(data: Vec<T>, mut f: impl FnMut(&T) -> bool) -> (Vec<T>, Vec<T>) {
    split_off_map(data, |item| {
        if f(&item) {
            Either::Right(item)
        } else {
            Either::Left(item)
        }
    })
}

/// Splits off items from a vector matching a predicate and mapping the removed items.
pub fn split_off_map<T, S>(data: Vec<T>, mut f: impl FnMut(T) -> Either<T, S>) -> (Vec<T>, Vec<S>) {
    let mut right = Vec::new();

    let left = data
        .into_iter()
        .filter_map(|item| match f(item) {
            Either::Left(item) => Some(item),
            Either::Right(p) => {
                right.push(p);
                None
            }
        })
        .collect();

    (left, right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_off() {
        let data = vec!["apple", "apple", "orange"];

        let (apples, oranges) = split_off(data, |item| *item == "orange");
        assert_eq!(apples, vec!["apple", "apple"]);
        assert_eq!(oranges, vec!["orange"]);
    }

    #[test]
    fn test_split_off_map() {
        let data = vec!["apple", "apple", "orange"];

        let (apples, oranges) = split_off_map(data, |item| {
            if item != "orange" {
                Either::Left(item)
            } else {
                Either::Right(item)
            }
        });
        assert_eq!(apples, vec!["apple", "apple"]);
        assert_eq!(oranges, vec!["orange"]);
    }
}
