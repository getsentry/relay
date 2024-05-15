use itertools::Either;

/// Splits off items from a vector matching a predicate.
pub fn split_off<T>(data: Vec<T>, mut f: impl FnMut(&T) -> bool) -> (Vec<T>, Vec<T>) {
    split_off_map(data, |item| {
        if f(&item) {
            Either::Left(item)
        } else {
            Either::Right(item)
        }
    })
}

/// Splits off items from a vector matching a predicate and mapping the removed items.
pub fn split_off_map<T, S>(data: Vec<T>, mut f: impl FnMut(T) -> Either<T, S>) -> (Vec<T>, Vec<S>) {
    let mut split = Vec::new();

    let data = data
        .into_iter()
        .filter_map(|item| match f(item) {
            Either::Left(item) => Some(item),
            Either::Right(p) => {
                split.push(p);
                None
            }
        })
        .collect();

    (data, split)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_off() {
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
