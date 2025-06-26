use serde::{Deserialize, de};

/// Deserializes at least one element.
///
/// Singular elements can be specified without a list.
pub fn one_or_many<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: de::Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        Single(T),
        Many(Vec<T>),
    }

    let r = match OneOrMany::<T>::deserialize(deserializer)? {
        OneOrMany::Single(single) => vec![single],
        OneOrMany::Many(many) => many,
    };

    if r.is_empty() {
        return Err(de::Error::custom("expected at least one element"));
    }
    Ok(r)
}
