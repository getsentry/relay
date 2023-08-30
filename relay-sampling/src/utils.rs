/// Returns `true` if this value is equal to `Default::default()`.
pub fn is_default<T: Default + PartialEq>(t: &T) -> bool {
    *t == T::default()
}
