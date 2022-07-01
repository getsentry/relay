/// Helper macro to implement string based serialization.
///
/// If a type implements `Display` then this automatically
/// implements a serializer for that type that dispatches
/// appropriately.
#[macro_export]
macro_rules! impl_str_ser {
    ($type:ty) => {
        impl ::serde::ser::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::ser::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }
    };
}

pub use impl_str_ser;

/// Helper macro to implement string based deserialization.
///
/// If a type implements `FromStr` then this automatically
/// implements a deserializer for that type that dispatches
/// appropriately.
#[macro_export]
macro_rules! impl_str_de {
    ($type:ty, $expectation:expr) => {
        impl<'de> ::serde::de::Deserialize<'de> for $type {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: ::serde::de::Deserializer<'de>,
            {
                struct V;

                impl<'de> ::serde::de::Visitor<'de> for V {
                    type Value = $type;

                    fn expecting(&self, formatter: &mut ::std::fmt::Formatter<'_>) -> fmt::Result {
                        formatter.write_str($expectation)
                    }

                    fn visit_str<E>(self, value: &str) -> Result<$type, E>
                    where
                        E: ::serde::de::Error,
                    {
                        value.parse().map_err(|_| {
                            ::serde::de::Error::invalid_value(
                                ::serde::de::Unexpected::Str(value),
                                &self,
                            )
                        })
                    }
                }

                deserializer.deserialize_str(V)
            }
        }
    };
}

pub use impl_str_de;

/// Helper macro to implement string based serialization and deserialization.
///
/// If a type implements `FromStr` and `Display` then this automatically
/// implements a serializer/deserializer for that type that dispatches
/// appropriately.
#[macro_export]
macro_rules! impl_str_serde {
    ($type:ty, $expectation:expr) => {
        $crate::impl_str_ser!($type);
        $crate::impl_str_de!($type, $expectation);
    };
}

pub use impl_str_serde;

/// Same as `try` but to be used in functions that return `Box<Future>` instead of `Result`.
///
/// Useful when calling synchronous (but cheap enough) functions in async code.
#[macro_export]
macro_rules! tryf {
    ($e:expr) => {
        match $e {
            Ok(value) => value,
            Err(e) => return Box::new(::futures::future::err(::std::convert::From::from(e))),
        }
    };
}

pub use tryf;

/// A cloning alternative to a `move` closure.
///
/// When one needs to use a closure with move semantics one often needs to clone and move some of
/// the free variables. This macro automates the process of cloning and moving variables.
///
/// The following code:
///
/// ```
/// # use std::sync::{Arc, Mutex};
/// let shared = Arc::new(Mutex::new(0));
///
/// let cloned = shared.clone();
/// std::thread::spawn(move || {
///     *cloned.lock().unwrap() = 42
/// }).join();
///
/// assert_eq!(*shared.lock().unwrap(), 42);
/// ```
///
/// Can be rewritten in a cleaner way by using the `clone!` macro like so:
///
/// ```
/// # use std::sync::{Arc, Mutex};
/// use relay_common::clone;
///
/// let shared = Arc::new(Mutex::new(0));
/// std::thread::spawn(clone!(shared, || {
///     *shared.lock().unwrap() = 42
/// })).join();
///
/// assert_eq!(*shared.lock().unwrap(), 42);
/// ```
#[macro_export]
macro_rules! clone {
    ($($n:ident ,)+ || $body:expr) => {{
        $( let $n = $n.clone(); )+
        move || $body
    }};
    ($($n:ident ,)+ |$($p:pat),+| $body:expr) => {{
        $( let $n = $n.clone(); )+
        move |$($p),+| $body
    }};
}

pub use clone;
