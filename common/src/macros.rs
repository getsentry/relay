/// Helper macro to implement string based serialization.
///
/// If a type implements `FromStr` and `Display` then this automatically
/// implements a serializer/deserializer for that type that dispatches
/// appropriately.  First argument is the name of the type, the second
/// is a message for the expectation error (human readable type effectively).
#[macro_export]
macro_rules! impl_str_serialization {
    ($type:ty, $expectation:expr) => {
        impl ::serde::ser::Serialize for $type {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: ::serde::ser::Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

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

/// An alternative to a `move` closure.
///
/// When one needs to use a clojure with move semantics one often needs to clone and
/// move some of the free variables. This macro automates the process of cloning and moving
/// variables.
///
/// The following code:
/// ```compile_fail
/// let arg1 = v1.clone()
/// let arg2 = v2.clone()
///
/// let result = some_function( move || f(arg1, arg2)})
/// ```
/// Can be rewritten in a cleaner way by using the `clone!` macro like so:
///
/// ```compile_fail
/// let result = some_function( clone! { v1, v2, || f(v1,v2)})
/// ```
#[macro_export]
macro_rules! clone {
    (@param _) => ( _ );
    (@param $x:ident) => ( $x );
    ($($n:ident),+ , || $body:expr) => (
        {
            $( let $n = $n.clone(); )+
            move || $body
        }
    );
    ($($n:ident),+ , |$($p:tt),+| $body:expr) => (
        {
            $( let $n = $n.clone(); )+
            move |$(clone!(@param $p),)+| $body
        }
    );
}
