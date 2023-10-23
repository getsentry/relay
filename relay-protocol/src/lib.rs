//! Types and traits for building JSON-based protocols and schemas
//!
//! This crate provides the types and aliases that are used for the meta part of the protocol. This
//! is the core annotation system as well as the dynamic value parts and the metadata that goes with
//! it.
//!
//! # Test Utilities
//!
//! When the `test` feature is enabled, this crate exposes the additional
//! `assert_annotated_snapshot` macro. This can be used with `insta` to render and compare snapshots
//! of annotated data with meta data.

#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]

pub mod condition;

mod annotated;
mod impls;
mod macros;
mod meta;
mod size;
mod traits;
mod value;

pub use self::annotated::*;
pub use self::condition::RuleCondition;
pub use self::impls::*;
pub use self::macros::*;
pub use self::meta::*;
pub use self::size::*;
pub use self::traits::*;
pub use self::value::*;

#[cfg(feature = "derive")]
pub use relay_protocol_derive::{Empty, FromValue, IntoValue};
