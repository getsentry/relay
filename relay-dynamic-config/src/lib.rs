//! Protocol for dynamic configuration passed down to Relay from Sentry.
//!
//! In contrast to static configuration files, parts of Relay's configuration is generated by
//! Sentry and polled by Relay in regular intervals.
//! Dynamic configuration includes (but is not limited to)
//!
//! 1. rate limits and quotas,
//! 1. feature flags,
//! 1. settings that the user configured in Sentry's UI.
//!
//! # Project Configuration
//!
//! So far, the only scope of dynamic configuration is per [`relay_auth::PublicKey`] a.k.a. [DSN](https://docs.sentry.io/product/sentry-basics/dsn-explainer/).
//! The schema for this configuration is defined in [`ProjectConfig`].
//!
//! ## Example Config
//!
//! ```json
//! {
//!     "organizationId": 1,
//!     "config": {
//!         "excludeFields": [],
//!         "filterSettings": {},
//!         "scrubIpAddresses": False,
//!         "sensitiveFields": [],
//!         "scrubDefaults": True,
//!         "scrubData": True,
//!         "groupingConfig": {
//!             "id": "legacy:2019-03-12",
//!             "enhancements": "eJybzDhxY05qemJypZWRgaGlroGxrqHRBABbEwcC",
//!         },
//!         "blacklistedIps": ["127.43.33.22"],
//!         "trustedRelays": [],
//!     },
//! }
//! ```
//!
//! # Global configuration
//!
//! This is the configuration for all Relays, independently from what per-project configuration is, and is defined in [`GlobalConfig`].
//!
//! ## Example Config
//!
//! ```json
//! {
//!     "measurements": {
//!        "builtinMeasurements": [
//!            {
//!                "name": "app_start_cold",
//!                "unit": "millisecond"
//!            }
//!        ],
//!        "maxCustomMeasurements": 1
//!     }
//! }
//!
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![allow(clippy::derive_partial_eq_without_eq)]

mod defaults;
mod error_boundary;
mod feature;
mod global;
mod metrics;
mod project;
mod trusted_relay;
mod utils;

pub use error_boundary::*;
pub use feature::*;
pub use global::*;
pub use metrics::*;
pub use project::*;
pub use trusted_relay::*;
pub use utils::*;
