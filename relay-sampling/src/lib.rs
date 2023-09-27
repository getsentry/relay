//! Sampling logic for performing sampling decisions of incoming events.
//!
//! In order to allow Sentry to offer performance at scale, Relay extracts key `metrics` from all
//! transactions, but only forwards a random sample of raw transaction payloads to the upstream.
//! What exact percentage is sampled is determined by `dynamic sampling rules`, and depends on the
//! project, the environment, the transaction name, etc.
//!
//! In order to determine the sample rate, Relay uses a [`SamplingConfig`] which contains a set of
//! [`SamplingRule`](crate::config::SamplingRule)s that are matched against incoming data.
//!
//! # Types of Sampling
//!
//! There are two main types of dynamic sampling:
//!
//! 1. **Trace sampling** ensures that either all transactions of a trace are sampled or none. Rules
//!    have access to information in the [`DynamicSamplingContext`].
//! 2. **Transaction sampling** does not guarantee complete traces and instead applies to individual
//!   transactions. Rules have access to the full data in transaction events.
//!
//! # Components
//!
//! The sampling system implemented in Relay is composed of the following components:
//!
//! - [`DynamicSamplingContext`]: a container for information associated with the trace.
//! - [`SamplingRule`](crate::config::SamplingRule): a rule that is matched against incoming data.
//!   It specifies a condition that acts as predicate on the incoming payload.
//! - [`SamplingMatch`](crate::evaluation::SamplingMatch): the result of the matching of one or more
//!   rules.
//!
//! # How It Works
//!
//! - The incoming data is received by Relay.
//! - Relay resolves the [`SamplingConfig`] of the project to which the incoming data belongs.
//!   Additionally, it tries to resolve the configuration of the project that started the trace, the
//!   so-called "root project".
//! - The two [`SamplingConfig`]s are merged together and dynamic sampling evaluates a result. The
//!   algorithm goes over each rule and compute either a factor or sample rate based on the
//!   value of the rule.
//! - The [`SamplingMatch`](crate::evaluation::SamplingMatch) is finally returned containing the
//!   final `sample_rate` and some additional data that will be used in `relay_server` to perform
//!   the sampling decision.
//!
//! # Determinism
//!
//! The concept of determinism is a core concept for dynamic sampling. Deterministic sampling
//! decisions allow to:
//!
//! - Run dynamic sampling repeatedly over a **chain of Relays** (e.g., we don't want to drop an
//! event that was retained by a previous Relay and vice-versa).
//! - Run dynamic sampling independently on **transactions of the same trace** (e.g., we want to be
//! able to sample all the transactions of the same trace, even though some exceptions apply).
//!
//! In order to perform deterministic sampling, Relay uses teh trace ID or event ID as the seed for
//! the random number generator.
//!
//! # Examples
//!
//! ## [`SamplingConfig`]
//!
//! ```json
#![doc = include_str!("../tests/fixtures/sampling_config.json")]
//! ```
//!
//! ## [`DynamicSamplingContext`]
//!
//! ```json
#![doc = include_str!("../tests/fixtures/dynamic_sampling_context.json")]
//! ```

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png",
    html_favicon_url = "https://raw.githubusercontent.com/getsentry/relay/master/artwork/relay-icon.png"
)]
#![warn(missing_docs)]

pub mod condition;
pub mod config;
pub mod dsc;
pub mod evaluation;
mod utils;

pub use config::SamplingConfig;
pub use dsc::DynamicSamplingContext;
