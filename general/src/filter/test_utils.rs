//! Utilities used by the event filter tests.

use crate::filter::config::FilterConfig;

/// Create a FilterConfig with the specified enabled state.
pub(super) fn get_f_config(is_enabled: bool) -> FilterConfig {
    FilterConfig { is_enabled }
}
