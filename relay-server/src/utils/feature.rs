use relay_config::{Config, RelayMode};
use relay_dynamic_config::Feature;

use crate::services::projects::project::ProjectInfo;

/// Function for on-off switches that filter specific item types (profiles, spans)
/// based on a feature flag.
///
/// If the project config did not come from the upstream, we keep the items.
pub fn should_filter(config: &Config, project_info: &ProjectInfo, feature: Feature) -> bool {
    match config.relay_mode() {
        RelayMode::Proxy => false,
        RelayMode::Managed => !project_info.has_feature(feature),
    }
}
