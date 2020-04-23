include!(concat!(env!("OUT_DIR"), "/constants.gen.rs"));

/// Name of the event attachment.
///
/// This is a special attachment that can contain a sentry event payload encoded as message pack.
pub const ITEM_NAME_EVENT: &str = "__sentry-event";

/// Name of the breadcrumb attachment (1).
///
/// This is a special attachment that can contain breadcrumbs encoded as message pack. There can be
/// two attachments that the SDK may use as swappable buffers. Both attachments will be merged and
/// truncated to the maxmimum number of allowed attachments.
pub const ITEM_NAME_BREADCRUMBS1: &str = "__sentry-breadcrumb1";

/// Name of the breadcrumb attachment (2).
///
/// This is a special attachment that can contain breadcrumbs encoded as message pack. There can be
/// two attachments that the SDK may use as swappable buffers. Both attachments will be merged and
/// truncated to the maxmimum number of allowed attachments.
pub const ITEM_NAME_BREADCRUMBS2: &str = "__sentry-breadcrumb2";

/// Envelope header used to store the UE4 user id.
pub const UNREAL_USER_HEADER: &str = "unreal_user_id";

/// The default retention for events if the server does not specify a value in project
/// configurations.
pub const DEFAULT_EVENT_RETENTION: u16 = 90;

/// The maximum age of an ingested session in days. Session updates for sessions older than this
/// will be discarded.
#[cfg_attr(not(feature = "processing"), allow(dead_code))]
pub const MAX_SESSION_DAYS: u8 = 5;
