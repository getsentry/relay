use std::fmt;

/// The type of an event attachment.
///
/// These item types must align with the Sentry processing pipeline.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AttachmentType {
    /// A regular attachment without special meaning.
    Attachment,

    /// A minidump crash report (binary data).
    Minidump,

    /// An apple crash report (text data).
    AppleCrashReport,

    /// A msgpack-encoded event payload submitted as part of multipart uploads.
    ///
    /// This attachment is processed by Relay immediately and never forwarded or persisted.
    EventPayload,

    /// A msgpack-encoded list of payloads.
    ///
    /// There can be two attachments that the SDK may use as swappable buffers. Both attachments
    /// will be merged and truncated to the maxmimum number of allowed attachments.
    ///
    /// This attachment is processed by Relay immediately and never forwarded or persisted.
    Breadcrumbs,

    // A prosperodump crash report (binary data)
    Prosperodump,

    /// This is a binary attachment present in Unreal 4 events containing event context information.
    ///
    /// This can be deserialized using the `symbolic` crate see
    /// [`symbolic_unreal::Unreal4Context`].
    ///
    /// [`symbolic_unreal::Unreal4Context`]: https://docs.rs/symbolic/*/symbolic/unreal/struct.Unreal4Context.html
    UnrealContext,

    /// This is a binary attachment present in Unreal 4 events containing event Logs.
    ///
    /// This can be deserialized using the `symbolic` crate see
    /// [`symbolic_unreal::Unreal4LogEntry`].
    ///
    /// [`symbolic_unreal::Unreal4LogEntry`]: https://docs.rs/symbolic/*/symbolic/unreal/struct.Unreal4LogEntry.html
    UnrealLogs,

    /// An application UI view hierarchy (json payload).
    ViewHierarchy,

    /// Unknown attachment type, forwarded for compatibility.
    /// Attachments with this type will be dropped if `accept_unknown_items` is set to false.
    Unknown(String),
}

impl Default for AttachmentType {
    fn default() -> Self {
        Self::Attachment
    }
}

impl fmt::Display for AttachmentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AttachmentType::Attachment => write!(f, "event.attachment"),
            AttachmentType::Minidump => write!(f, "event.minidump"),
            AttachmentType::AppleCrashReport => write!(f, "event.applecrashreport"),
            AttachmentType::EventPayload => write!(f, "event.payload"),
            AttachmentType::Prosperodump => write!(f, "playstation.prosperodump"),
            AttachmentType::Breadcrumbs => write!(f, "event.breadcrumbs"),
            AttachmentType::UnrealContext => write!(f, "unreal.context"),
            AttachmentType::UnrealLogs => write!(f, "unreal.logs"),
            AttachmentType::ViewHierarchy => write!(f, "event.view_hierarchy"),
            AttachmentType::Unknown(s) => s.fmt(f),
        }
    }
}

impl std::str::FromStr for AttachmentType {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "event.attachment" => AttachmentType::Attachment,
            "event.minidump" => AttachmentType::Minidump,
            "event.applecrashreport" => AttachmentType::AppleCrashReport,
            "event.payload" => AttachmentType::EventPayload,
            "playstation.prosperodump" => AttachmentType::Prosperodump,
            "event.breadcrumbs" => AttachmentType::Breadcrumbs,
            "event.view_hierarchy" => AttachmentType::ViewHierarchy,
            "unreal.context" => AttachmentType::UnrealContext,
            "unreal.logs" => AttachmentType::UnrealLogs,
            other => AttachmentType::Unknown(other.to_owned()),
        })
    }
}

relay_common::impl_str_serde!(
    AttachmentType,
    "an attachment type (see sentry develop docs)"
);
