/// A list of all valid [`platform`](super::Event::platform) identifiers.
pub const VALID_PLATFORMS: &[&str] = &[
    "as3",
    "c",
    "cfml",
    "cocoa",
    "csharp",
    "elixir",
    "go",
    "groovy",
    "haskell",
    "java",
    "javascript",
    "native",
    "node",
    "objc",
    "other",
    "perl",
    "php",
    "python",
    "ruby",
];

/// The latest version of the protocol.
pub const PROTOCOL_VERSION: u16 = 7;
