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

/// Invalid environment strings (case sensitive).
pub const INVALID_ENVIRONMENTS: &[&str] = &["none"];

/// Invalid release version strings (case insensitive).
pub const INVALID_RELEASES: &[&str] = &["latest", "..", "."];
