//! Code for scrubbing resource span description.
use std::collections::BTreeSet;

use once_cell::sync::Lazy;

// TODO: move functions from parent module here.

/// Parts of a resource path that we allowlist.
///
/// By default, all path segments except the last are dropped.
pub static COMMON_PATH_SEGMENTS: Lazy<BTreeSet<&str>> = Lazy::new(|| {
    BTreeSet::from([
        "_app",
        "_next",
        "_nuxt",
        "_shared",
        ".vite",
        "active_storage",
        "ajax",
        "assets",
        "avatar",
        "build",
        "cdn",
        "chunks",
        "coins",
        "data",
        "deps",
        "dist",
        "dms",
        "files",
        "icons",
        "image",
        "images",
        "img",
        "immutable",
        "js",
        "lib",
        "libs",
        "media",
        "node_modules",
        "products",
        "profile_images",
        "rails",
        "redirect",
        "releases",
        "representations",
        "shop",
        "static",
        "svg",
        "twemoji",
        "vi",
        "video",
        "webfonts",
        "webpack",
        "wl-image",
        "wp-includes",
    ])
});
