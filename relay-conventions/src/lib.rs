mod info;

pub use info::WriteBehavior;
use info::{AttributeInfo, AttributeType, Pii};

include!(concat!(env!("OUT_DIR"), "/attribute_map.rs"));

/// Returns information about which names an attribute should be saved under.
pub fn write_behavior(key: &str) -> Option<WriteBehavior> {
    let attr = ATTRIBUTES.get(key)?;
    Some(attr.write_behavior)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_response_content_length() {
        let info = ATTRIBUTES.get("http.response_content_length").unwrap();

        insta::assert_debug_snapshot!(info, @r###"
        AttributeInfo {
            write_behavior: BothNames(
                "http.response.body.size",
            ),
            pii: False,
            ty: Integer,
            aliases: [
                "http.response.body.size",
                "http.response.header.content-length",
                "http.response.header['content-length']",
            ],
        }
        "###);
    }
}
