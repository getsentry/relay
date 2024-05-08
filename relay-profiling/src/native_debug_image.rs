use relay_event_schema::protocol::{Addr, DebugId, NativeImagePath};
use serde::{Deserialize, Serialize};

use crate::utils;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
enum ImageType {
    MachO,
    Symbolic,
    Sourcemap,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NativeDebugImage {
    #[serde(alias = "name")]
    code_file: NativeImagePath,
    #[serde(alias = "id")]
    debug_id: DebugId,
    #[serde(rename = "type")]
    image_type: ImageType,

    #[serde(skip_serializing_if = "Option::is_none")]
    image_addr: Option<Addr>,

    #[serde(skip_serializing_if = "Option::is_none")]
    image_vmaddr: Option<Addr>,

    #[serde(
        default,
        deserialize_with = "utils::deserialize_number_from_string",
        skip_serializing_if = "utils::is_zero"
    )]
    image_size: u64,
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{Addr, DebugImage, NativeDebugImage as SchemaImage};
    use relay_protocol::{Annotated, Map};

    use super::NativeDebugImage;

    #[test]
    fn test_native_debug_image_compatibility() {
        let image_json = r#"{"debug_id":"32420279-25E2-34E6-8BC7-8A006A8F2425","image_addr":"0x000000010258c000","code_file":"/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies","type":"macho","image_size":1720320,"image_vmaddr":"0x0000000100000000"}"#;
        let image: NativeDebugImage = serde_json::from_str(image_json).unwrap();
        let json = serde_json::to_string(&image).unwrap();
        let annotated = Annotated::from_json(&json[..]).unwrap();
        assert_eq!(
            Annotated::new(DebugImage::MachO(Box::new(SchemaImage {
                arch: Annotated::empty(),
                code_file: Annotated::new("/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies".into()),
                code_id: Annotated::empty(),
                debug_file: Annotated::empty(),
                debug_id: Annotated::new("32420279-25E2-34E6-8BC7-8A006A8F2425".parse().unwrap()),
                debug_checksum: Annotated::empty(),
                image_addr: Annotated::new(Addr(4334338048)),
                image_size: Annotated::new(1720320),
                image_vmaddr: Annotated::new(Addr(4294967296)),
                other: Map::new(),
            }))), annotated);
    }
}
