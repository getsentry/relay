use serde::{Deserialize, Serialize};

use relay_general::protocol::{Addr, DebugId, NativeImagePath};

use crate::utils::deserialize_number_from_string;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
enum ImageType {
    MachO,
    Symbolic,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct NativeDebugImage {
    #[serde(alias = "name")]
    code_file: NativeImagePath,
    #[serde(alias = "id")]
    debug_id: DebugId,
    image_addr: Addr,

    #[serde(default)]
    image_vmaddr: Addr,

    #[serde(deserialize_with = "deserialize_number_from_string")]
    image_size: u64,

    #[serde(rename = "type")]
    image_type: ImageType,
}

#[cfg(test)]
mod tests {
    use super::NativeDebugImage;

    use relay_general::protocol::{Addr, DebugImage, NativeDebugImage as RelayNativeDebugImage};
    use relay_general::types::{Annotated, Map};

    #[test]
    fn test_native_debug_image_compatibility() {
        let image_json = r#"{"debug_id":"32420279-25E2-34E6-8BC7-8A006A8F2425","image_addr":"0x000000010258c000","code_file":"/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies","type":"macho","image_size":1720320,"image_vmaddr":"0x0000000100000000"}"#;
        let image: NativeDebugImage = serde_json::from_str(image_json).unwrap();
        let json = serde_json::to_string(&image).unwrap();
        let annotated = Annotated::from_json(&json[..]).unwrap();
        assert_eq!(
            Annotated::new(DebugImage::MachO(Box::new(RelayNativeDebugImage {
                arch: Annotated::empty(),
                code_file: Annotated::new("/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies".into()),
                code_id: Annotated::empty(),
                debug_file: Annotated::empty(),
                debug_id: Annotated::new("32420279-25E2-34E6-8BC7-8A006A8F2425".parse().unwrap()),
                image_addr: Annotated::new(Addr(4334338048)),
                image_size: Annotated::new(1720320),
                image_vmaddr: Annotated::new(Addr(4294967296)),
                other: Map::new(),
            }))), annotated);
    }
}
