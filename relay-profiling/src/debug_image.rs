use std::str::FromStr;

use relay_event_schema::protocol::{Addr, DebugId, NativeImagePath};
use serde::{Deserialize, Serialize};
use uuid::{Error as UuidError, Uuid};

use crate::utils;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
enum ImageType {
    MachO,
    Symbolic,
    Sourcemap,
    Proguard,
    Jvm,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DebugImage {
    #[serde(skip_serializing_if = "Option::is_none", alias = "name")]
    code_file: Option<NativeImagePath>,
    #[serde(skip_serializing_if = "Option::is_none", alias = "id")]
    debug_id: Option<DebugId>,
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

    #[serde(skip_serializing_if = "Option::is_none", alias = "build_id")]
    uuid: Option<Uuid>,
}

impl DebugImage {
    /// Creates a native (ELF/Symbolic) debug image from Perfetto mapping data.
    pub fn native_image(
        code_file: String,
        debug_id: DebugId,
        image_addr: u64,
        image_vmaddr: u64,
        image_size: u64,
    ) -> Self {
        Self {
            code_file: Some(code_file.into()),
            debug_id: Some(debug_id),
            image_type: ImageType::Symbolic,
            image_addr: Some(Addr(image_addr)),
            image_vmaddr: Some(Addr(image_vmaddr)),
            image_size,
            uuid: None,
        }
    }
}

pub fn get_proguard_image(uuid: &str) -> Result<DebugImage, UuidError> {
    Ok(DebugImage {
        code_file: None,
        debug_id: None,
        image_type: ImageType::Proguard,
        image_addr: None,
        image_vmaddr: None,
        image_size: 0,
        uuid: Some(Uuid::from_str(uuid)?),
    })
}

#[cfg(test)]
mod tests {
    use relay_event_schema::protocol::{
        Addr, DebugImage, NativeDebugImage as SchemaImage, ProguardDebugImage,
    };
    use relay_protocol::{Annotated, Map};

    use crate::debug_image::DebugImage as ProfDebugImage;

    #[test]
    fn test_native_debug_image_compatibility() {
        let image_json = r#"{"debug_id":"32420279-25E2-34E6-8BC7-8A006A8F2425","image_addr":"0x000000010258c000","code_file":"/private/var/containers/Bundle/Application/C3511752-DD67-4FE8-9DA2-ACE18ADFAA61/TrendingMovies.app/TrendingMovies","type":"macho","image_size":1720320,"image_vmaddr":"0x0000000100000000"}"#;
        let image: ProfDebugImage = serde_json::from_str(image_json).unwrap();
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

    #[test]
    fn test_android_image_compatibility() {
        let image_json = r#"{"uuid":"32420279-25E2-34E6-8BC7-8A006A8F2425","type":"proguard"}"#;
        let image: ProfDebugImage = serde_json::from_str(image_json).unwrap();
        let json = serde_json::to_string(&image).unwrap();
        let annotated = Annotated::from_json(&json[..]).unwrap();
        assert_eq!(
            Annotated::new(DebugImage::Proguard(Box::new(ProguardDebugImage {
                uuid: Annotated::new("32420279-25E2-34E6-8BC7-8A006A8F2425".parse().unwrap()),
                other: Map::new(),
            }))),
            annotated
        );
    }
}
