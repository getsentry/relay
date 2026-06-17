use std::str::FromStr;

use relay_event_schema::protocol::{Addr, DebugId, NativeImagePath};
use serde::{Deserialize, Serialize};
use uuid::{Error as UuidError, Uuid};

use crate::utils;

/// The type of a debug image.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ImageType {
    MachO,
    Symbolic,
    Sourcemap,
    Proguard,
    Jvm,
}

/// A debug information image referenced by a profile.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct DebugImage {
    /// Path or name of the code file (e.g. shared library or executable).
    #[serde(skip_serializing_if = "Option::is_none", alias = "name")]
    pub code_file: Option<NativeImagePath>,
    /// Debug identifier for symbolication.
    #[serde(skip_serializing_if = "Option::is_none", alias = "id")]
    pub debug_id: Option<DebugId>,
    /// The type of debug image (e.g. `symbolic`, `proguard`).
    #[serde(rename = "type")]
    pub image_type: ImageType,
    /// Start address of the image in virtual memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_addr: Option<Addr>,
    /// Preferred load address of the image in virtual memory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_vmaddr: Option<Addr>,
    /// Size of the image in bytes.
    #[serde(
        default,
        deserialize_with = "utils::deserialize_number_from_string",
        skip_serializing_if = "utils::is_zero"
    )]
    pub image_size: u64,
    /// Optional UUID, used as the build ID for proguard images.
    #[serde(skip_serializing_if = "Option::is_none", alias = "build_id")]
    pub uuid: Option<Uuid>,
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
