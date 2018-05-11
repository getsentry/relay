#![allow(dead_code)]
use base64;
use serde::Serializer;
use url::Url;

base64_serde_type!(pub StandardBase64, base64::STANDARD);

pub fn serialize_origin<S>(url: &Option<Url>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if let Some(url) = url {
        let string = url.origin().ascii_serialization();
        serializer.serialize_some(&string)
    } else {
        serializer.serialize_none()
    }
}
