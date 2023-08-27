#[cfg(feature = "jsonschema")]
use relay_jsonschema_derive::JsonSchema;
use relay_protocol::{Annotated, Empty, FromValue, IntoValue, Object, Value};

use crate::processor::ProcessValue;

/// Cloud Resource Context.
///
/// This context describes the cloud resource the event originated from.
///
/// Example:
///
/// ```json
/// "cloud_resource": {
///     "cloud.account.id": "499517922981",
///     "cloud.provider": "aws",
///     "cloud.platform": "aws_ec2",
///     "cloud.region": "us-east-1",
///     "cloud.vavailability_zone": "us-east-1e",
///     "host.id": "i-07d3301208fe0a55a",
///     "host.type": "t2.large"
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct CloudResourceContext {
    /// The cloud account ID the resource is assigned to.
    #[metastructure(pii = "maybe")]
    #[metastructure(field = "cloud.account.id")]
    pub cloud_account_id: Annotated<String>,

    /// Name of the cloud provider.
    #[metastructure(field = "cloud.provider")]
    pub cloud_provider: Annotated<String>,

    /// The cloud platform in use.
    /// The prefix of the service SHOULD match the one specified in cloud_provider.
    #[metastructure(field = "cloud.platform")]
    pub cloud_platform: Annotated<String>,

    /// The geographical region the resource is running.
    #[metastructure(field = "cloud.region")]
    pub cloud_region: Annotated<String>,

    /// The zone where the resource is running.
    #[metastructure(field = "cloud.availability_zone")]
    pub cloud_availability_zone: Annotated<String>,

    /// Unique host ID.
    #[metastructure(pii = "maybe")]
    #[metastructure(field = "host.id")]
    pub host_id: Annotated<String>,

    /// Machine type of the host.
    #[metastructure(field = "host.type")]
    pub host_type: Annotated<String>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties, retain = "true", pii = "maybe")]
    pub other: Object<Value>,
}

impl super::DefaultContext for CloudResourceContext {
    fn default_key() -> &'static str {
        "cloud_resource"
    }

    fn from_context(context: super::Context) -> Option<Self> {
        match context {
            super::Context::CloudResource(c) => Some(*c),
            _ => None,
        }
    }

    fn cast(context: &super::Context) -> Option<&Self> {
        match context {
            super::Context::CloudResource(c) => Some(c),
            _ => None,
        }
    }

    fn cast_mut(context: &mut super::Context) -> Option<&mut Self> {
        match context {
            super::Context::CloudResource(c) => Some(c),
            _ => None,
        }
    }

    fn into_context(self) -> super::Context {
        super::Context::CloudResource(Box::new(self))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Context;

    #[test]
    pub(crate) fn test_cloud_resource_context_roundtrip() {
        let json = r#"{
  "cloud.account.id": "499517922981",
  "cloud.provider": "aws",
  "cloud.platform": "aws_ec2",
  "cloud.region": "us-east-1",
  "cloud.availability_zone": "us-east-1e",
  "host.id": "i-07d3301208fe0a55a",
  "host.type": "t2.large",
  "other": "value",
  "type": "cloudresource"
}"#;
        let context = Annotated::new(Context::CloudResource(Box::new(CloudResourceContext {
            cloud_account_id: Annotated::new("499517922981".into()),
            cloud_provider: Annotated::new("aws".into()),
            cloud_platform: Annotated::new("aws_ec2".into()),
            cloud_region: Annotated::new("us-east-1".into()),
            cloud_availability_zone: Annotated::new("us-east-1e".into()),
            host_id: Annotated::new("i-07d3301208fe0a55a".into()),
            host_type: Annotated::new("t2.large".into()),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        })));

        assert_eq!(context, Annotated::from_json(json).unwrap());
        assert_eq!(json, context.to_json_pretty().unwrap());
    }
}
