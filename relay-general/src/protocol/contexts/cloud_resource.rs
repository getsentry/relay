use crate::types::Annotated;

/// Cloud Resource Context
///
/// This context describes the cloud resource the event originated from.
///
/// Example:
///
/// ```json
/// {
///   "contexts": {
///     "cloud_resource": {
///       "cloud.provider": "aws",
///       "cloud.platform": "aws_ec2",
///       "cloud.account.id": "499517922981",
///       "cloud.region": "us-east-1",
///       "cloud.availability_zone": "us-east-1e",
///       "host.id": "i-07d3301208fe0a55a",
///       "host.type": "t2.large"
///     }
///   }
/// }
/// ```
#[derive(Clone, Debug, Default, PartialEq, Empty, FromValue, IntoValue, ProcessValue)]
#[cfg_attr(feature = "jsonschema", derive(JsonSchema))]
pub struct CloudResourceContext {
    /// Name of the cloud provider.
    pub cloud_provider: Annotated<String>,

    /// The cloud account ID the resource is assigned to.
    #[metastructure(pii = "maybe")]
    pub cloud_account_id: Annotated<String>,

    /// The geographical region the resource is running.
    pub cloud_region: Annotated<String>,

    /// The zone where the resource is running.
    pub cloud_availability_zone: Annotated<String>,

    /// The cloud platform in use.
    /// The prefix of the service SHOULD match the one specified in cloud_provider.
    pub cloud_platform: Annotated<String>,

    /// Unique host ID.
    #[metastructure(pii = "maybe")]
    pub host_id: Annotated<String>,

    /// Machine type of host.
    pub host_type: Annotated<String>,
}

impl CloudResourceContext {
    /// The key under which a runtime context is generally stored (in `Contexts`).
    pub fn default_key() -> &'static str {
        "cloud_resource"
    }
}
