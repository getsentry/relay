use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::metrics::{MetricNamespace, MetricType};

/// Optimized string represenation of a metric name.
///
/// The contained name does not need to be valid MRI, but it usually is.
///
/// The metric name can be efficiently cloned.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
#[serde(transparent)]
pub struct MetricName(Arc<str>);

impl MetricName {
    /// Extracts the type from a well formed MRI.
    ///
    /// If the contained metric name is not a well formed MRI this function returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_base_schema::metrics::{MetricName, MetricType};
    ///
    /// let name = MetricName::from("cfoo");
    /// assert!(name.try_type().is_none());
    /// let name = MetricName::from("c:custom/foo@none");
    /// assert_eq!(name.try_type(), Some(MetricType::Counter));
    /// let name = MetricName::from("d:custom/foo@none");
    /// assert_eq!(name.try_type(), Some(MetricType::Distribution));
    /// let name = MetricName::from("s:custom/foo@none");
    /// assert_eq!(name.try_type(), Some(MetricType::Set));
    /// let name = MetricName::from("g:custom/foo@none");
    /// assert_eq!(name.try_type(), Some(MetricType::Gauge));
    /// ```
    pub fn try_type(&self) -> Option<MetricType> {
        match self.0.as_bytes().get(..2) {
            Some(b"c:") => Some(MetricType::Counter),
            Some(b"d:") => Some(MetricType::Distribution),
            Some(b"s:") => Some(MetricType::Set),
            Some(b"g:") => Some(MetricType::Gauge),
            _ => None,
        }
    }

    /// Extracts the namespace from a well formed MRI.
    ///
    /// Returns [`MetricNamespace::Unsupported`] if the metric name is not a well formed MRI.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_base_schema::metrics::{MetricName, MetricNamespace};
    ///
    /// let name = MetricName::from("foo");
    /// assert_eq!(name.namespace(), MetricNamespace::Unsupported);
    /// let name = MetricName::from("c:custom_oops/foo@none");
    /// assert_eq!(name.namespace(), MetricNamespace::Unsupported);
    ///
    /// let name = MetricName::from("c:custom/foo@none");
    /// assert_eq!(name.namespace(), MetricNamespace::Custom);
    /// ```
    pub fn namespace(&self) -> MetricNamespace {
        self.try_namespace().unwrap_or(MetricNamespace::Unsupported)
    }

    /// Extracts the namespace from a well formed MRI.
    ///
    /// If the contained metric name is not a well formed MRI this function returns `None`.
    ///
    /// # Examples
    ///
    /// ```
    /// use relay_base_schema::metrics::{MetricName, MetricNamespace};
    ///
    /// let name = MetricName::from("foo");
    /// assert!(name.try_namespace().is_none());
    /// let name = MetricName::from("c:custom_oops/foo@none");
    /// assert!(name.try_namespace().is_none());
    ///
    /// let name = MetricName::from("c:custom/foo@none");
    /// assert_eq!(name.try_namespace(), Some(MetricNamespace::Custom));
    /// ```
    pub fn try_namespace(&self) -> Option<MetricNamespace> {
        // A well formed MRI is always in the format `<type>:<namespace>/<name>[@<unit>]`,
        // `<type>` is always a single ascii character.
        //
        // Skip the first two ascii characters and extract the namespace.
        let maybe_namespace = self.0.get(2..)?.split('/').next()?;

        MetricNamespace::all()
            .into_iter()
            .find(|namespace| maybe_namespace == namespace.as_str())
    }
}

impl PartialEq<str> for MetricName {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl fmt::Display for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for MetricName {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<Arc<str>> for MetricName {
    fn from(value: Arc<str>) -> Self {
        Self(value)
    }
}

impl From<&str> for MetricName {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

impl std::ops::Deref for MetricName {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl AsRef<str> for MetricName {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::borrow::Borrow<str> for MetricName {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}
