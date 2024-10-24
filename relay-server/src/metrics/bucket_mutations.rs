use relay_metrics::Bucket;

/// Tag name that indicates whether the indexed payload from which this metric was extracted, was
/// actually ingested by Sentry (indexed).
const INDEXED_TAG_NAME: &str = "indexed";

/// Returns `true` if the [`Bucket`] is a usage metric, `false` otherwise.
///
/// The usage metric is a special metric that is used to count the occurrences of a transaction
/// or span. This means that the metric is used by consumers to know how many transactions or spans
/// were received by the system (before dynamic sampling).
fn is_usage_metric(bucket: &Bucket) -> bool {
    &bucket.name == "c:transactions/usage@none" || &bucket.name == "c:spans/usage@none"
}

/// Adds an "indexed=true" tag to usage metrics.
///
/// The rationale behind this tag is that we want to notify upstream consumers that a metric refers
/// to an indexed payload (e.g., span or transaction). Knowing this information, the consumer can
/// count only transactions and spans that have no indexed payload, since the ones that have the
/// indexed payload are already counted in their respective consumer.
///
/// # Example
///
/// Let's assume the transactions are handled as follows:
/// - 10 transactions are received by Relay
/// - 5 transactions pass both dynamic sampling and rate limiting and are sent to Kafka
/// - 2 transactions are dropped by dynamic sampling
/// - 3 transactions are dropped by rate limiting
///
/// The transactions/spans that are sent to Kafka will have the tag set to `true` and they won't be counted
/// in the billing consumer since that consumer assumes that those 5 transactions/spans will be
/// received by their respective consumer and thus counted.
///
/// The transactions/spans that are dropped will not have the tag and they will be counted in the
/// billing consumer.
pub fn add_indexed_tag(bucket: &mut Bucket) {
    if is_usage_metric(bucket) {
        bucket
            .tags
            .insert(INDEXED_TAG_NAME.to_owned(), "true".to_owned());
    }
}

/// Removes the indexed tag if present.
///
/// The rationale behind this mutation is to remove the tag if it was set because the payload was
/// kept during dynamic sampling but dropped during rate limiting.
pub fn remove_indexed_tag(bucket: &mut Bucket) {
    if is_usage_metric(bucket) {
        bucket.tags.remove(INDEXED_TAG_NAME);
    }
}
