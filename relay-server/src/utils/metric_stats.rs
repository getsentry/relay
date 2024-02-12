use relay_metrics::{Bucket, BucketView, MetricNamespace, MetricResourceIdentifier};
use relay_statsd::metric;

use crate::statsd::RelayCounters;

#[derive(Default)]
#[must_use]
pub struct MetricStats {
    custom: Stat,
    sessions: Stat,
    spans: Stat,
    transactions: Stat,
    unsupported: Stat,
}

impl MetricStats {
    pub fn new(buckets: &[Bucket]) -> Self {
        let mut this = Self::default();
        this.update(buckets);
        this
    }

    pub fn update(&mut self, buckets: &[Bucket]) {
        for bucket in buckets {
            let namespace = namespace(bucket);
            self.stat(namespace).add(bucket);
        }
    }

    pub fn emit(self, calls: RelayCounters, count: RelayCounters, cost: RelayCounters) {
        let stats = [
            (MetricNamespace::Custom, self.custom),
            (MetricNamespace::Sessions, self.sessions),
            (MetricNamespace::Spans, self.spans),
            (MetricNamespace::Transactions, self.transactions),
            (MetricNamespace::Unsupported, self.unsupported),
        ];

        for (namespace, stat) in stats {
            if stat.count > 0 {
                metric!(counter(calls) += 1, namespace = namespace.as_str());
                metric!(counter(count) += stat.count, namespace = namespace.as_str());
                metric!(counter(cost) += stat.cost, namespace = namespace.as_str());
            }
        }
    }

    fn stat(&mut self, namespace: MetricNamespace) -> &mut Stat {
        match namespace {
            MetricNamespace::Sessions => &mut self.sessions,
            MetricNamespace::Transactions => &mut self.transactions,
            MetricNamespace::Spans => &mut self.spans,
            MetricNamespace::Custom => &mut self.custom,
            MetricNamespace::Unsupported => &mut self.unsupported,
        }
    }
}

#[derive(Default)]
struct Stat {
    cost: i64,
    count: i64,
}

impl Stat {
    fn add(&mut self, bucket: &Bucket) {
        self.count += 1;
        self.cost += BucketView::new(bucket).estimated_size() as i64;
    }
}

fn namespace(bucket: &Bucket) -> MetricNamespace {
    MetricResourceIdentifier::parse(&bucket.name)
        .map(|mri| mri.namespace)
        .unwrap_or(MetricNamespace::Unsupported)
}
