use relay_statsd::metric;
use relay_system::{FromMessage, Interface, MessageHandler, NoResponse};
use relay_threading::{AsyncPoolService, ServicePool};

use crate::services::metrics::FlushBuckets;
use crate::statsd::RelayTimers;

/// CPU-intensive processing tasks for metric buckets
pub enum BucketProcessor {
    Flush(FlushBuckets),
}

impl Interface for BucketProcessor {}

impl FromMessage<FlushBuckets> for BucketProcessor {
    type Response = NoResponse;

    fn from_message(message: FlushBuckets, _: ()) -> Self {
        Self::Flush(message)
    }
}

/// The message handler for [`BucketProcessor`].
pub struct BucketProcessorService {}

impl BucketProcessorService {
    /// Creates a new, multi-threaded service.
    pub fn new_service(pool: ServicePool) -> AsyncPoolService<BucketProcessorService> {
        let handler = BucketProcessorService {};
        AsyncPoolService::new(handler, pool)
    }

    async fn handle_flush_buckets(&self, mut message: FlushBuckets) {
        for (project_key, pb) in message.buckets.iter_mut() {
            let buckets = std::mem::take(&mut pb.buckets);
            pb.buckets =
                common::check_buckets(*project_key, &pb.project_info, &pb.rate_limits, buckets);
        }

        let config = self.inner.config.current();

        #[cfg(feature = "processing")]
        if config.processing_enabled()
            && let Some(ref store_forwarder) = self.inner.addrs.store_forwarder
        {
            return self
                .encode_metrics_processing(message, store_forwarder)
                .await;
        }

        // Processing Relays never send outcomes as client reports, which is why this check is after
        // the processing check.
        if config.emit_outcomes() == EmitOutcomes::AsClientReports {
            // Remove client reports from metrics to be sent, if configured as client reports
            // and send them separately.
            message = self.encode_metrics_client_reports(message);
        }

        if config.http_global_metrics() {
            self.encode_metrics_global(message)
        } else {
            self.encode_metrics_envelope(message)
        }
    }
}

impl MessageHandler for BucketProcessorService {
    type Interface = BucketProcessor;

    async fn handle_message(&self, BucketProcessor::Flush(message): Self::Interface) {
        metric!(
            timer(RelayTimers::ProcessMessageDuration),
            message = "FlushBuckets",
            { self.handle_flush_buckets(message).await }
        )
    }
}
