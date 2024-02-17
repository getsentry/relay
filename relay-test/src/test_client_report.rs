#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    fn tet_client_reports() {
        let config = json!( {
            "outcomes": {
                "emit_outcomes": true,
                "batch_size": 1,
                "batch_interval": 1,
                "source": "my-layer",
                "aggregator": {
                    "bucket_interval": 1,
                    "flush_interval": 1,
                },
            }
        });

        let sentry = MiniSentry::new();
        let relay = Relay::builder(&sentry).merge_config(config).build();
    }
}
