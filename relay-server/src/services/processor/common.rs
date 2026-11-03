pub fn check_buckets(
    &self,
    project_key: ProjectKey,
    project_info: &ProjectInfo,
    rate_limits: &RateLimits,
    buckets: Vec<Bucket>,
) -> Vec<Bucket> {
    let Some(scoping) = project_info.scoping(project_key) else {
        relay_log::error!(
            tags.project_key = project_key.as_str(),
            "there is no scoping: dropping {} buckets",
            buckets.len(),
        );
        return Vec::new();
    };

    let mut buckets =
        super::metrics::remove_invalid_namespaces(buckets, &self.inner.metric_outcomes, scoping);

    let mut namespaces: BTreeSet<MetricNamespace> = buckets
        .iter()
        .filter_map(|bucket| bucket.name.try_namespace())
        .collect();

    // Never rate limit outcomes.
    namespaces.remove(&MetricNamespace::Outcomes);

    for namespace in namespaces {
        let limits = rate_limits
            .check_with_quotas(project_info.get_quotas(), &scoping.metric_bucket(namespace));

        if limits.is_limited() {
            let rejected;
            (buckets, rejected) = utils::split_off(buckets, |bucket| {
                bucket.name.try_namespace() == Some(namespace)
            });

            let reason_code = limits.longest().and_then(|limit| limit.reason_code.clone());
            self.inner
                .metric_outcomes
                .track(scoping, &rejected, Outcome::RateLimited(reason_code));
        }
    }

    let quotas = project_info.config.quotas.clone();
    match MetricsLimiter::create(buckets, quotas, scoping) {
        Ok(mut bucket_limiter) => {
            bucket_limiter.enforce_limits(rate_limits, &self.inner.metric_outcomes);
            bucket_limiter.into_buckets()
        }
        Err(buckets) => buckets,
    }
}
