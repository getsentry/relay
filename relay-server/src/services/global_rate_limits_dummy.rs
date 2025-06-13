/// A dummy implementation of the real, on processing conditional, `GlobalRateLimitsServiceHandle`.
///
/// It exists to not have to infect a lot of the code with compilation conditional code.
#[derive(Clone)]
pub struct GlobalRateLimitsServiceHandle {}

#[cfg(feature = "processing")]
impl relay_quotas::GlobalLimiter for GlobalRateLimitsServiceHandle {
    fn check_global_rate_limits<'a>(
        &self,
        _: &'a [relay_quotas::RedisQuota<'a>],
        _: usize,
    ) -> impl Future<
        Output = Result<Vec<&'a relay_quotas::RedisQuota<'a>>, relay_quotas::RateLimitingError>,
    > + Send {
        async { unreachable!("dummy implementation not used") }
    }
}
