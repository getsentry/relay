searchState.loadedDescShard("relay_cardinality", 0, "Relay Cardinality Module\nA cardinality limit.\nRedis Set based cardinality limiter.\nA scope to restrict the <code>CardinalityLimit</code> to.\nContains the error value\nError for the cardinality module.\nA per metric name cardinality limit.\nContains the success value\nAn organization level cardinality limit.\nA project level cardinality limit.\nSomething went wrong with Redis.\nImplementation uses Redis sets to keep track of …\nConfiguration options for the <code>RedisSetLimiter</code>.\nResult type for the cardinality module, using <code>Error</code> as the …\nA sliding window.\nA per metric type cardinality limit.\nAny other scope that is not known by this Relay.\nThe active bucket is the oldest active granule.\nReturns the string representation of this scope.\nCache vacuum interval for the in memory cache.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nA number between 1 and <code>window_seconds</code>. Since <code>window_seconds</code>…\nUnique identifier of the cardinality limit.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nIterate over the quota’s window, yielding values …\nThe cardinality limit.\nRelay Cardinality Limiter\nMetric namespace the limit applies to.\nCreates a new <code>RedisSetLimiter</code>.\nWhether this is a passive limit.\nIf <code>true</code> additional reporting of cardinality is enabled.\nScope which the limit applies to.\nThe sliding window to enforce the cardinality limits in.\nThe number of seconds to apply the limit to.\nUnit of operation for the cardinality limiter.\nCardinality Limiter enforcing cardinality limits on …\nResult of <code>CardinalityLimiter::check_cardinality_limits</code>.\nSplit of the original source containing accepted and …\nCardinality report for a specific limit.\nA single entry to check cardinality for.\nRepresents a unique Id for a bucket within one invocation …\nLimiter responsible to enforce limits.\nAccumulator of all cardinality limiter decisions.\nData scoping information.\nThe list of accepted elements of the source.\nThe current cardinality.\nReturns all cardinality reports grouped by the cardinality …\nVerifies cardinality limits.\nChecks cardinality limits of a list of buckets.\nReturns all id’s of cardinality limits which were …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns <code>true</code> if any items have been rejected.\nHash of the metric name and tags.\nOpaque entry id, used to keep track of indices and buckets.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nRecovers the original list of items passed to the …\nConsumes the result and returns <code>CardinalityLimitsSplit</code> …\nMetric name for which the cardinality limit was applied.\nMetric type for which the cardinality limit was applied.\nName of the item.\nName to which the cardinality limit can be scoped.\nMetric namespace of the item.\nMetric namespace to which the cardinality limit can be …\nCreates a new cardinality limiter.\nCreates a new entry.\nThe organization id.\nOrganization id for which the cardinality limit was …\nThe project id.\nProject id for which the cardinality limit was applied.\nCalled for ever <code>Entry</code> which was rejected from the <code>Limiter</code>.\nReturns an iterator yielding only rejected items.\nThe list of rejected elements of the source, together with …\nCalled for every individual limit applied.\nTime for which the cardinality limit was enforced.\nTransforms this item into a consistent hash.")