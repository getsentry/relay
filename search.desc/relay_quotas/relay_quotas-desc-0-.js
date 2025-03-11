searchState.loadedDescShard("relay_quotas", 0, "Quotas and rate limiting for Relay.\nThis item contains metrics of any namespace.\nAn attachment. Quantity is the size of the attachment in …\nCounts the number of individual attachments, as opposed to …\nCounts the number of bytes across items.\nLike <code>RateLimits</code>, a collection of scoped rate limits but …\nThe unit in which a data category is measured.\nCounts the number of items.\nAn efficient container for data categories that avoids …\nClassifies the type of data that is being ingested.\nReserved and unused.\nError events and Events with an <code>event_type</code> not explicitly …\nGlobal scope, matches everything.\nGlobal scope.\nThe supplied delay in seconds was not valid.\nError parsing a <code>RetryAfter</code>.\nData categorization and scoping information.\nA project key, which corresponds to a DSN entry.\nA DSN public key.\nLogByte\nLogItem\nMetric buckets.\nItem scoping of metric namespaces.\nMetricSecond\nCounts the accumulated times across items.\nMonitor check-ins.\nMonitor Seat\nThis item does not contain metrics of any namespace. This …\nThe organization that this project belongs to.\nAn organization with identifier.\nProfile\nProfileChunk\nProfileDuration\nProfileDurationUi\nIndexed Profile\nThe project.\nA project with identifier.\nConfiguration for a data ingestion quota (rate limiting).\nThe scope that a quota applies to.\nA bounded rate limit.\nThe scope that a rate limit applied to.\nAn error returned by <code>RedisRateLimiter</code>.\nA collection of scoped rate limits.\nAn iterator that moves out of <code>RateLimtis</code>.\nImmutable rate limits iterator.\nA machine readable, freeform reason code for rate limits.\nFailed to communicate with Redis.\nA service that executes quotas and checks for rate limits …\nSession Replays\nReplay Video\nA monotonic expiration marker for <code>RateLimit</code>s.\nData scoping information.\nEvents with an event type of <code>csp</code>, <code>hpkp</code>, <code>expectct</code> and …\nSession updates. Quantity is the number of updates in the …\nThis item contains metrics of a specific namespace.\nSpan\nSpanIndexed\nTransaction events.\nIndexed transaction events.\nDEPRECATED: A transaction for which metrics were extracted.\nAny other data category not known by this Relay.\nAny other scope that is not known by this Relay.\nThis is the data category for Uptime monitors.\nUser Feedback\nAdds a limit to this collection.\nAdds a limit to this collection.\nReturns the string representation of this reason code.\nA set of data categories that this quota applies to. If …\nA set of data categories that this quota applies to. If …\nThe data category of the item.\nChecks whether any rate limits apply to the given scoping.\nChecks whether any rate limits apply to the given scoping.\nRemoves expired rate limits from this instance.\nReturns a reference to the contained rate limits.\nReturns whether this rate limit has expired.\nReturns whether this rate limit has expired at the passed …\nExtracts a rate limiting scope from the given item scoping …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the data category corresponding to the given name.\nReturns the quota scope corresponding to the given name.\nCreates a new rate limit for the given <code>Quota</code>.\nCreates a retry after instance.\nThe unique identifier for counting this quota. Required, …\nReturns a dedicated category for indexing if this data can …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns <code>true</code> if there are any limits contained.\nReturns true if the DataCategory refers to an error (i.e …\nReturns <code>true</code> if this data category is an indexed data …\nReturns <code>true</code> if this instance contains active rate limits.\nReturns <code>true</code> if this instance contains no active limits.\nChecks whether any of the quotas in effect for the given …\nReturns whether this quota is valid for tracking.\nReturns an <code>ItemScoping</code> for this scope.\nReturns an iterator over the rate limits.\nThe public key’s internal id.\nMaximum number of matching events allowed. Can be <code>0</code> to …\nReturns the longest rate limit.\nReturns <code>true</code> if the given namespace matches the namespace …\nChecks whether the quota’s constraints match the current …\nChecks whether the rate limit applies to the given item.\nSets the maximum rate limit in seconds.\nMerges all limits into this instance.\nMerges more rate limits into this instance.\nReturns an <code>ItemScoping</code> for metric buckets in this scope.\nReturns the canonical name of this data category.\nReturns the canonical name of this scope.\nReturns the canonical name of this scope.\nNamespace for metric items, requiring …\nThe namespace the quota applies to.\nThe metric namespace of this rate limit.\nCreates a new <code>RedisRateLimiter</code> instance.\nCreates a new reason code from a string.\nCreates an empty RateLimits instance.\nCreates a new, empty instance without any rate limits …\nThe organization id.\nThe project id.\nThe DSN public key.\nA machine readable reason returned when this quota is …\nA machine readable reason indicating which quota caused it.\nReturns the remaining duration until the rate limit …\nReturns the remaining duration until the rate limit …\nReturns the remaining seconds until the rate limit expires.\nReturns the remaining seconds until the rate limit expires …\nA marker when this rate limit expires.\nA scope for this quota. This quota is enforced separately …\nThe scope of this rate limit.\nReturns the identifier of the given scope.\nIdentifier of the scope to apply to. If set, then this …\nScoping of the data.\nReturns the numeric value for this outcome.\nThe time window in seconds to enforce this quota in. …")