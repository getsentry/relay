searchState.loadedDescShard("relay_base_schema", 0, "Basic types for Relay’s API schema used across multiple …\nDefines the <code>DataCategory</code> type that classifies data Relay …\nDefines types related to Sentry events.\nType definitions for Sentry metrics.\nContains <code>ProjectKey</code> and <code>ProjectId</code> types and necessary …\nDefines data types relating to performance spans.\nAn attachment. Quantity is the size of the attachment in …\nClassifies the type of data that is being ingested.\nReserved and unused.\nError events and Events with an <code>event_type</code> not explicitly …\nMetric buckets.\nMetricSecond\nMonitor check-ins.\nMonitor Seat\nProfile\nProfileChunk\nProfileDuration\nIndexed Profile\nSession Replays\nEvents with an event type of <code>csp</code>, <code>hpkp</code>, <code>expectct</code> and …\nSession updates. Quantity is the number of updates in the …\nSpan\nSpanIndexed\nTransaction events.\nIndexed transaction events.\nDEPRECATED: A transaction for which metrics were extracted.\nAny other data category not known by this Relay.\nUser Feedback\nReturns the argument unchanged.\nReturns the data category corresponding to the given name.\nReturns a dedicated category for indexing if this data can …\nCalls <code>U::from(self)</code>.\nReturns true if the DataCategory refers to an error (i.e …\nReturns the canonical name of this data category.\nReturns the numeric value for this outcome.\nA CSP violation payload.\nAll events that do not qualify as any other type.\nEvents that carry an exception payload.\nThe type of an event.\nAn ExpectCT violation payload.\nAn ExpectStaple violation payload.\nAn HPKP violation payload.\nNetwork Error Logging report.\nAn error used when parsing <code>EventType</code>.\nPerformance monitoring transactions carrying spans.\nUser feedback payload.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nBit (<code>&quot;bit&quot;</code>), corresponding to 1/8 of a byte.\nByte (<code>&quot;byte&quot;</code>).\nCounts instances of an event.\nUser-defined metrics directly sent by SDKs and …\nUser-defined units without built-in conversion or default.\nCustom user-defined units without builtin conversion.\nDay (<code>&quot;day&quot;</code>), 86,400 seconds.\nBuilds a statistical distribution over values reported.\nA time duration, defaulting to <code>&quot;millisecond&quot;</code>.\nTime duration units used in <code>MetricUnit::Duration</code>.\nExabyte (<code>&quot;exabyte&quot;</code>), 10^18 bytes.\nExbibyte (<code>&quot;exbibyte&quot;</code>), 2^60 bytes.\nFractions such as percentages, defaulting to <code>&quot;ratio&quot;</code>.\nUnits of fraction used in <code>MetricUnit::Fraction</code>.\nStores absolute snapshots of values.\nGibibyte (<code>&quot;gibibyte&quot;</code>), 2^30 bytes.\nGigabyte (<code>&quot;gigabyte&quot;</code>), 10^9 bytes.\nHour (<code>&quot;hour&quot;</code>), 3600 seconds.\nSize of information derived from bytes, defaulting to …\nSize of information derived from bytes, used in …\nKibibyte (<code>&quot;kibibyte&quot;</code>), 2^10 bytes.\nKilobyte (<code>&quot;kilobyte&quot;</code>), 10^3 bytes.\nMebibyte (<code>&quot;mebibyte&quot;</code>), 2^20 bytes.\nMegabyte (<code>&quot;megabyte&quot;</code>), 10^6 bytes.\nOptimized string represenation of a metric name.\nThe namespace of a metric.\nA unique identifier for metrics including typing and …\nThe type of a <code>MetricResourceIdentifier</code>, determining its …\nThe unit of measurement of a metric value.\nMicrosecond (<code>&quot;microsecond&quot;</code>), 10^-6 seconds.\nMillisecond (<code>&quot;millisecond&quot;</code>), 10^-3 seconds.\nMinute (<code>&quot;minute&quot;</code>), 60 seconds.\nNanosecond (<code>&quot;nanosecond&quot;</code>), 10^-9 seconds.\nUntyped value without a unit (<code>&quot;&quot;</code>).\nAn error returned when metrics or MRIs cannot be parsed.\nAn error parsing a <code>MetricUnit</code> or one of its variants.\nPebibyte (<code>&quot;pebibyte&quot;</code>), 2^50 bytes.\nRatio expressed as a fraction of <code>100</code>. <code>100%</code> equals a ratio …\nPetabyte (<code>&quot;petabyte&quot;</code>), 10^15 bytes.\nMetrics extracted from profile functions.\nFloating point fraction of <code>1</code>.\nFull second (<code>&quot;second&quot;</code>).\nMetrics extracted from sessions.\nCounts the number of unique reported values.\nMetrics extracted from spans.\nMetric stats.\nTebibyte (<code>&quot;tebibyte&quot;</code>), 2^40 bytes.\nTerabyte (<code>&quot;terabyte&quot;</code>), 10^12 bytes.\nMetrics extracted from transaction events.\nAn unknown and unsupported metric.\nWeek (<code>&quot;week&quot;</code>), 604,800 seconds.\nReturns all namespaces/variants of this enum.\nReturn the shortcode for this metric type.\nReturns the string representation for this metric type.\nReturns the string representation for this metric unit.\nReturns the string representation for this duration unit.\nReturns the string representation for this information …\nReturns the string representation for this fraction unit.\nReturns the string representation of this unit.\nReturns whether <code>try_normalize_metric_name</code> can normalize …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns <code>true</code> if metric stats are enabled for this …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nConverts the MRI into an owned version with a static …\nReturns <code>true</code> if the metric_unit is <code>None</code>.\nThe display name of the metric in the allowed character …\nExtracts the namespace from a well formed MRI.\nThe namespace for this metric.\nParses and validates an MRI.\nParses a <code>CustomUnit</code> from a string.\nParses an MRI from a string and a separate type.\nExtracts the namespace from a well formed MRI.\nValidates a metric name and normalizes it. This is the …\nExtracts the type from a well formed MRI.\nThe type of a metric, determining its aggregation and …\nThe verbatim unit name of the metric value.\nRaised if an empty value is parsed.\nRaised if the value is not an integer in the supported …\nRaised if a project ID cannot be parsed from a string.\nAn error parsing <code>ProjectKey</code>.\nThe unique identifier of a Sentry project.\nThe public key used in a DSN to identify and authenticate …\nReturns the bytes of the project key.\nReturns the string representation of the project key.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCreates a new project ID from its numeric value.\nParses a <code>ProjectKey</code> from a string.\nParses a <code>ProjectKey</code> from a string with flags.\nReturns the numeric value of this project ID.\nThe operation was aborted, typically due to a concurrency …\nAlready exists (409)\nThe operation was cancelled (typically by the user).\nUnrecoverable data loss or corruption\nDeadline expired before operation could complete.\nOperation was rejected because the system is not in a …\nOther/generic 5xx.\nClient specified an invalid argument. 4xx.\n404 Not Found. Some requested entity (file or directory) …\nThe operation completed successfully.\nOperation was attempted past the valid range.\nError parsing a <code>SpanStatus</code>.\n403 Forbidden\n429 Too Many Requests\nTrace status.\n401 Unauthorized (actually does mean unauthenticated …\n503 Service Unavailable\n501 Not Implemented\nUnknown. Any non-standard HTTP status code.\nReturns the string representation of the status.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.")