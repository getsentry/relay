searchState.loadedDescShard("relay_cabi", 0, "This package contains the C-ABI of Relay. It builds a C …\nThe operation was aborted, typically due to a concurrency …\nAllows newlines.\nAlready exists (409)\nAn attachment. Quantity is the size of the attachment in …\nCounts the number of individual attachments, as opposed to …\nThe operation was cancelled (typically by the user).\nEnables case insensitive path matching.\nA CSP violation payload.\nClassifies the type of data that is being ingested.\nUnrecoverable data loss or corruption\nDeadline expired before operation could complete.\nReserved and unused.\nAll events that do not qualify as any other type.\nReplay Video\nWhen enabled <code>**</code> matches over path separators and <code>*</code> does …\nError events and Events with an <code>event_type</code> not explicitly …\nEvents that carry an exception payload.\nThe type of an event.\nAn ExpectCT violation payload.\nAn ExpectStaple violation payload.\nOperation was rejected because the system is not in a …\nControls the globbing behaviors.\nAn HPKP violation payload.\nOther/generic 5xx.\nClient specified an invalid argument. 4xx.\nLogByte\nLogItem\nMetric buckets.\nMetricSecond\nMonitor check-ins.\nMonitor Seat\nNetwork Error Logging report.\n404 Not Found. Some requested entity (file or directory) …\nThe operation completed successfully.\nOperation was attempted past the valid range.\nEnables path normalization.\n403 Forbidden\nProfile\nProfileChunk\nUI Profile Chunk.\nProfileDuration\nProfile duration of a UI profile.\nIndexed Profile\nA binary buffer of known length.\nRepresents all possible error codes.\nA geo ip lookup helper based on maxmind db files.\nRepresents a key pair from key generation.\nRepresents a public key in Relay.\nRepresents a register request.\nRepresents a secret key in Relay.\nThe processor that normalizes events for store.\nA length-prefixed UTF-8 string.\nA 16-byte UUID.\nSession Replays\n429 Too Many Requests\nEvents with an event type of <code>csp</code>, <code>hpkp</code>, <code>expectct</code> and …\nSession updates. Quantity is the number of updates in the …\nSpan\nSpanIndexed\nTrace status.\nConfiguration for the store step – validation and …\nTransaction events.\nPerformance monitoring transactions carrying spans.\nIndexed transaction events.\nDEPRECATED: A transaction for which metrics were extracted.\n401 Unauthorized (actually does mean unauthenticated …\n503 Service Unavailable\n501 Not Implemented\nAny other data category not known by this Relay.\nUnknown. Any non-standard HTTP status code.\nThis is the data category for Uptime monitors.\nUser Feedback\nUser feedback payload.\nReturns the string representation of this event type.\nReturns the string representation of the status.\nEmit breakdowns based on given configuration.\nThe name and version of the SDK that sent the event.\nA collection of headers sent by newer browsers about the …\nThe IP address of the SDK that sent the event.\nThe SDK’s sample rate as communicated via envelope …\nPointer to the UTF-8 encoded string data.\nUUID bytes in network byte order (big endian).\nPointer to the raw data.\nWhen <code>Some(true)</code>, individual parts of the event payload is …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nThis maps all errors that can possibly happen.\nReturns the data category corresponding to the given name.\nConfiguration for issue grouping.\nReturns a dedicated category for indexing if this data can …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns true if the DataCategory refers to an error (i.e …\nReturns <code>true</code> if this data category is an indexed data …\nWhen <code>Some(true)</code>, it is assumed that the event has been …\nThe internal identifier of the DSN, which gets added to …\nThe length of the string pointed to by <code>data</code>.\nThe length of the buffer pointed to by <code>data</code>.\nThe maximum amount of seconds an event can be predated …\nThe maximum amount of seconds an event can be dated in the …\nReturns the canonical name of this data category.\nNormalize a cardinality limit config.\nControls whether spans should be normalized (e.g. …\nWhen <code>Some(true)</code>, context information is extracted from the …\nIndicates that the string is owned and must be freed.\nIndicates that the buffer is owned and must be freed.\nThe identifier of the target project, which gets added to …\nThe version of the protocol.\nThe public key used for verifying Relay signatures.\nThe time at which the event was received in this Relay.\nFrees a Relay buf.\nCompares two versions.\nConvert an old datascrubbing config to the new PII config …\nCreates a challenge from a register request and returns …\nParses a <code>DataCategory</code> from an event type.\nReturns the API name of the given <code>DataCategory</code>.\nParses a <code>DataCategory</code> from its API name.\nClears the last error.\nReturns the panic information as string.\nReturns the last error code.\nReturns the last error message.\nGenerates a secret, public key pair.\nRandomly generates an relay id\nFrees a <code>RelayGeoIpLookup</code>.\nOpens a maxminddb file by path.\nInitializes the library\nReturns <code>true</code> if the codeowners path matches the value, …\nPerforms a glob operation on bytes.\nNormalize a global config.\nNormalize a project config.\nParse a sentry release structure from a string.\nWalk through the event and collect selectors that can be …\nScrub an event using new PII stripping config.\nFrees a public key.\nParses a public key from a string.\nConverts a public key into a string.\nVerifies a signature\nVerifies a signature\nFrees a secret key.\nParses a secret key from a string.\nVerifies a signature\nConverts a secret key into a string.\nChunks the given text based on remarks.\nFrees a <code>RelayStoreNormalizer</code>.\nCreates a new normalization config.\nNormalizes the event given as JSON.\nFrees a Relay str.\nCreates a Relay string from a c string.\nA test function that always panics.\nReplaces invalid JSON generated by Python.\nReturns true if the uuid is nil.\nFormats the UUID into a string.\nReturns a list of all valid platform identifiers.\nValidate a PII config against the schema. Used in project …\nValidates a PII selector spec. Used to validate …\nValidates a register response.\nValidate a dynamic rule condition.\nValidate whole rule ( this will be also implemented in …\nReturns true if the given version is supported by this …\nOverrides the default flag for other removal.\nThe identifier of the Replay running while this event was …\nThe secret key used for signing Relay requests.\nThe time at which the event was sent by the client.\nThe raw user-agent string obtained from the submission …\nReturns the numeric value for this outcome.")