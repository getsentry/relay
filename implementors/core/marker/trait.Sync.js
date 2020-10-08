(function() {var implementors = {};
implementors["document_metrics"] = [{"text":"impl Sync for ParseSchemaFormatError","synthetic":true,"types":[]},{"text":"impl !Sync for MetricPath","synthetic":true,"types":[]},{"text":"impl Sync for Metric","synthetic":true,"types":[]},{"text":"impl Sync for Cli","synthetic":true,"types":[]},{"text":"impl Sync for SchemaFormat","synthetic":true,"types":[]},{"text":"impl Sync for MetricType","synthetic":true,"types":[]}];
implementors["generate_schema"] = [{"text":"impl Sync for ParseSchemaFormatError","synthetic":true,"types":[]},{"text":"impl Sync for Cli","synthetic":true,"types":[]},{"text":"impl Sync for SchemaFormat","synthetic":true,"types":[]}];
implementors["process_event"] = [{"text":"impl Sync for Cli","synthetic":true,"types":[]}];
implementors["relay"] = [{"text":"impl Sync for THEME","synthetic":true,"types":[]}];
implementors["relay_auth"] = [{"text":"impl Sync for RelayVersion","synthetic":true,"types":[]},{"text":"impl Sync for ParseRelayVersionError","synthetic":true,"types":[]},{"text":"impl Sync for SignatureHeader","synthetic":true,"types":[]},{"text":"impl Sync for PublicKey","synthetic":true,"types":[]},{"text":"impl Sync for SecretKey","synthetic":true,"types":[]},{"text":"impl Sync for Registration","synthetic":true,"types":[]},{"text":"impl Sync for SignedRegisterState","synthetic":true,"types":[]},{"text":"impl Sync for RegisterState","synthetic":true,"types":[]},{"text":"impl Sync for RegisterRequest","synthetic":true,"types":[]},{"text":"impl Sync for RegisterChallenge","synthetic":true,"types":[]},{"text":"impl Sync for RegisterResponse","synthetic":true,"types":[]},{"text":"impl Sync for KeyParseError","synthetic":true,"types":[]},{"text":"impl Sync for UnpackError","synthetic":true,"types":[]}];
implementors["relay_cabi"] = [{"text":"impl Sync for RelayPublicKey","synthetic":true,"types":[]},{"text":"impl Sync for RelaySecretKey","synthetic":true,"types":[]},{"text":"impl !Sync for RelayKeyPair","synthetic":true,"types":[]},{"text":"impl Sync for RelayRegisterRequest","synthetic":true,"types":[]},{"text":"impl Sync for RelayUuid","synthetic":true,"types":[]},{"text":"impl !Sync for RelayBuf","synthetic":true,"types":[]},{"text":"impl Sync for RelayGeoIpLookup","synthetic":true,"types":[]},{"text":"impl Sync for RelayStoreNormalizer","synthetic":true,"types":[]},{"text":"impl Sync for RelayErrorCode","synthetic":true,"types":[]},{"text":"impl Sync for GlobFlags","synthetic":true,"types":[]},{"text":"impl Sync for RelayStr","synthetic":false,"types":[]}];
implementors["relay_common"] = [{"text":"impl&lt;T&gt; Sync for UpsertingLazyCell&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Send + Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for ParseEventTypeError","synthetic":true,"types":[]},{"text":"impl Sync for ParseSpanStatusError","synthetic":true,"types":[]},{"text":"impl Sync for GlobOptions","synthetic":true,"types":[]},{"text":"impl&lt;'a, E:&nbsp;?Sized&gt; Sync for LogError&lt;'a, E&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;E: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for ParseProjectKeyError","synthetic":true,"types":[]},{"text":"impl Sync for ProjectKey","synthetic":true,"types":[]},{"text":"impl Sync for RetryBackoff","synthetic":true,"types":[]},{"text":"impl Sync for UnixTimestamp","synthetic":true,"types":[]},{"text":"impl Sync for Glob","synthetic":true,"types":[]},{"text":"impl&lt;T&gt; Sync for GlobMatcher&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl&lt;'a, T&gt; Sync for LazyCellRef&lt;'a, T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for EventType","synthetic":true,"types":[]},{"text":"impl Sync for DataCategory","synthetic":true,"types":[]},{"text":"impl Sync for SpanStatus","synthetic":true,"types":[]},{"text":"impl Sync for MetricsClient","synthetic":true,"types":[]}];
implementors["relay_config"] = [{"text":"impl Sync for ByteSize","synthetic":true,"types":[]},{"text":"impl Sync for ConfigError","synthetic":true,"types":[]},{"text":"impl Sync for OverridableConfig","synthetic":true,"types":[]},{"text":"impl Sync for Credentials","synthetic":true,"types":[]},{"text":"impl Sync for Relay","synthetic":true,"types":[]},{"text":"impl Sync for TopicNames","synthetic":true,"types":[]},{"text":"impl Sync for KafkaConfigParam","synthetic":true,"types":[]},{"text":"impl Sync for Processing","synthetic":true,"types":[]},{"text":"impl Sync for Outcomes","synthetic":true,"types":[]},{"text":"impl Sync for MinimalConfig","synthetic":true,"types":[]},{"text":"impl Sync for Config","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for UpstreamDescriptor&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for ConfigErrorKind","synthetic":true,"types":[]},{"text":"impl Sync for RelayMode","synthetic":true,"types":[]},{"text":"impl Sync for LogFormat","synthetic":true,"types":[]},{"text":"impl Sync for HttpEncoding","synthetic":true,"types":[]},{"text":"impl Sync for KafkaTopic","synthetic":true,"types":[]},{"text":"impl Sync for UpstreamError","synthetic":true,"types":[]},{"text":"impl Sync for UpstreamParseError","synthetic":true,"types":[]}];
implementors["relay_ffi"] = [{"text":"impl Sync for Panic","synthetic":true,"types":[]}];
implementors["relay_filter"] = [{"text":"impl Sync for GlobPatterns","synthetic":true,"types":[]},{"text":"impl Sync for FilterConfig","synthetic":true,"types":[]},{"text":"impl Sync for ClientIpsFilterConfig","synthetic":true,"types":[]},{"text":"impl Sync for CspFilterConfig","synthetic":true,"types":[]},{"text":"impl Sync for ErrorMessagesFilterConfig","synthetic":true,"types":[]},{"text":"impl Sync for ReleasesFilterConfig","synthetic":true,"types":[]},{"text":"impl Sync for LegacyBrowsersFilterConfig","synthetic":true,"types":[]},{"text":"impl Sync for FiltersConfig","synthetic":true,"types":[]},{"text":"impl Sync for FilterStatKey","synthetic":true,"types":[]},{"text":"impl Sync for LegacyBrowser","synthetic":true,"types":[]}];
implementors["relay_general"] = [{"text":"impl&lt;'a&gt; Sync for PiiAttachmentsProcessor&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for CompiledPiiConfig","synthetic":true,"types":[]},{"text":"impl Sync for AliasRule","synthetic":true,"types":[]},{"text":"impl Sync for MultipleRule","synthetic":true,"types":[]},{"text":"impl Sync for Pattern","synthetic":true,"types":[]},{"text":"impl Sync for PatternRule","synthetic":true,"types":[]},{"text":"impl Sync for PiiConfig","synthetic":true,"types":[]},{"text":"impl Sync for RedactPairRule","synthetic":true,"types":[]},{"text":"impl Sync for RuleSpec","synthetic":true,"types":[]},{"text":"impl Sync for Vars","synthetic":true,"types":[]},{"text":"impl Sync for DataScrubbingConfig","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for PiiProcessor&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for ReplaceRedaction","synthetic":true,"types":[]},{"text":"impl Sync for ScrubEncodings","synthetic":true,"types":[]},{"text":"impl Sync for RuleType","synthetic":true,"types":[]},{"text":"impl Sync for ScrubMinidumpError","synthetic":true,"types":[]},{"text":"impl Sync for Redaction","synthetic":true,"types":[]},{"text":"impl Sync for FieldAttrs","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for Path&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for ProcessingState&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for UnknownValueTypeError","synthetic":true,"types":[]},{"text":"impl Sync for BagSize","synthetic":true,"types":[]},{"text":"impl Sync for MaxChars","synthetic":true,"types":[]},{"text":"impl Sync for Pii","synthetic":true,"types":[]},{"text":"impl Sync for ValueType","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for Chunk&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for SelectorPathItem","synthetic":true,"types":[]},{"text":"impl Sync for SelectorSpec","synthetic":true,"types":[]},{"text":"impl Sync for Breadcrumb","synthetic":true,"types":[]},{"text":"impl Sync for ClientSdkInfo","synthetic":true,"types":[]},{"text":"impl Sync for ClientSdkPackage","synthetic":true,"types":[]},{"text":"impl Sync for AppContext","synthetic":true,"types":[]},{"text":"impl Sync for BrowserContext","synthetic":true,"types":[]},{"text":"impl Sync for ContextInner","synthetic":true,"types":[]},{"text":"impl Sync for Contexts","synthetic":true,"types":[]},{"text":"impl Sync for DeviceContext","synthetic":true,"types":[]},{"text":"impl Sync for GpuContext","synthetic":true,"types":[]},{"text":"impl Sync for OsContext","synthetic":true,"types":[]},{"text":"impl Sync for RuntimeContext","synthetic":true,"types":[]},{"text":"impl Sync for SpanId","synthetic":true,"types":[]},{"text":"impl Sync for TraceContext","synthetic":true,"types":[]},{"text":"impl Sync for TraceId","synthetic":true,"types":[]},{"text":"impl Sync for AppleDebugImage","synthetic":true,"types":[]},{"text":"impl Sync for CodeId","synthetic":true,"types":[]},{"text":"impl Sync for DebugId","synthetic":true,"types":[]},{"text":"impl Sync for DebugMeta","synthetic":true,"types":[]},{"text":"impl Sync for NativeDebugImage","synthetic":true,"types":[]},{"text":"impl Sync for NativeImagePath","synthetic":true,"types":[]},{"text":"impl Sync for SystemSdkInfo","synthetic":true,"types":[]},{"text":"impl Sync for Event","synthetic":true,"types":[]},{"text":"impl Sync for EventId","synthetic":true,"types":[]},{"text":"impl Sync for EventProcessingError","synthetic":true,"types":[]},{"text":"impl Sync for ExtraValue","synthetic":true,"types":[]},{"text":"impl Sync for GroupingConfig","synthetic":true,"types":[]},{"text":"impl Sync for Exception","synthetic":true,"types":[]},{"text":"impl Sync for Fingerprint","synthetic":true,"types":[]},{"text":"impl Sync for LogEntry","synthetic":true,"types":[]},{"text":"impl Sync for Message","synthetic":true,"types":[]},{"text":"impl Sync for Measurements","synthetic":true,"types":[]},{"text":"impl Sync for CError","synthetic":true,"types":[]},{"text":"impl Sync for MachException","synthetic":true,"types":[]},{"text":"impl Sync for Mechanism","synthetic":true,"types":[]},{"text":"impl Sync for MechanismMeta","synthetic":true,"types":[]},{"text":"impl Sync for PosixSignal","synthetic":true,"types":[]},{"text":"impl Sync for Metrics","synthetic":true,"types":[]},{"text":"impl Sync for Cookies","synthetic":true,"types":[]},{"text":"impl Sync for HeaderName","synthetic":true,"types":[]},{"text":"impl Sync for HeaderValue","synthetic":true,"types":[]},{"text":"impl Sync for Headers","synthetic":true,"types":[]},{"text":"impl Sync for Query","synthetic":true,"types":[]},{"text":"impl Sync for Request","synthetic":true,"types":[]},{"text":"impl Sync for Csp","synthetic":true,"types":[]},{"text":"impl Sync for ExpectCt","synthetic":true,"types":[]},{"text":"impl Sync for ExpectStaple","synthetic":true,"types":[]},{"text":"impl Sync for Hpkp","synthetic":true,"types":[]},{"text":"impl Sync for ParseSessionStatusError","synthetic":true,"types":[]},{"text":"impl Sync for SessionAttributes","synthetic":true,"types":[]},{"text":"impl Sync for SessionUpdate","synthetic":true,"types":[]},{"text":"impl Sync for Span","synthetic":true,"types":[]},{"text":"impl Sync for Frame","synthetic":true,"types":[]},{"text":"impl Sync for FrameData","synthetic":true,"types":[]},{"text":"impl Sync for FrameVars","synthetic":true,"types":[]},{"text":"impl Sync for RawStacktrace","synthetic":true,"types":[]},{"text":"impl Sync for Stacktrace","synthetic":true,"types":[]},{"text":"impl Sync for TagEntry","synthetic":true,"types":[]},{"text":"impl Sync for Tags","synthetic":true,"types":[]},{"text":"impl Sync for TemplateInfo","synthetic":true,"types":[]},{"text":"impl Sync for Thread","synthetic":true,"types":[]},{"text":"impl Sync for Addr","synthetic":true,"types":[]},{"text":"impl Sync for InvalidRegVal","synthetic":true,"types":[]},{"text":"impl Sync for IpAddr","synthetic":true,"types":[]},{"text":"impl Sync for JsonLenientString","synthetic":true,"types":[]},{"text":"impl Sync for LenientString","synthetic":true,"types":[]},{"text":"impl&lt;T&gt; Sync for PairList&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for ParseLevelError","synthetic":true,"types":[]},{"text":"impl Sync for RegVal","synthetic":true,"types":[]},{"text":"impl Sync for Timestamp","synthetic":true,"types":[]},{"text":"impl&lt;T&gt; Sync for Values&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for Geo","synthetic":true,"types":[]},{"text":"impl Sync for User","synthetic":true,"types":[]},{"text":"impl Sync for UserReport","synthetic":true,"types":[]},{"text":"impl Sync for Context","synthetic":true,"types":[]},{"text":"impl Sync for DebugImage","synthetic":true,"types":[]},{"text":"impl Sync for SecurityReportType","synthetic":true,"types":[]},{"text":"impl Sync for SessionStatus","synthetic":true,"types":[]},{"text":"impl Sync for ThreadId","synthetic":true,"types":[]},{"text":"impl Sync for Level","synthetic":true,"types":[]},{"text":"impl Sync for ClockDriftProcessor","synthetic":true,"types":[]},{"text":"impl Sync for GeoIpLookup","synthetic":true,"types":[]},{"text":"impl Sync for StoreConfig","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for StoreProcessor&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl&lt;T&gt; Sync for Annotated&lt;T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for MetaTree","synthetic":true,"types":[]},{"text":"impl&lt;'a, T&gt; Sync for SerializableAnnotated&lt;'a, T&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: Sync,&nbsp;</span>","synthetic":true,"types":[]},{"text":"impl Sync for Error","synthetic":true,"types":[]},{"text":"impl Sync for Meta","synthetic":true,"types":[]},{"text":"impl Sync for Remark","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for ValueDescription&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for ProcessingAction","synthetic":true,"types":[]},{"text":"impl Sync for ErrorKind","synthetic":true,"types":[]},{"text":"impl Sync for RemarkType","synthetic":true,"types":[]},{"text":"impl Sync for SkipSerialization","synthetic":true,"types":[]},{"text":"impl Sync for Value","synthetic":true,"types":[]}];
implementors["relay_quotas"] = [{"text":"impl Sync for Scoping","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for ItemScoping&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for ReasonCode","synthetic":true,"types":[]},{"text":"impl Sync for Quota","synthetic":true,"types":[]},{"text":"impl Sync for RetryAfter","synthetic":true,"types":[]},{"text":"impl Sync for RateLimit","synthetic":true,"types":[]},{"text":"impl Sync for RateLimits","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for RateLimitsIter&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl Sync for RateLimitsIntoIter","synthetic":true,"types":[]},{"text":"impl Sync for RedisRateLimiter","synthetic":true,"types":[]},{"text":"impl Sync for QuotaScope","synthetic":true,"types":[]},{"text":"impl Sync for InvalidRetryAfter","synthetic":true,"types":[]},{"text":"impl Sync for RateLimitScope","synthetic":true,"types":[]},{"text":"impl Sync for RateLimitingError","synthetic":true,"types":[]}];
implementors["relay_redis"] = [{"text":"impl&lt;'a&gt; !Sync for Connection&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl !Sync for PooledClient","synthetic":true,"types":[]},{"text":"impl Sync for RedisPool","synthetic":true,"types":[]},{"text":"impl Sync for RedisConfig","synthetic":true,"types":[]},{"text":"impl Sync for RedisError","synthetic":true,"types":[]}];
implementors["relay_server"] = [{"text":"impl Sync for ServerError","synthetic":true,"types":[]}];
implementors["relay_wstring"] = [{"text":"impl Sync for Utf16Error","synthetic":true,"types":[]},{"text":"impl Sync for WStr","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for WStrChars&lt;'a&gt;","synthetic":true,"types":[]},{"text":"impl&lt;'a&gt; Sync for WStrCharIndices&lt;'a&gt;","synthetic":true,"types":[]}];
implementors["scrub_minidump"] = [{"text":"impl Sync for Cli","synthetic":true,"types":[]}];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()