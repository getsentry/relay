searchState.loadedDescShard("relay_dynamic_config", 0, "Protocol for dynamic configuration passed down to Relay …\nDeprecated. Defines whether URL transactions should be …\nThe array encoding.\nBase64 encoding.\nAll supported metric bucket encodings.\nConfiguration container to control <code>BucketEncoding</code> per …\nKill switch for controlling the cardinality limiter.\nFor some SDKs, accept all transaction names, while for …\nCombined view of global and project-specific metrics …\nEnable continuous profiling.\nConfiguration for extracting custom measurements from …\nAllow ingestion of metrics in the “custom” namespace.\nEnables device.class synthesis\nCardinality limiter is disabled.\nDiscard transactions in a spans-only world.\nEmpty config, used in tests and as a fallback.\nCardinality limiter is enabled.\nContains the error value.\nWraps a serialization / deserialization result to prevent …\nEnables metric extraction from spans for addon modules.\nEnables metric extraction from spans for common modules.\nWhen enabled, spans will be extracted from a transaction.\nWhen enabled, every standalone segment span will be …\nFeatures exposed by project config.\nA set of <code>Feature</code>s.\nPath to a field to evaluate.\nA dynamic configuration for all Relays passed down from …\nEnumeration of keys in <code>MetricExtractionGroups</code>. In JSON, …\nEnable processing and extracting data from profiles that …\nThe default legacy encoding.\nSubset of <code>ProjectConfig</code> that is passed to external Relays.\nA literal value.\nThe latest version for this config struct.\nConfiguration for generic extraction of metrics from all …\nGroup of metrics &amp; tags that can be enabled or disabled as …\nConfigures global metrics extraction groups.\nGlobal groups for metric extraction.\nSpecification for a metric to extract from some data.\nConfiguration for metrics filtering.\nContains the success value.\nAll options passed down from Sentry to Relay.\nEnable standalone span ingestion via the <code>/spans/</code> OTel …\nAny other group defined by the upstream.\nCardinality limiter is enabled but cardinality limits are …\nEnable processing profiles.\nDynamic, per-DSN configuration passed down from Sentry.\nConfiguration for metric extraction from sessions.\nEnables ingestion of Session Replays (Replay Recordings …\nEnables combining session replay envelope items (Replay …\nEnables data scrubbing of replay recording payloads.\n“addon” metrics.\nMetric extracted for all plans.\nMetrics extracted from spans in the transaction namespace.\nEnable standalone span ingestion.\nOnly accept transaction names with a low-cardinality …\nBuilder for <code>TagSpec</code>.\nConfiguration for removing tags matching the <code>tag</code> pattern …\nMapping between extracted metrics and additional tags to …\nSpecifies how to obtain the value of a tag in <code>TagSpec</code>.\nConfiguration for a tag to add to a metric.\nIntermediate result of the tag spec builder.\nRule defining when a target tag should be set on a metric.\nConfiguration for extracting metrics from transaction …\nAn unsupported or unknown source.\nEnables new User Feedback ingest.\nZstd.\nThis config has been extended with fields from …\nThis config has been extended with default span metrics.\nConfiguration for AI span measurements.\nURLs that are permitted for cross original JavaScript …\nDefines a tag that is extracted unconditionally.\nConfiguration for operation breakdown. Will be emitted …\nSample rate for Cardinality Limiter Sentry errors.\nKill switch for controlling the cardinality limiter.\nList of cardinality limits to enforce for this project.\nCategory of data to extract this metric for.\nCondition that defines when to set the tag.\nAn optional condition to meet before extraction.\nAn optional condition to meet before extraction.\nConverts the given tagging rules from <code>conditional_tagging</code> …\nDeprecated in favor of top-level config field. Still here …\nConfiguration for data scrubbers.\nList of patterns for blocking metrics based on their name.\nConfiguration for removing tags from a bucket.\nDeprecated. Defines whether URL transactions should be …\nReturns an empty <code>MetricExtractionConfig</code> with the latest …\nMaximum event retention for the organization.\nCustom event tags that are transferred from the …\nExposable features enabled for this project.\nRollout rate for producing to the ingest-feedback-events …\nA path to the field to extract the metric from.\nPath to a field containing the tag’s value.\nConfiguration for filter rules.\nReturns the generic inbound filters.\nConfiguration for global inbound filters.\nReturns the configured encoding for a specific namespace.\nIf enabled, runs full normalization in non-processing …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCreates a combined config with an empty global component. …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nDefines the field from which the tag value gets its data.\nInserts a value computed from <code>f</code> into the error boundary if …\nConfiguration of global metric groups.\nThe grouping configuration.\nMapping from group name to metrics specs &amp; tags.\nReturns <code>true</code> if the given feature is in the set.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns <code>true</code> if Relay should not extract metrics from …\nReturns <code>true</code> if the set of features is empty.\nReturns <code>true</code> if there are no changes to the metrics config.\nReturns <code>true</code> if the contained groups are empty.\nReturns <code>true</code> if session metrics is enabled and compatible.\nReturns <code>true</code> if metrics extraction is enabled and …\nReturns <code>true</code> if metric extraction is configured and …\nWhether the set is enabled by default.\n<code>true</code> if a template should be enabled.\nReturns <code>true</code> if the result is <code>Err</code>.\nReturns <code>true</code> if the result is <code>Ok</code>.\nReturns <code>true</code> if the version of this metric extraction …\nThe key of the tag to extract.\nLoads the <code>GlobalConfig</code> from a file if it’s provided.\nReturns <code>true</code> if this mapping matches the provided MRI.\nConfiguration for measurements normalization.\nConfiguration for measurements. NOTE: do not access …\nMetric bucket encoding configuration for distributions by …\nMetric bucket encoding configuration for sets by metric …\nRules for applying metrics tags depending on the event’s …\nConfiguration for global metrics extraction rules.\nConfiguration for generic metrics extraction from all data …\nRollout rate for metric stats.\nReturns an iterator of metric specs.\nA list of metric specifications to extract.\nA list of metric specifications to extract.\nA list of Metric Resource Identifiers (MRI) to apply tags …\nConfiguration for metrics.\nThe Metric Resource Identifier (MRI) of the metric to …\nName of metric of which we want to remove certain tags.\nCreates an enabled configuration with empty defaults.\nCreates a new combined view from two references.\nModifies the global config after deserialization.\nNormalizes the given value by deserializing it and …\nConverts from <code>Result&lt;T, E&gt;</code> to <code>Option&lt;T&gt;</code>.\nSentry options passed down to Relay.\nConfiguration for performance score calculations. Will be …\nConfiguration for PII stripping.\nIf enabled, disables normalization in processing Relays.\nReturns <code>true</code> if any spans are produced for this project.\nList of platform names for which we allow using unsampled …\nSample rate for tuning the amount of unsampled profiles …\nKill switch for shutting down profile function metrics …\nQuotas that apply to all projects.\nUsage quotas for this project.\nConfiguration for sampling traces, if not present there …\nValidates fields in this project config and removes values …\nConfiguration for extracting metrics from sessions.\nWhether or not the abnormal mechanism should be extracted …\nReturns the source of tag values, either literal or a …\nSpan description renaming rules.\nSpan description renaming rules.\nOverall sampling of span extraction.\nValue of the tag that is set.\nReturns an iterator of tag mappings.\nPattern to match keys of tags that we want to remove.\nA list of tags to add to previously extracted metrics.\nA list of tags to add to previously extracted metrics.\nA list of tags to add to the metric.\nA list of tags to add to the metric.\nMetrics on which the tag is set.\nName of the tag that is set.\nConfiguration for extracting metrics from transaction …\nList of relay public keys that are permitted to access …\nWhether or not a project is ready to mark all URL …\nWhether or not a project is ready to mark all URL …\nTransaction renaming rules.\nKill switch for shutting down unsampled_profile metrics\nReturns the contained <code>Ok</code> value or computes it from a …\nLiteral value of the tag.\nThe required version to extract transaction metrics.\nVersioning of metrics extraction. Relay skips extraction …\nDefines a tag that is extracted under the given condition.\nPrepares a tag with a given tag name.\nDefines what value to set for a tag.")