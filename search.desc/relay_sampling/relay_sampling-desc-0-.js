searchState.loadedDescShard("relay_sampling", 0, "Sampling logic for performing sampling decisions of …\nDynamic sampling rule configuration.\nContextual information stored on traces.\nEvaluation of dynamic sampling rules.\nApply the sample rate of the rule for the full time window …\nSpecifies how to interpolate sample rates for rules with …\nA factor to apply on a subsequently matching rule.\nApply linear interpolation of the sample rate in the time …\nA reservoir limit.\nThe identifier of a <code>SamplingRule</code>.\nDefines what a dynamic sampling rule applies to.\nA direct sample rate to apply.\nRepresents the dynamic sampling configuration available to …\nA sampling rule as it is deserialized from the project …\nA sampling strategy definition.\nA range of time.\nA trace rule matches on the <code>DynamicSamplingContext</code> and …\nA transaction rule matches directly on the transaction …\nIf the sampling config contains new rule types, do not …\nApplies the decaying function to the given sample rate.\nApplies its decaying function to the given sample rate.\nA condition to match for this sampling rule.\nReturns whether the provided time matches the time range.\nDeclares how to interpolate the sample rate for rules with …\nThe exclusive end of the time range.\nFilters the sampling rules by the given <code>RuleType</code>.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nThe unique identifier of this rule.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns true if neither the start nor end time limits are …\nCreates an enabled configuration with empty defaults and …\nUpgrades legacy sampling configs into the latest format.\nThe ordered sampling rules for the project.\n<strong>Deprecated</strong>. The ordered sampling rules for the project in …\nThe sample rate to apply when this rule matches.\nThe inclusive start of the time range.\nThe time range the rule should be applicable in.\nThe rule type declares what to apply a dynamic sampling …\nReturns <code>true</code> if any of the rules in this configuration is …\nThe required version to run dynamic sampling.\nThe target value at the end of the time window.\nThe limit of how many times this rule will be sampled …\nThe sample rate to apply to the rule.\nThe factor to apply on another matched sample rate.\nDynamicSamplingContext created by the first Sentry SDK in …\nUser-related information in a <code>DynamicSamplingContext</code>.\nThe environment.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nAdditional arbitrary fields for forwards compatibility.\nThe project key.\nThe release.\nIf the event occurred during a session replay, the …\nThe sample rate with which this trace was sampled in the …\nSet to true if the transaction starting the trace has been …\nID created by clients to represent the current call flow.\nIn the transaction-based model, this is the name of the …\nThe user specific identifier (e.g. a user segment, or …\nThe value of the <code>user.id</code> property.\nThe value of the <code>user.segment</code> property.\nThe item is not sampled and should be dropped.\nThe item is sampled and should not be dropped.\nRepresents a list of rule ids which is used for outcomes.\nThe amount of matches for each reservoir rule in a given …\nUtility for evaluating reservoir-based sampling rules.\nA sampling decision.\nState machine for dynamic sampling.\nRepresents the specification for sampling an incoming …\nReturns a string representation of the sampling decision.\nGets shared ownership of the reservoir counters.\nReturns the sampling decision.\nEvaluates a reservoir rule, returning <code>true</code> if it should be …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nEvaluates a reservoir rule, returning <code>true</code> if it should be …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns the matched rules for the sampling match.\nReturns <code>true</code> if the sampling decision is <code>Self::Drop</code>.\nReturns <code>true</code> if the sampling decision is <code>Self::Keep</code>.\nAttempts to find a match for sampling rules using …\nConstructor for <code>ReservoirEvaluator</code>.\nConstructs an evaluator without reservoir sampling.\nConstructs an evaluator with reservoir sampling.\nParses <code>MatchedRuleIds</code> from a string with concatenated rule …\nReturns the sample rate.\nSets the Redis pool and organization ID for the …")