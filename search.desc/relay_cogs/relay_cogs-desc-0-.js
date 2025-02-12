searchState.loadedDescShard("relay_cogs", 0, "Break down the cost of Relay by its components and …\nApp feature a COGS measurement is related to.\nA COGS category.\nA categorized COGS measurement.\nCrons check ins.\nClient reports.\nCOGS measurements collector.\nA COGS measurement.\nCogs recorder, recording actual measurements.\nErrors.\nA collection of weighted app features.\nA builder for <code>FeatureWeights</code> which can be used to …\nLogs.\nMetrics in the custom namespace.\nMetrics in the sessions namespace.\nMetrics in the spans namespace.\nMetrics in the <code>metric_stats</code> namespace.\nMetrics in the transactions namespace.\nMetrics in the unsupported namespace.\nA recorder which discards all measurements.\nProfiles.\nThe Relay resource.\nReplays.\nResource ID as tracked in COGS.\nSessions.\nSpans.\nA time measurement.\nAn in progress COGS measurement.\nTransactions.\nA placeholder which should not be emitted but can be …\nWhen processing an envelope cannot be attributed or is not …\nMetrics are attributed by their namespace, whenever this …\nA COGS measurement value.\nIncreases the <code>weight</code> of an <code>AppFeature</code>.\nReturns the string representation for this app feature.\nBuilds and returns the <code>FeatureWeights</code>.\nReturns an <code>FeatureWeights</code> builder.\nCancels the COGS measurement.\nOptional category for this measurement.\nThe measured app feature.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns <code>true</code> if there are no weights contained.\nMerges two instances of <code>FeatureWeights</code> and sums the …\nString representation of the category.\nCreates a new <code>Cogs</code> from a <code>recorder</code>.\nAttributes all measurements to a single <code>AppFeature</code>.\nAttributes the measurement to nothing.\nCreates a new no-op token, which records nothing.\nShortcut for creating a <code>Cogs</code> from a <code>crate::NoopRecorder</code>.\nRecord a single COGS measurement.\nThe measured resource.\nStarts a categorized measurement.\nStarts a recording for a COGS measurement.\nUpdates the app features to which the active measurement …\nThe measurement value.\nSets the specified <code>weight</code> for an <code>AppFeature</code>.\nReturns an iterator yielding an app feature and it’s …\nRecords a categorized measurement of the passed <code>body</code>, in …")