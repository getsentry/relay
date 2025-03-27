searchState.loadedDescShard("relay_server", 0, "The Sentry relay server application.\nA stack-like data structure that holds <code>Envelope</code>s.\nThe error type that is returned when an error is …\nAn enveloper buffer that uses in-memory envelopes stacks.\nStruct that composes a <code>Config</code> and <code>MemoryStat</code> and provides …\nWrapper which hides the <code>Arc</code> and exposes utils method to …\nPolymorphic envelope buffering interface.\nAn enveloper buffer that uses sqlite envelopes stacks.\nAn <code>EnvelopeStack</code> that is implemented on an SQLite database.\nStruct that offers access to a SQLite-based store of …\nChecks if the used memory is below both percentage and …\nChecks if the used memory (in bytes) is below the …\nChecks if the used percentage of memory is below the …\nReturns the current memory data without instantiating …\nDeletes and returns at most <code>limit</code> <code>Envelope</code>s from the …\nPersists all envelopes in the <code>EnvelopeStack</code>s to external …\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nCreates either a memory-based or a disk-based envelope …\nReturns <code>true</code> whether the buffer has capacity to accept new …\nInitializes the envelope buffer.\nInserts one or more envelopes into the database.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns true if the implementation stores all envelopes in …\nReturns the total number of envelopes that have been …\nMarks a project as ready or not ready.\nMarks a stack as seen.\nReturns a copy of the most up-to-date memory data.\nCreates a new empty <code>SqliteEnvelopeStack</code>.\nInitializes the <code>SqliteEnvelopeStore</code> with a supplied <code>Pool</code>.\nCreates an instance of <code>MemoryStat</code> and obtains the current …\nCreate an instance of <code>MemoryChecker</code>.\nPeeks the <code>Envelope</code> on top of the stack.\nReturns a reference to the next-in-line envelope.\nPops the <code>Envelope</code> on top of the stack.\nPops the next-in-line envelope.\nPrepares the <code>SqliteEnvelopeStore</code> by running all the …\nReturns a set of project key pairs, representing all the …\nPushes an <code>Envelope</code> on top of the stack.\nAdds an envelope to the buffer.\nRuns a relay web server and spawns all internal worker …\nShuts down the <code>PolymorphicEnvelopeBuffer</code>.\nReturns the total count of envelopes stored in the …\nReturns the total number of bytes that the spooler storage …\nReturns an approximate measure of the used size of the …")