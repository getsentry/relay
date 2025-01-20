searchState.loadedDescShard("relay_kafka", 0, "Kafka-related functionality.\nComplex events (with attachments) topic.\nKafka producer errors.\nKafka configuration errors.\nSimple events (without attachments) topic.\nFeedback events topic.\nFailed to create a kafka producer because of the invalid …\nFailed to serialize the json message using serde.\nFailed to serialize the message.\nThe user did not configure 0 shard\nConfiguration is wrong and it cannot be used to identify …\nFailed to find configured producer for the requested kafka …\nKeeps all the configured kafka producers and responsible …\nHelper structure responsible for building the actual …\nA name value pair of Kafka config parameter.\nConfig for creating a Kafka producer.\nDefine the topics over which Relay communicates with …\nConfiguration for topic\nDescribes the type which can be sent using kafka producer …\nFailed to fetch the metadata of Kafka.\nGeneric metrics topic, excluding sessions (release health).\nAny metric that is extracted from sessions.\nMonitor check-ins.\nLogs (our log product).\nShared outcomes topic for Relay and Sentry.\nOverride for billing critical outcomes.\nString containing the kafka topic name. In this case the …\nProfiles\nReplayEvents, breadcrumb + session updates for replays\nReplayRecordings, large blobs sent by the replay sdk\nFailed to run schema validation on message.\nObject containing topic name and string identifier of one …\nFailed to send a kafka message.\nStandalone spans without a transaction.\nConfiguration for a “logical” topic/datasink that …\nConfiguration for topics.\nFailed to validate the topic.\nTransaction events topic.\nThe user referenced a kafka config name that does not …\nAdds topic configuration to the current <code>KafkaClientBuilder</code>…\nEvents with attachments topic name.\nConsumes self and returns the built <code>KafkaClient</code>.\nReturns the <code>KafkaClientBuilder</code>\nThe Kafka config name will be used to produce data.\nSimple events topic name.\nFeedback events topic.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nReturns the argument unchanged.\nGet a topic assignment by <code>KafkaTopic</code> value\nReturn the list of headers to be provided when payload is …\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nCalls <code>U::from(self)</code>.\nReturns iterator over the variants of <code>KafkaTopic</code>. It will …\nGet the kafka config for the current topic assignment.\nReturns the partitioning key for this kafka message …\nTopic name for all other kinds of metrics.\nTopic name for metrics extracted from sessions, aka …\nMonitor check-ins.\nName of the Kafka config parameter.\nCreates an empty KafkaClientBuilder.\nLogs from our logs product.\nOutcomes topic name.\nOutcomes topic name for billing critical outcomes.\nParameters for the Kafka producer configuration.\nStacktrace topic name\nReplay Events topic name.\nRecordings topic name.\nSends the payload to the correct producer for the current …\nSends message to the provided kafka topic.\nSerializes the message into its binary format.\nStandalone spans without a transaction.\nThe topic name to use.\nTransaction events topic name.\nAdditional topic assignments configured but currently …\nValue of the Kafka config parameter.\nReturns the type of the message.")