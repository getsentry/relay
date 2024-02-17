use rdkafka::admin::{AdminClient, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::runtime::{self, Runtime};
use tokio::time::timeout;
use uuid::Uuid;

fn x_get_topic_name() -> impl Fn(&str) -> String {
    let random = Uuid::new_v4().simple().to_string();
    move |topic| format!("relay-test-{}-{}", topic, random)
}

fn get_topic_name(topic: &str) -> String {
    let random = Uuid::new_v4().simple().to_string();
    format!("relay-test-{}-{}", topic, random)
}

pub fn processing_config() -> Value {
    let bootstrap_servers =
        std::env::var("KAFKA_BOOTSTRAP_SERVER").unwrap_or_else(|_| "127.0.0.1:49092".to_string());

    json!({
        "processing": {
            "enabled": true,
            "kafka_config": [
                {
                    "name": "bootstrap.servers",
                    "value": bootstrap_servers
                }
            ],
            "topics": {
                "events": get_topic_name("events"),
                "attachments": get_topic_name("attachments"),
                "transactions": get_topic_name("transactions"),
                "outcomes": get_topic_name("outcomes"),
                "sessions": get_topic_name("sessions"),
                "metrics": get_topic_name("metrics"),
                "metrics_generic": get_topic_name("metrics"),
                "replay_events": get_topic_name("replay_events"),
                "replay_recordings": get_topic_name("replay_recordings"),
                "monitors": get_topic_name("monitors"),
                "spans": get_topic_name("spans")
            },
            "redis": "redis://127.0.0.1",
            "projectconfig_cache_prefix": format!("relay-test-relayconfig-{}", uuid::Uuid::new_v4())
        }
    })
}

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;

pub struct AttachmentsConsumer {
    base: ConsumerBase,
}

impl AttachmentsConsumer {
    pub fn new(processing_config: &serde_json::Value) -> Self {
        let topic = "attachments";
        let consumer = kafka_consumer(topic, processing_config, true).unwrap();
        let base = ConsumerBase::new(consumer);

        Self { base }
    }

    pub fn poll(&self) -> String {
        dbg!("omg we're polling");
        self.base.poll()
    }
}

pub struct ConsumerBase {
    consumer: StreamConsumer,
    test_producer: Option<FutureProducer>,
    // Other fields...
}

impl ConsumerBase {
    pub fn new(consumer: StreamConsumer) -> Self {
        ConsumerBase {
            consumer,
            test_producer: None,
        }
    }

    pub fn poll<U: DeserializeOwned>(&self) -> U {
        let rt = runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            dbg!("lets wait for some msg!");
            let message = self.consumer.recv().await.unwrap().detach();
            dbg!("hey, the msg is here!", &message);
            let payload = message.payload().unwrap();

            serde_json::from_slice(payload).unwrap()
        })
    }
}

pub fn kafka_consumer(
    topic: &str,
    processing_config: &serde_json::Value,
    create_topic: bool,
) -> Option<StreamConsumer> {
    dbg!("enter kafka consumer");
    // Get the Kafka bootstrap servers from the processing_config
    let bootstrap_servers = match processing_config
        .get("processing")
        .unwrap()
        .get("kafka_config")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .find(|config| {
            config
                .get("name")
                .and_then(|name| name.as_str())
                .map(|name| name == "bootstrap.servers")
                .unwrap_or(false)
        })
        .and_then(|config| config.get("value").and_then(|value| value.as_str()))
    {
        Some(bootstrap_servers) => bootstrap_servers.to_owned(),
        None => panic!("Bad kafka_config, could not find 'bootstrap.servers'"),
    };

    // Create Kafka consumer configuration
    let mut consumer_config = ClientConfig::new();
    consumer_config
        .set("bootstrap.servers", &bootstrap_servers)
        .set(
            "group.id",
            &format!("test-consumer-{}", uuid::Uuid::new_v4().simple()),
        )
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");

    // Create a Kafka consumer
    let consumer: StreamConsumer = Runtime::new().unwrap().block_on(async {
        create_kafka_topic(&bootstrap_servers, "wtf man").await;
        dbg!("lets gogogogo , new topic!");
        consumer_config.create().expect("Consumer creation failed")
    });

    // Get the topic name for the specified topic from the processing config
    let topic_name = processing_config
        .get("processing")
        .unwrap()
        .get("topics")
        .unwrap()
        .get(topic)
        .unwrap()
        .to_string();

    let topic_name = String::from("relay-test-attachments-c3508c214e7b475ebff49f5d383c2fa9");
    //let topic_name = String::from("dumb topic");

    // Assign the topic to the consumer
    consumer
        .subscribe(&[&topic_name])
        .expect("Failed to subscribe to topics");

    Some(consumer)
}

pub async fn produce_message(topic: &str, message: &str) -> Result<(), Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:49092")
        .set("message.timeout.ms", "30000") // Wait up to 30 seconds for message delivery
        .set("acks", "1") // This ensures all replicas acknowledge the message. For faster delivery, use "1".
        .create()?;

    let msg = format!("key-{}", message);
    let record = FutureRecord::to(topic).payload(message).key(&msg);
    dbg!(&record);
    dbg!("ok ima send this msg now");
    let send_result = producer.send_result(record).unwrap();
    dbg!("dang that was easy");
    let x = send_result.await;
    dbg!(x);

    Ok(())
}

async fn create_kafka_topic(bootstrap_servers: &str, topic_name: &str) {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()
        .expect("Failed to create AdminClient");

    let new_topic = NewTopic::new(topic_name, 1, TopicReplication::Fixed(1));
    dbg!();
    let result = admin_client
        .create_topics(&[new_topic], &Default::default())
        .await;

    dbg!();

    match result {
        Ok(_) => println!("Topic '{}' created successfully", topic_name),
        Err(e) => println!("Failed to create topic '{}': {}", topic_name, e),
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::BorrowedMessage;
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::Message;
    use tokio::time::{self, Duration};

    #[tokio::test]
    async fn produce_and_consume() {
        let topic = "test_topic";

        // Producer setup
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:29092")
            .create()
            .expect("Producer creation failed");

        // Produce a message
        let produce_future = async {
            let record = FutureRecord::to(topic)
                .payload("Hello from Redpanda!")
                .key("test_key");
            producer
                .send(record, Duration::from_secs(0))
                .await
                .expect("Failed to send message");
            println!("Message produced to topic {}", topic);
        };

        // Consumer setup
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", "test_group")
            .set("bootstrap.servers", "localhost:29092")
            .set("auto.offset.reset", "earliest")
            .create()
            .expect("Consumer creation failed");

        consumer
            .subscribe(&[topic])
            .expect("Failed to subscribe to topics");

        // Consume messages
        let consume_future = async {
            let mut message_stream = consumer.stream();
            if let Some(message) = message_stream.next().await {
                match message {
                    Ok(m) => process_message(&m),
                    Err(e) => println!("Error receiving message: {:?}", e),
                }
            }
        };

        // Execute both futures concurrently (produce, then consume)
        let _ = tokio::join!(
            produce_future,
            time::sleep(Duration::from_secs(1)),
            consume_future
        );
    }

    fn process_message(message: &BorrowedMessage) {
        match message.payload_view::<str>() {
            Some(Ok(payload)) => println!("Received message: {}", payload),
            Some(Err(e)) => println!("Error decoding message: {:?}", e),
            None => println!("Empty message"),
        }
    }
}
