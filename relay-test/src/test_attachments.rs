#[cfg(test)]
mod tests {
    use relay_base_schema::project::ProjectId;
    use tokio::runtime::Runtime;

    use crate::consumers::{
        kafka_consumer, processing_config, produce_message, AttachmentsConsumer,
    };
    use crate::mini_sentry::MiniSentry;
    use crate::relay::Relay;
    use crate::StateBuilder;

    #[test]
    fn test_attachments_400() {
        let project_id = ProjectId(42);
        let proc = processing_config();

        let sentry = MiniSentry::new().add_project_state(StateBuilder::new());
        let relay = Relay::builder(&sentry)
            .merge_config(proc.clone())
            //.enable_processing()
            .build();

        let consumer = AttachmentsConsumer::new(&proc);
        std::thread::spawn(|| {
            Runtime::new().unwrap().block_on(async {
                dbg!("hey man");
                std::thread::sleep(std::time::Duration::new(3, 0));
                dbg!("lets go!");
                let topic = String::from("relay-test-attachments-c3508c214e7b475ebff49f5d383c2fa9");
                produce_message(&topic, "hey there!:D ").await.unwrap();
                dbg!("aight i produced a msg i guess");
            });
        });
        std::thread::sleep(std::time::Duration::new(0, 0));
        dbg!(consumer.poll());
    }
}
