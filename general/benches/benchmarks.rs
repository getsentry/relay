#![cfg(feature = "bench")]
#![feature(test)]

macro_rules! benchmark {
    ($sdk:ident) => {
        mod $sdk {
            use std::collections::BTreeSet;

            use semaphore_general::processor::process_value;
            use semaphore_general::protocol::Event;
            use semaphore_general::store::{StoreConfig, StoreNormalizeProcessor};
            use semaphore_general::types::Annotated;

            fn load_json() -> String {
                let path = concat!("fixtures/payloads/", stringify!($sdk), ".json");
                std::fs::read_to_string(path).expect("failed to load json")
            }

            #[bench]
            fn bench_from_value(b: &mut test::Bencher) {
                let json = load_json();
                b.iter(|| Annotated::<Event>::from_json(&json).expect("failed to deserialize"));
            }

            #[bench]
            fn bench_to_json(b: &mut test::Bencher) {
                let json = load_json();
                let event = Annotated::<Event>::from_json(&json).expect("failed to deserialize");
                b.iter(|| event.to_json().expect("failed to serialize"));
            }

            #[bench]
            fn bench_processing(b: &mut test::Bencher) {
                let mut platforms = BTreeSet::new();
                platforms.insert("cocoa".to_string());
                platforms.insert("csharp".to_string());
                platforms.insert("javascript".to_string());
                platforms.insert("native".to_string());
                platforms.insert("node".to_string());
                platforms.insert("python".to_string());

                let config = StoreConfig {
                    project_id: Some(4711),
                    client_ip: Some("127.0.0.1".to_string()),
                    client: Some("sentry.tester".to_string()),
                    is_public_auth: true,
                    key_id: Some("feedface".to_string()),
                    protocol_version: Some("8".to_string()),
                    stacktrace_frames_hard_limit: Some(50),
                    valid_platforms: platforms,
                    max_secs_in_future: Some(3600),
                    max_secs_in_past: Some(2_592_000),
                };

                let json = load_json();
                let mut processor = StoreNormalizeProcessor::new(config, None);
                let event = Annotated::<Event>::from_json(&json).expect("failed to deserialize");

                b.iter(|| {
                    let mut event = test::black_box(event.clone());
                    process_value(&mut event, &mut processor, Default::default());
                    event
                });
            }
        }
    };
}

benchmark!(legacy_js_exception);
benchmark!(legacy_js_message);
benchmark!(legacy_js_onerror);
benchmark!(legacy_js_promise);
benchmark!(legacy_node_exception);
benchmark!(legacy_node_express);
benchmark!(legacy_node_message);
benchmark!(legacy_node_onerror);
benchmark!(legacy_node_promise);
benchmark!(legacy_python);
benchmark!(legacy_swift);

benchmark!(cocoa);
benchmark!(cordova);
benchmark!(dotnet);
benchmark!(electron_main);
benchmark!(electron_renderer);
