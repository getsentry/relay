use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use relay_config::Config;
use relay_dynamic_config::ProjectConfig;
use relay_server::services::processor::{ProcessEnvelope, ProcessingGroup};
use relay_server::services::project::ProjectInfo;
use relay_server::utils::ManagedEnvelope;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use relay_base_schema::project::{ProjectId, ProjectKey};
use relay_server::{
    Envelope, EnvelopeStack, MemoryChecker, MemoryStat, PolymorphicEnvelopeBuffer,
    SqliteEnvelopeStack, SqliteEnvelopeStore,
};

fn setup_db(path: &PathBuf) -> Pool<Sqlite> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .journal_mode(SqliteJournalMode::Wal)
        .create_if_missing(true);

    let runtime = Runtime::new().unwrap();
    runtime.block_on(async {
        let db = SqlitePoolOptions::new()
            .connect_with(options)
            .await
            .unwrap();

        sqlx::migrate!("../migrations").run(&db).await.unwrap();

        db
    })
}

async fn reset_db(db: Pool<Sqlite>) {
    sqlx::query("DELETE FROM envelopes")
        .execute(&db)
        .await
        .unwrap();
}

fn mock_envelope(size: &str) -> Box<Envelope> {
    let project_key = "e12d836b15bb49d7bbf99e64295d995b";
    mock_envelope_with_project_key(&ProjectKey::parse(project_key).unwrap(), size)
}

fn mock_envelope_with_project_key(project_key: &ProjectKey, size: &str) -> Box<Envelope> {
    let payload = match size {
        "small" => "small_payload".to_string(),
        "medium" => "medium_payload".repeat(100),
        "big" => "big_payload".repeat(1000),
        "huge" => "huge_payload".repeat(10000),
        _ => "default_payload".to_string(),
    };

    let bytes = Bytes::from(format!(
        "\
         {{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://{}:@sentry.io/42\"}}\n\
         {{\"type\":\"attachment\"}}\n\
         {}\n\
         ",
        project_key,
        payload
    ));

    let mut envelope = Envelope::parse_bytes(bytes).unwrap();
    envelope.set_start_time(Instant::now());
    envelope
}

fn benchmark_sqlite_envelope_stack(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let db = setup_db(&db_path);
    let envelope_store = SqliteEnvelopeStore::new(db.clone(), Duration::from_millis(100));

    let runtime = Runtime::new().unwrap();

    let mut group = c.benchmark_group("sqlite_envelope_stack");
    group.measurement_time(Duration::from_secs(60));

    let disk_batch_size = 1000;
    for size in [1_000, 10_000, 100_000].iter() {
        for envelope_size in &["small", "medium", "big", "huge"] {
            group.throughput(Throughput::Elements(*size as u64));

            // Benchmark push operations
            group.bench_with_input(
                BenchmarkId::new(format!("push_{}", envelope_size), size),
                size,
                |b, &size| {
                    b.iter_with_setup(
                        || {
                            runtime.block_on(async {
                                reset_db(db.clone()).await;
                            });

                            let stack = SqliteEnvelopeStack::new(
                                envelope_store.clone(),
                                disk_batch_size,
                                2,
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                            );

                            let mut envelopes = Vec::with_capacity(size);
                            for _ in 0..size {
                                envelopes.push(mock_envelope(envelope_size));
                            }

                            (stack, envelopes)
                        },
                        |(mut stack, envelopes)| {
                            runtime.block_on(async {
                                for envelope in envelopes {
                                    stack.push(envelope).await.unwrap();
                                }
                            });
                        },
                    );
                },
            );

            // Benchmark pop operations
            group.bench_with_input(
                BenchmarkId::new(format!("pop_{}", envelope_size), size),
                size,
                |b, &size| {
                    b.iter_with_setup(
                        || {
                            runtime.block_on(async {
                                reset_db(db.clone()).await;

                                let mut stack = SqliteEnvelopeStack::new(
                                    envelope_store.clone(),
                                    disk_batch_size,
                                    2,
                                    ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                    ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                );

                                // Pre-fill the stack
                                for _ in 0..size {
                                    let envelope = mock_envelope(envelope_size);
                                    stack.push(envelope).await.unwrap();
                                }

                                stack
                            })
                        },
                        |mut stack| {
                            runtime.block_on(async {
                                // Benchmark popping
                                for _ in 0..size {
                                    stack.pop().await.unwrap();
                                }
                            });
                        },
                    );
                },
            );

            // Benchmark mixed push and pop operations
            group.bench_with_input(
                BenchmarkId::new(format!("mixed_{}", envelope_size), size),
                size,
                |b, &size| {
                    b.iter_with_setup(
                        || {
                            runtime.block_on(async {
                                reset_db(db.clone()).await;
                            });

                            let stack = SqliteEnvelopeStack::new(
                                envelope_store.clone(),
                                disk_batch_size,
                                2,
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                                ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b").unwrap(),
                            );

                            // Pre-generate envelopes
                            let envelopes: Vec<Box<Envelope>> =
                                (0..size).map(|_| mock_envelope(envelope_size)).collect();

                            (stack, envelopes)
                        },
                        |(mut stack, envelopes)| {
                            runtime.block_on(async {
                                let mut envelope_iter = envelopes.into_iter();
                                for _ in 0..size {
                                    if rand::random::<bool>() {
                                        if let Some(envelope) = envelope_iter.next() {
                                            stack.push(envelope).await.unwrap();
                                        }
                                    } else if stack.pop().await.is_err() {
                                        // If pop fails (empty stack), push instead
                                        if let Some(envelope) = envelope_iter.next() {
                                            stack.push(envelope).await.unwrap();
                                        }
                                    }
                                }
                            });
                        },
                    );
                },
            );
        }
    }

    group.finish();
}

fn benchmark_envelope_buffer(c: &mut Criterion) {
    use rand::seq::SliceRandom;
    let mut group = c.benchmark_group("envelope_buffer");
    group.sample_size(10);

    let runtime = Runtime::new().unwrap();

    let num_projects = 100000;
    let envelopes_per_project = 10;

    let config: Arc<Config> = Config::from_json_value(serde_json::json!({
        "spool": {
            "health": {
                "max_memory_percent": 1.0
            }
        }
    }))
    .unwrap()
    .into();
    let memory_checker = MemoryChecker::new(MemoryStat::default(), config.clone());

    group.throughput(Throughput::Elements(
        num_projects * envelopes_per_project as u64,
    ));

    group.bench_function("push_only", |b| {
        b.iter_with_setup(
            || {
                let project_keys: Vec<_> = (0..num_projects)
                    .map(|i| ProjectKey::parse(&format!("{:#032x}", i)).unwrap())
                    .collect();

                let mut envelopes = vec![];
                for project_key in &project_keys {
                    for _ in 0..envelopes_per_project {
                        envelopes.push(mock_envelope_with_project_key(project_key, "small"))
                    }
                }

                envelopes.shuffle(&mut rand::thread_rng());

                envelopes
            },
            |envelopes| {
                runtime.block_on(async {
                    let mut buffer =
                        PolymorphicEnvelopeBuffer::from_config(&config, memory_checker.clone())
                            .await
                            .unwrap();
                    for envelope in envelopes.into_iter() {
                        buffer.push(envelope).await.unwrap();
                    }
                })
            },
        );
    });

    group.bench_function("push_pop", |b| {
        b.iter_with_setup(
            || {
                let project_keys: Vec<_> = (0..num_projects)
                    .map(|i| ProjectKey::parse(&format!("{:#032x}", i)).unwrap())
                    .collect();

                let mut envelopes = vec![];
                for project_key in &project_keys {
                    for _ in 0..envelopes_per_project {
                        envelopes.push(mock_envelope_with_project_key(project_key, "big"))
                    }
                }

                envelopes.shuffle(&mut rand::thread_rng());

                envelopes
            },
            |envelopes| {
                runtime.block_on(async {
                    let mut buffer =
                        PolymorphicEnvelopeBuffer::from_config(&config, memory_checker.clone())
                            .await
                            .unwrap();
                    let n = envelopes.len();
                    for envelope in envelopes.into_iter() {
                        let public_key = envelope.meta().public_key();
                        buffer.push(envelope).await.unwrap();
                        // Mark as ready:
                        buffer.mark_ready(&public_key, true);
                    }
                    for _ in 0..n {
                        let envelope = buffer.pop().await.unwrap().unwrap();
                        // Send back to end of queue to get worse-case behavior:
                        buffer.mark_ready(&envelope.meta().public_key(), false);
                    }
                })
            },
        );
    });

    group.finish();
}

mod testutils {
    use std::sync::Arc;

    use rayon::ThreadPool;
    use relay_cogs::Cogs;
    use relay_config::Config;
    use relay_dynamic_config::GlobalConfig;
    use relay_server::metrics::{MetricOutcomes, MetricStats};
    use relay_server::service::create_redis_pools;
    use relay_server::services::global_config::GlobalConfigHandle;
    use relay_server::services::metrics::Aggregator;
    use relay_server::services::outcome::TrackOutcome;
    use relay_server::services::processor::{self, EnvelopeProcessorService};
    use relay_server::services::test_store::TestStore;
    use relay_server::utils::ThreadPoolBuilder;
    use relay_system::{channel, Addr, Interface};
    use tokio::sync::mpsc::UnboundedReceiver;
    use tokio::task::JoinHandle;

    /// Spawns a mock service that handles messages through a closure.
    ///
    /// Note: Addr must be dropped before handle can be awaited.
    pub fn mock_service<S, I, F>(
        name: &'static str,
        mut state: S,
        mut f: F,
    ) -> (Addr<I>, JoinHandle<S>)
    where
        S: Send + 'static,
        I: Interface,
        F: FnMut(&mut S, I) + Send + 'static,
    {
        let (addr, mut rx) = channel(name);

        let handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                f(&mut state, msg);
            }

            state
        });

        (addr, handle)
    }

    pub fn create_processor_pool() -> ThreadPool {
        ThreadPoolBuilder::new("processor")
            .num_threads(1)
            .runtime(tokio::runtime::Handle::current())
            .build()
            .unwrap()
    }

    fn create_metric_stats(rollout_rate: f32) -> (MetricStats, UnboundedReceiver<Aggregator>) {
        let config = Config::from_json_value(serde_json::json!({
            "processing": {
                "enabled": true,
                "kafka_config": [],
            }
        }))
        .unwrap();

        let mut global_config = GlobalConfig::default();
        global_config.options.metric_stats_rollout_rate = rollout_rate;
        let global_config = GlobalConfigHandle::fixed(global_config);

        let (addr, receiver) = Addr::custom();
        let ms = MetricStats::new(Arc::new(config), global_config, addr);

        (ms, receiver)
    }

    pub fn create_test_processor(config: Config) -> EnvelopeProcessorService {
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (project_cache, _) = mock_service("project_cache", (), |&mut (), _| {});
        let (aggregator, _) = mock_service("aggregator", (), |&mut (), _| {});
        let (upstream_relay, _) = mock_service("upstream_relay", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});

        #[cfg(feature = "processing")]
        let redis_pools = config.redis().map(create_redis_pools).transpose().unwrap();

        let metric_outcomes =
            MetricOutcomes::new(create_metric_stats(1.0).0, outcome_aggregator.clone());

        let config = Arc::new(config);
        EnvelopeProcessorService::new(
            create_processor_pool(),
            Arc::clone(&config),
            GlobalConfigHandle::fixed(Default::default()),
            Cogs::noop(),
            #[cfg(feature = "processing")]
            redis_pools,
            processor::Addrs {
                outcome_aggregator,
                project_cache,
                upstream_relay,
                test_store,
                #[cfg(feature = "processing")]
                store_forwarder: None,
                aggregator,
            },
            metric_outcomes,
        )
    }

    pub fn processor_services() -> (Addr<TrackOutcome>, Addr<TestStore>) {
        let (outcome_aggregator, _) = mock_service("outcome_aggregator", (), |&mut (), _| {});
        let (test_store, _) = mock_service("test_store", (), |&mut (), _| {});
        (outcome_aggregator, test_store)
    }
}

fn mock_envelope2() -> Box<Envelope> {
    let payload = include_str!("tx.json");

    let bytes = Bytes::from(format!(
        "\
         {{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://e12d836b15bb49d7bbf99e64295d995b:@sentry.io/42\"}}\n\
         {{\"type\":\"transaction\"}}\n\
         {payload}\n\
         ",
    ));

    let mut envelope = Envelope::parse_bytes(bytes).unwrap();
    envelope.set_start_time(Instant::now());
    envelope
}

fn bench_tx_processing(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let _guard = runtime.enter();

    let config = Config::from_json_value(serde_json::json!({
        "processing": {
            "enabled": true,
            "kafka_config": [],
        }
    }))
    .unwrap();
    let processor = testutils::create_test_processor(config);

    let project_key = ProjectKey::parse("e12d836b15bb49d7bbf99e64295d995b");
    let envelope = mock_envelope2();

    let pcjson = include_str!("config.json");
    let project_info = Arc::new(serde_json::from_str::<ProjectInfo>(pcjson).unwrap());

    let (outcome_aggregator, test_store) = testutils::processor_services();

    c.bench_function("process_envelope", |b| {
        b.iter_with_large_drop(|| {
            let envelope = ManagedEnvelope::new(
                envelope.clone(),
                outcome_aggregator.clone(),
                test_store.clone(),
                ProcessingGroup::Transaction,
            );

            let msg = ProcessEnvelope {
                envelope,
                project_info: project_info.clone(),
                sampling_project_info: Some(project_info.clone()),
                reservoir_counters: Default::default(),
            };

            // runtime.block_on(async { processor.handle_process_envelope(criterion::black_box(msg)) })
            processor.handle_process_envelope(criterion::black_box(msg))
        })
    });
}

criterion_group!(processor, bench_tx_processing);
criterion_group!(sqlite, benchmark_sqlite_envelope_stack);
criterion_group!(buffer, benchmark_envelope_buffer);
criterion_main!(sqlite, buffer, processor);
