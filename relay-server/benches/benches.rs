use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use relay_config::Config;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Pool, Sqlite};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use tokio::runtime::Runtime;

use relay_base_schema::project::ProjectKey;
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
                                true,
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
                                    true,
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
                                true,
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

criterion_group!(sqlite, benchmark_sqlite_envelope_stack);
criterion_group!(buffer, benchmark_envelope_buffer);
criterion_main!(sqlite, buffer);
