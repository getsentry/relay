use bytes::Bytes;
use chrono::Utc;
use clap::{Parser, ValueEnum};
use relay_config::Config;
use relay_server::{Envelope, MemoryChecker, MemoryStat, PolymorphicEnvelopeBuffer};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Impl {
    Memory,
    Sqlite,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Sequential,
    Interleaved,
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    envelope_size_kib: usize,
    #[arg(long)]
    batch_size: usize,
    #[arg(long)]
    implementation: Impl,
    #[arg(long)]
    mode: Mode,
    #[arg(long)]
    projects: usize,
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    println!("{:?}", &args);

    let dir = tempfile::tempdir().unwrap();

    let config = Arc::new(
        Config::from_json_value(serde_json::json!({
            "spool": {
                "envelopes": {
                    "buffer_strategy": match args.implementation {
                        Impl::Memory => "memory",
                        Impl::Sqlite => "sqlite",
                    },
                    "path": match args.implementation {
                        Impl::Memory => None,
                        Impl::Sqlite => Some(dir.path().join("envelopes.db")),
                    },
                    "disk_batch_size": args.batch_size,
                }
            }
        }))
        .unwrap(),
    );

    let memory_checker = MemoryChecker::new(MemoryStat::default(), config.clone());
    let buffer = PolymorphicEnvelopeBuffer::from_config(&config, memory_checker)
        .await
        .unwrap();

    match args.mode {
        Mode::Sequential => {
            run_sequential(
                buffer,
                args.envelope_size_kib * 1024,
                args.projects,
                Duration::from_secs(args.duration_secs),
            )
            .await
        }
        Mode::Interleaved => {
            run_interleaved(
                buffer,
                args.envelope_size_kib * 1024,
                args.projects,
                Duration::from_secs(args.duration_secs),
            )
            .await
        }
    };

    println!("Cleaning up temporary files...");
    drop(dir);
    println!("Done...");
}

async fn run_sequential(
    mut buffer: PolymorphicEnvelopeBuffer,
    envelope_size: usize,
    project_count: usize,
    duration: Duration,
) {
    // Determine envelope size once:
    let proto_envelope = mock_envelope(envelope_size, project_count);
    let bytes_per_envelope = proto_envelope.to_vec().unwrap().len();

    let start_time = Instant::now();

    let mut last_check = Instant::now();
    let mut write_duration = Duration::ZERO;
    let mut writes = 0;
    while start_time.elapsed() < duration / 2 {
        let envelope = mock_envelope(envelope_size, project_count);

        let before = Instant::now();
        buffer.push(envelope).await.unwrap();
        let after = Instant::now();

        write_duration += after - before;
        writes += 1;

        if (after - last_check) > Duration::from_secs(1) {
            let throughput = (writes * bytes_per_envelope) as f64 / write_duration.as_secs_f64();
            let throughput = throughput / 1024.0 / 1024.0;
            println!("Write throughput: {throughput:.2} MiB / s");
            write_duration = Duration::ZERO;
            writes = 0;
            last_check = after;
        }
    }

    let mut last_check = Instant::now();
    let mut read_duration = Duration::ZERO;
    let mut reads = 0;
    while start_time.elapsed() < duration {
        let before = Instant::now();
        if buffer.pop().await.unwrap().is_none() {
            break;
        };
        let after = Instant::now();

        read_duration += after - before;
        reads += 1;

        if (after - last_check) > Duration::from_secs(1) {
            let throughput = (reads * bytes_per_envelope) as f64 / read_duration.as_secs_f64();
            let throughput = throughput / 1024.0 / 1024.0;
            println!("Read throughput: {throughput:.2} MiB / s");
            read_duration = Duration::ZERO;
            reads = 0;
            last_check = after;
        }
    }
}

async fn run_interleaved(
    mut buffer: PolymorphicEnvelopeBuffer,
    envelope_size: usize,
    project_count: usize,
    duration: Duration,
) {
    // Determine envelope size once:
    let proto_envelope = mock_envelope(envelope_size, project_count);
    let bytes_per_envelope = proto_envelope.to_vec().unwrap().len();

    let start_time = Instant::now();

    let mut last_check = Instant::now();
    let mut write_duration = Duration::ZERO;
    let mut read_duration = Duration::ZERO;
    let mut iterations = 0;
    while start_time.elapsed() < duration {
        let envelope = mock_envelope(envelope_size, project_count);

        let before = Instant::now();
        buffer.push(envelope).await.unwrap();
        let after_write = Instant::now();
        buffer.pop().await.unwrap();
        let after_read = Instant::now();

        write_duration += after_write - before;
        read_duration += after_read - after_write;
        iterations += 1;

        if (after_read - last_check) > Duration::from_secs(1) {
            let write_throughput =
                (iterations * bytes_per_envelope) as f64 / write_duration.as_secs_f64();
            let write_throughput = write_throughput / 1024.0 / 1024.0;
            let read_throughput =
                (iterations * bytes_per_envelope) as f64 / read_duration.as_secs_f64();
            let read_throughput = read_throughput / 1024.0 / 1024.0;
            println!("Write throughput: {write_throughput:.2} MiB / s");

            println!("Read throughput: {read_throughput:.2} MiB / s");
            write_duration = Duration::ZERO;
            read_duration = Duration::ZERO;
            iterations = 0;

            last_check = after_read;
        }
    }
}

fn mock_envelope(payload_size: usize, project_count: usize) -> Box<Envelope> {
    let project_key = (rand::random::<f64>() * project_count as f64) as u128;
    let bytes = Bytes::from(format!(
            "\
             {{\"event_id\":\"9ec79c33ec9942ab8353589fcb2e04dc\",\"dsn\":\"https://{:032x}:@sentry.io/42\"}}\n\
             {{\"type\":\"attachment\"}}\n\
             {}\n\
             ",
            project_key,
            "X".repeat(payload_size)
        ));

    let mut envelope = Envelope::parse_bytes(bytes).unwrap();
    envelope.set_received_at(Utc::now());
    envelope
}
