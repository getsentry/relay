use rand::{random, thread_rng, Rng, RngCore};
use reqwest::Client;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

const NUM_PROJECTS: usize = 100000;
const MIN_PAYLOAD_SIZE: usize = 300;
const MAX_PAYLOAD_SIZE: usize = 10_000;
const DEFAULT_DURATION_SECS: u64 = 1;
const CONCURRENT_TASKS: usize = 32; // Number of concurrent tasks
const ENVELOPE_POOL_SIZE: usize = 10000; // Number of pre-generated envelopes

/// Creates a mock envelope with random payload size
fn create_envelope(project_key: &str, project_id: u64, payload_size: usize) -> Vec<u8> {
    // Create envelope header with project DSN and project_id
    let header = json!({
        "event_id": "9ec79c33ec9942ab8353589fcb2e04dc",
        "dsn": format!("https://{}:@sentry.io/{}", project_key, project_id),
        "project_id": project_id
    });

    // Generate random payload
    let mut payload = vec![0u8; payload_size];
    thread_rng().fill_bytes(&mut payload);

    // Format envelope following Sentry protocol
    let mut envelope = format!(
        "{}\n{}\n",
        header.to_string(),
        json!({
            "type": "attachment",
            "length": payload_size
        })
    )
    .into_bytes();

    // Add payload
    envelope.extend(payload);
    envelope
}

/// Generate random project pairs
fn generate_project_pairs(count: usize) -> Vec<(String, u64)> {
    let mut rng = thread_rng();
    (0..count)
        .map(|_| {
            (
                format!("{:032x}", random::<u128>()),
                rng.gen_range(0..NUM_PROJECTS) as u64,
            )
        })
        .collect()
}

/// Pre-generate a pool of envelopes
fn generate_envelope_pool(project_pairs: &[(String, u64)]) -> Vec<(u64, Vec<u8>)> {
    let mut pool = Vec::with_capacity(ENVELOPE_POOL_SIZE);
    let mut rng = thread_rng();

    for _ in 0..ENVELOPE_POOL_SIZE {
        let (project_key, project_id) = &project_pairs[rng.gen_range(0..project_pairs.len())];
        let payload_size = rng.gen_range(MIN_PAYLOAD_SIZE..=MAX_PAYLOAD_SIZE);
        let envelope = create_envelope(project_key, *project_id, payload_size);
        pool.push((*project_id, envelope));
    }
    pool
}

async fn worker_thread(
    client: Client,
    envelope_pool: Arc<Vec<(u64, Vec<u8>)>>,
    counter: Arc<Mutex<usize>>,
    duration: Duration,
) {
    let start_time = Instant::now();
    let mut local_count = 0;
    let mut rng = thread_rng();

    while start_time.elapsed() < duration {
        let (project_id, envelope) = &envelope_pool[rng.gen_range(0..envelope_pool.len())];

        let response = client
            .post(format!(
                "http://localhost:3000/api/{}/envelope/",
                project_id
            ))
            .body(envelope.clone())
            .header("content-type", "application/x-sentry-envelope")
            .send()
            .await
            .unwrap();

        local_count += 1;
        if local_count % 100 == 0 {
            let mut total_count = counter.lock().await;
            *total_count += 100;
            println!(
                "Total requests: {}. Last response status: {}. Time elapsed: {:.1}s",
                *total_count,
                response.status(),
                start_time.elapsed().as_secs_f64()
            );
        }
    }

    // Add remaining counts
    let mut total_count = counter.lock().await;
    *total_count += local_count % 100;
}

#[tokio::main]
async fn main() {
    let duration_secs = std::env::var("DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    let duration = Duration::from_secs(duration_secs);

    println!(
        "Starting load test with {} projects for {} seconds using {} concurrent tasks",
        NUM_PROJECTS, duration_secs, CONCURRENT_TASKS
    );

    // Generate projects and envelope pool
    let project_pairs = generate_project_pairs(NUM_PROJECTS);
    let envelope_pool = Arc::new(generate_envelope_pool(&project_pairs));
    let request_counter = Arc::new(Mutex::new(0));

    let start_time = Instant::now();
    let mut handles = Vec::new();

    // Spawn worker threads
    for _ in 0..CONCURRENT_TASKS {
        let client = Client::new();
        let pool = Arc::clone(&envelope_pool);
        let counter = Arc::clone(&request_counter);

        handles.push(tokio::task::spawn_blocking(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(worker_thread(client, pool, counter, duration))
        }));
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start_time.elapsed();
    let total_requests = *request_counter.lock().await;
    let requests_per_second = total_requests as f64 / elapsed.as_secs_f64();

    println!("\nLoad test completed:");
    println!("Total requests: {}", total_requests);
    println!("Total time: {:.2} seconds", elapsed.as_secs_f64());
    println!("Throughput: {:.2} requests/second", requests_per_second);
    println!("Average latency: {:.2} ms", 1000.0 / requests_per_second);
}
