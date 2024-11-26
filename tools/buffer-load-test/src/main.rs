use rand::{rngs::StdRng, Rng, RngCore, SeedableRng};
use reqwest::Client;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

const NUM_PROJECTS: usize = 10;
const DEFAULT_DURATION_SECS: u64 = 60;
const CONCURRENT_TASKS: usize = 10;
const ENVELOPE_POOL_SIZE: usize = 100;
const DEFAULT_REQUESTS_PER_SECOND: f64 = 2500.0;
const DEFAULT_SEED: u64 = 12345;
const TINY_PAYLOAD: usize = 830; // 830 bytes
const SMALL_PAYLOAD: usize = 18 * 1024; // 18 KiB
const MEDIUM_PAYLOAD: usize = 100 * 1024; // 100 KiB
const LARGE_PAYLOAD: usize = 20 * 1024 * 1024; // 20 MiB

/// Generate random project pairs
fn generate_project_pairs(rng: &mut impl RngCore, count: usize) -> Vec<(String, u64)> {
    (0..count)
        .map(|_| {
            (
                format!("{:032x}", rng.gen::<u128>()),
                rng.gen_range(0..NUM_PROJECTS) as u64,
            )
        })
        .collect()
}

/// Pre-generate a pool of envelopes
fn generate_envelope_pool(rng: &mut impl RngCore) -> Vec<Vec<u8>> {
    let mut pool = Vec::with_capacity(ENVELOPE_POOL_SIZE);

    for _ in 0..ENVELOPE_POOL_SIZE {
        // Generate random number between 0 and 10000 for probability distribution
        let rand_val = rng.gen_range(0..10000);

        // Implement probability distribution
        let payload_size = match rand_val {
            0..=7500 => TINY_PAYLOAD,      // 75.00% - 830 bytes
            7501..=9900 => SMALL_PAYLOAD,  // 24.00% - 18 KiB
            9901..=9990 => MEDIUM_PAYLOAD, // 0.90% - 100 KiB
            _ => LARGE_PAYLOAD,            // 0.10% - 20 MiB
        };

        // Generate random payload
        let mut payload = vec![0u8; payload_size];
        rng.fill_bytes(&mut payload);
        pool.push(payload);
    }
    pool
}

async fn worker_thread(
    client: Client,
    envelope_pool: Arc<Vec<Vec<u8>>>,
    project_pairs: Arc<Vec<(String, u64)>>,
    counter: Arc<Mutex<usize>>,
    duration: Duration,
    requests_per_second: f64,
    worker_seed: u64,
) {
    let mut rng = StdRng::seed_from_u64(worker_seed);
    let start_time = Instant::now();
    let mut local_count = 0;

    // Calculate the delay between requests based on the desired rate
    let delay = if requests_per_second.is_finite() {
        Some(Duration::from_secs_f64(1.0 / requests_per_second))
    } else {
        None
    };

    let mut next_send_time = Instant::now();

    while start_time.elapsed() < duration {
        // Wait until it's time to send the next request
        if let Some(delay) = delay {
            let now = Instant::now();
            if now < next_send_time {
                tokio::time::sleep(next_send_time - now).await;
            }
            next_send_time = now + delay;
        }

        // Select random project and payload
        let (project_key, project_id) = &project_pairs[rng.gen_range(0..project_pairs.len())];
        let payload = &envelope_pool[rng.gen_range(0..envelope_pool.len())];

        // Build envelope header on the fly
        let header = json!({
            "event_id": format!("{:032x}", rng.gen::<u128>()),
            "dsn": format!("https://{}:@sentry.io/{}", project_key, project_id),
            "project_id": project_id
        });

        // Format envelope
        let mut envelope = format!(
            "{}\n{}\n",
            header.to_string(),
            json!({
                "type": "attachment",
                "length": payload.len()
            })
        )
        .into_bytes();
        envelope.extend(payload);

        let response = client
            .post(format!(
                "http://localhost:3000/api/{}/envelope/",
                project_id
            ))
            .body(envelope)
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
    let seed = std::env::var("SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SEED);

    println!("Using random seed: {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let duration_secs = std::env::var("DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    let duration = Duration::from_secs(duration_secs);

    let requests_per_second = std::env::var("REQUESTS_PER_SECOND")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_REQUESTS_PER_SECOND);

    println!(
        "Starting load test with {} projects for {} seconds using {} concurrent tasks",
        NUM_PROJECTS, duration_secs, CONCURRENT_TASKS
    );
    if requests_per_second.is_finite() {
        println!(
            "Rate limit: {:.1} requests/second per worker ({:.1} total)",
            requests_per_second,
            requests_per_second * CONCURRENT_TASKS as f64
        );
    } else {
        println!("Rate limit: unlimited");
    }

    // Generate projects and envelope pool using seeded RNG
    let project_pairs = Arc::new(generate_project_pairs(&mut rng, NUM_PROJECTS));
    let envelope_pool = Arc::new(generate_envelope_pool(&mut rng));
    let request_counter = Arc::new(Mutex::new(0));

    let start_time = Instant::now();
    let mut handles = Vec::new();

    // Spawn worker threads with different seeds derived from the main seed
    for worker_id in 0..CONCURRENT_TASKS {
        let client = Client::new();
        let pool = Arc::clone(&envelope_pool);
        let pairs = Arc::clone(&project_pairs);
        let counter = Arc::clone(&request_counter);
        let worker_seed = seed.wrapping_add(worker_id as u64);

        handles.push(tokio::task::spawn_blocking(move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(worker_thread(
                    client,
                    pool,
                    pairs,
                    counter,
                    duration,
                    requests_per_second,
                    worker_seed,
                ))
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
