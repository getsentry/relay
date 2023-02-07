use relay_general::pii::{DataScrubbingConfig, PiiConfig};
use relay_replays::recording::ReplayScrubber;
use std::fs;
use std::sync::mpsc::channel;
use threadpool::ThreadPool;

enum ScrubbingResult {
    Scrubbed,
    NotScrubbed,
    Skipped,
}

fn scrubbing_function<R, W>(input: R, output: W)
where
    R: std::io::Read,
    W: std::io::Write,
{
    let binding = default_pii_config();
    let mut scrubber = ReplayScrubber::new(usize::MAX, Some(&binding), None);

    scrubber.scrub_replay(input, output).unwrap();

    fn default_pii_config() -> PiiConfig {
        let mut scrubbing_config = DataScrubbingConfig::default();
        scrubbing_config.scrub_data = true;
        scrubbing_config.scrub_defaults = true;
        scrubbing_config.scrub_ip_addresses = true;
        scrubbing_config.pii_config_uncached().unwrap().unwrap()
    }
}

fn main() {
    let paths = fs::read_dir("./input").unwrap();
    let n_workers = num_cpus::get();
    let pool = ThreadPool::new(n_workers);
    let mut num_paths = 0;
    let (tx, rx) = channel();

    for path in paths {
        num_paths += 1;
        let tx = tx.clone();

        pool.execute(move || {
            let path = path.unwrap().path();
            let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
            let path_name = path.to_string_lossy().into_owned();

            if file_name == ".DS_Store" {
                tx.send(ScrubbingResult::Skipped)
                    .expect("channel will be there waiting for the pool");
                return;
            }

            let file_contents =
                fs::read_to_string(path_name).expect("Should have been able to read the file");

            let replay_data_str = file_contents.as_str();

            let mut transcoded = Vec::new();

            scrubbing_function(replay_data_str.as_bytes(), &mut transcoded);

            let parsed = std::str::from_utf8(&transcoded).unwrap();

            if parsed != replay_data_str {
                println!("scrubing-modified {file_name}");
                writefile(&file_name, parsed).expect("file should have been written");
                tx.send(ScrubbingResult::Scrubbed)
                    .expect("channel will be there waiting for the pool");
                // scrubbed += 1;
            } else {
                println!("scrubing did not modify {file_name}");
                tx.send(ScrubbingResult::NotScrubbed)
                    .expect("channel will be there waiting for the pool");
            }
        });
    }

    let mut scrubbed = 0;
    let mut not_scrubbed = 0;
    rx.iter().take(num_paths).for_each(|result| match result {
        ScrubbingResult::Scrubbed => scrubbed += 1,
        ScrubbingResult::NotScrubbed => not_scrubbed += 1,
        ScrubbingResult::Skipped => (),
    });

    println!("Finished running scrubbing! Scrubbed: {scrubbed}, Not Scrubbed: {not_scrubbed}");
}

fn writefile(name: &str, s: &str) -> std::io::Result<()> {
    use std::fs::File;
    use std::io::prelude::*;
    let mut path: String = "./output/".to_owned();
    path.push_str(name);
    let mut file = File::create(path)?;
    file.write_all(s.as_bytes())?;
    Ok(())
}
