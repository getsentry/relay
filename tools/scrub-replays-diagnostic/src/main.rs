use std::fs;
use std::io::Error;
use std::io::{BufWriter, Write};
use std::sync::mpsc::channel;
use threadpool::ThreadPool;

enum ScrubbingStatus {
    Scrubbed,
    NotScrubbed,
    Skipped,
}

#[derive(Debug)]
struct ScrubbingResult {
    scrubbed: i32,
    not_scrubbed: i32,
}

impl ScrubbingResult {
    pub fn new() -> Self {
        Self {
            scrubbed: 0,
            not_scrubbed: 0,
        }
    }
    pub fn incr_scrubbed(self) -> Self {
        Self {
            scrubbed: self.scrubbed + 1,
            not_scrubbed: self.not_scrubbed,
        }
    }
    pub fn incr_not_scrubbed(self) -> Self {
        Self {
            scrubbed: self.scrubbed,
            not_scrubbed: self.not_scrubbed + 1,
        }
    }
}

fn identity_scrubbing_function<R, W>(input: R, output: W)
where
    R: std::io::Read,
    W: std::io::Write,
{
    let byte_iterator = input.bytes();
    let collected_bytes_result: Result<Vec<u8>, Error> = byte_iterator.collect();

    let input_bytes = collected_bytes_result.unwrap();

    let mut bufwriter = BufWriter::new(output);
    Write::write(&mut bufwriter, &input_bytes).unwrap();
}

fn scrubbing_function<R, W>(input: R, output: W)
where
    R: std::io::Read,
    W: std::io::Write,
{
    // a no-op scrubbing function
    identity_scrubbing_function(input, output);

    // replace with your scrubbing function
    // EG:
    // use relay_general::pii::{DataScrubbingConfig, PiiConfig};
    // use relay_replays::recording::ReplayScrubber;
    // let binding = default_pii_config();
    // let mut scrubber = ReplayScrubber::new(usize::MAX, Some(&binding), None);

    // scrubber.scrub_replay(input, output).unwrap();

    // fn default_pii_config() -> PiiConfig {
    //     let mut scrubbing_config = DataScrubbingConfig::default();
    //     scrubbing_config.scrub_data = true;
    //     scrubbing_config.scrub_defaults = true;
    //     scrubbing_config.scrub_ip_addresses = true;
    //     scrubbing_config.pii_config_uncached().unwrap().unwrap()
    // }
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

            tx.send(match file_name.as_str() {
                ".DS_Store" => ScrubbingStatus::Skipped,
                _ => {
                    let file_contents = fs::read_to_string(path_name).unwrap();
                    let replay_data_str = file_contents.as_str();
                    let mut transcoded = Vec::new();

                    scrubbing_function(replay_data_str.as_bytes(), &mut transcoded);

                    let parsed = std::str::from_utf8(&transcoded).unwrap();

                    if parsed != replay_data_str {
                        println!("scrubbing modified {file_name}");
                        writefile(&file_name, parsed).unwrap();
                        ScrubbingStatus::Scrubbed
                    } else {
                        println!("scrubbing did not modify {file_name}");
                        ScrubbingStatus::NotScrubbed
                    }
                }
            })
            .unwrap();
        });
    }

    let init = ScrubbingResult::new();

    let final_result =
        rx.iter()
            .take(num_paths)
            .fold(init, |acc, scrub_status| match scrub_status {
                ScrubbingStatus::Skipped => acc,
                ScrubbingStatus::NotScrubbed => acc.incr_not_scrubbed(),
                ScrubbingStatus::Scrubbed => acc.incr_scrubbed(),
            });
    println!(
        "Finished running scrubbing! Scrubbed: {}, Not Scrubbed: {}",
        final_result.scrubbed, final_result.not_scrubbed
    );
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
