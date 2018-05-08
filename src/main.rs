extern crate clap;
extern crate ctrlc;
extern crate failure;
extern crate futures;
#[macro_use]
extern crate log;
extern crate parking_lot;
extern crate pretty_env_logger;
extern crate sentry;

extern crate smith_common;
extern crate smith_config;
extern crate smith_server;

mod setup;
mod cli;

use std::env;

pub fn main() {
    if let Err(err) = cli::execute() {
        println!("error: {}", err);
        for cause in err.causes().skip(1) {
            println!("  caused by: {}", cause);
        }
        match env::var("RUST_BACKTRACE").as_ref().map(|x| x.as_str()) {
            Ok("1") | Ok("full") => {
                let bt = err.backtrace();
                println!("");
                println!("{}", bt);
            }
            _ => if cfg!(debug_assertions) {
                println!("");
                println!("hint: you can set RUST_BACKTRACE=1 to get the entire backtrace.");
            },
        }
    }
}
