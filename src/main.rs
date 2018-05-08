extern crate clap;
extern crate ctrlc;
extern crate dialoguer;
extern crate failure;
extern crate futures;
#[macro_use]
extern crate log;
extern crate parking_lot;
extern crate pretty_env_logger;
extern crate sentry;

extern crate semaphore_common;
extern crate semaphore_config;
extern crate semaphore_server;

mod setup;
mod cli;
mod utils;

use std::env;

pub fn main() {
    // only print backtrace in console if we were requested before the
    // start of the app.  The envvar is overwritten by our own internal
    // sentry integration later.
    let console_bt = match env::var("RUST_BACKTRACE").as_ref().map(|x| x.as_str()) {
        Ok("1") | Ok("full") => true,
        _ => false,
    };

    if let Err(err) = cli::execute() {
        println!("error: {}", err);
        for cause in err.causes().skip(1) {
            println!("  caused by: {}", cause);
        }
        if console_bt {
            let bt = err.backtrace();
            println!("");
            println!("{}", bt);
        } else if cfg!(debug_assertions) {
            println!("");
            println!("hint: you can set RUST_BACKTRACE=1 to get the entire backtrace.");
        }
    }
}
