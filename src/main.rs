extern crate clap;
extern crate failure;
extern crate smith_server;

use failure::Fail;
use clap::App;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const ABOUT: &'static str = "Test";

pub fn execute() {
    let app = App::new("sentry-agent")
        .help_message("Print this help message.")
        .version(VERSION)
        .version_message("Print version information.")
        .about(ABOUT);
}

pub fn main() {
    if let Err(err) = smith_server::run() {
        println!("error: {}", err);
        for cause in err.causes().skip(1) {
            println!("  caused by: {}", cause);
        }
        if let Some(bt) = err.backtrace() {
            println!("{}", bt);
        }
    }
}
