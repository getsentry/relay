extern crate chrono;
extern crate clap;
extern crate console;
extern crate dialoguer;
extern crate env_logger;
extern crate failure;
extern crate futures;
#[cfg(not(windows))]
extern crate openssl_probe;
extern crate pretty_env_logger;
extern crate serde;
extern crate serde_json;
extern crate uuid;

#[macro_use]
extern crate log;
#[macro_use]
extern crate sentry;
#[macro_use]
extern crate serde_derive;

extern crate semaphore_common;
extern crate semaphore_server;

mod cli;
mod cliapp;
mod setup;
mod utils;

use sentry::Hub;
use std::env;
use std::process;

pub fn main() {
    // on non windows machines we want to initialize the openssl envvars based on
    // what openssl probe tells us.  We will eventually stop doing that if we
    // kill openssl.
    #[cfg(not(windows))]
    {
        use openssl_probe::init_ssl_cert_env_vars;
        init_ssl_cert_env_vars();
    }

    let mut exit_code = 0;

    // only print backtrace in console if we were requested before the
    // start of the app.  The envvar is overwritten by our own internal
    // sentry integration later.
    let console_bt = match env::var("RUST_BACKTRACE").as_ref().map(|x| x.as_str()) {
        Ok("1") | Ok("full") => true,
        _ => false,
    };

    if let Err(err) = cli::execute() {
        exit_code = 1;
        println!("error: {}", err);
        for cause in err.iter_causes() {
            println!("  caused by: {}", cause);
        }
        if console_bt {
            let bt = err.backtrace();
            println!();
            println!("{}", bt);
        } else if cfg!(debug_assertions) {
            println!();
            println!("hint: you can set RUST_BACKTRACE=1 to get the entire backtrace.");
        }
    };

    Hub::current().client().map(|x| x.close(None));
    process::exit(exit_code);
}
