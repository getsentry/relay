mod cli;
mod cliapp;
mod env_arg_override;
mod setup;
mod utils;

use sentry::Hub;
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

    let exit_code = match cli::execute() {
        Ok(()) => 0,
        Err(err) => {
            cli::ensure_log_error(&err);
            1
        }
    };

    Hub::current().client().map(|x| x.close(None));
    process::exit(exit_code);
}
