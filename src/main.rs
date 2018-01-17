extern crate smith_server;
extern crate failure;

use failure::Fail;

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
