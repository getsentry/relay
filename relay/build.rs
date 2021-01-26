use clap::Shell;
use std::env;

mod cli {
    include!("src/cliapp.rs");
}

fn main() {
    let outdir = match env::var_os("OUT_DIR") {
        None => return,
        Some(outdir) => outdir,
    };

    let mut app = cli::make_app();
    app.gen_completions("relay", Shell::Bash, &outdir);
    app.gen_completions("relay", Shell::Zsh, &outdir);
    app.gen_completions("relay", Shell::Fish, &outdir);
    app.gen_completions("relay", Shell::PowerShell, &outdir);
}
