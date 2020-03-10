extern crate structopt;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
/// exastash
enum Command {
    #[structopt(name = "ls")]
    /// List files in a stash directory
    Ls {
        /// Path
        #[structopt(name = "PATH")]
        path: String,

        #[structopt(short = "j")]
        /// Print just the filenames
        just_names: bool,
    },
}

fn main() {
    match Command::from_args() {
        Command::Ls { path, just_names } => {
            dbg!(path, just_names);
        }
    }
}
