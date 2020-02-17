extern crate structopt;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
/// exastash
enum Opt {
    #[structopt(name = "ls")]
    /// List files in a stash directory
    Ls {
        #[structopt(short = "j")]
        /// Print just the filenames without any decoration
        just_names: bool,
    },
}

fn main() {
    let matches = Opt::from_args();

    println!("{:?}", matches);
}
