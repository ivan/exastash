extern crate structopt;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "ts")]
/// terastash
enum Opt {
    #[structopt(name = "add")]
    /// Add a file to the stash
    Add {
        #[structopt(short = "d", long = "drop-old-if-different")]
        /// If path already exists in the db, and (mtime, size, executable) of
        /// new file is different, drop the old file instead of throwing 'already exists'
        drop_old_if_different: bool,

        #[structopt(short = "c", long = "continue-on-exists")]
        /// Keep going on 'already exists' errors
        continue_on_exists: bool,

        #[structopt(long = "ignore-mtime")]
        /// Ignore mtime when checking that local file is equivalent to db file
        ignore_mtime: bool,
    },

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
