use anyhow::Result;
use structopt::StructOpt;
use exastash::db;
use chrono::Utc;

#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
/// exastash
enum Command {
    #[structopt(name = "create-dir")]
    /// Create an unparented directory (for e.g. use as a root inode) and print its id to stdout
    CreateDir,

    #[structopt(name = "ls")]
    /// List files in a stash directory
    Ls {
        #[structopt(short = "r")]
        /// Use this dir inode as the root of the filesystem
        root: i64,

        #[structopt(short = "j")]
        /// Print just the filenames
        just_names: bool,

        /// Path
        #[structopt(name = "PATH")]
        path: String,
    },
}

fn main() -> Result<()> {
    match Command::from_args() {
        Command::CreateDir => {
            let mut client = db::postgres_client_production()?;
            let mut transaction = db::start_transaction(&mut client)?;
            let mtime = Utc::now();
            let birth = db::inode::Birth::here_and_now();
            let inode = db::inode::create_dir(&mut transaction, mtime, &birth)?;
            println!("{}", inode.dir_id()?);
        },
        Command::Ls { root, just_names, path } => {
            let mut client = db::postgres_client_production()?;
            let mut transaction = db::start_transaction(&mut client)?;
            // TODO use path instead of root
            let inode = db::inode::Inode::Dir(root);
            let rows = db::dirent::list_dir(&mut transaction, inode)?;
            for row in rows {
                dbg!(row.basename);
            }
        }
    }
    Ok(())
}
