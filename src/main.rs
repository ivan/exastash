use anyhow::{anyhow, bail, Result};
use structopt::StructOpt;
use exastash::db;
use chrono::Utc;
use crate::db::traversal::walk_path;

#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
/// exastash
enum ExastashCommand {
    /// Subcommands to work with dirs
    #[structopt(name = "dir")]
    Dir(DirCommand),

    /// Subcommands to work with files
    #[structopt(name = "file")]
    File(FileCommand),

    /// Subcommands to work with symlinks
    #[structopt(name = "symlink")]
    Symlink(SymlinkCommand),

    /// Subcommands to work with dirents
    #[structopt(name = "dirent")]
    Dirent(DirentCommand),
}

#[derive(StructOpt, Debug)]
enum DirCommand {
    /// Create an unparented directory (for e.g. use as a root inode) and print its id to stdout
    #[structopt(name = "create")]
    Create,

    #[structopt(name = "ls")]
    /// List files in a stash directory
    Ls {
        #[structopt(short = "j")]
        /// Print just the filenames
        just_names: bool,

        /// Directory id to list.
        /// Either this or path must be provided, not both.
        #[structopt(long, short = "d")]
        dir_id: Option<i64>,

        /// Path to list (path = slash-separated basenames).
        /// Either this or directory id must be provided, not both.
        #[structopt(long, short = "p")]
        path: Option<String>,

        /// Treat this as the root directory for path traversal
        /// Must be provided if path is provided.
        #[structopt(long, short = "r")]
        root: Option<i64>,
    }
}

#[derive(StructOpt, Debug)]
enum FileCommand {

}

#[derive(StructOpt, Debug)]
enum SymlinkCommand {

}

#[derive(StructOpt, Debug)]
enum DirentCommand {
    /// Create a dirent
    #[structopt(name = "create")]
    Create {
        #[structopt(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[structopt(name = "BASENAME")]
        basename: String,

        #[structopt(long, short = "d")]
        child_dir: Option<i64>,

        #[structopt(long, short = "f")]
        child_file: Option<i64>,

        #[structopt(long, short = "s")]
        child_symlink: Option<i64>,
    },
}

fn main() -> Result<()> {
    let mut client = db::postgres_client_production()?;
    let mut transaction = db::start_transaction(&mut client)?;
    match ExastashCommand::from_args() {
        ExastashCommand::Dir(dir) => {
            match dir {
                DirCommand::Create => {
                    let mtime = Utc::now();
                    let birth = db::inode::Birth::here_and_now();
                    let inode = db::inode::create_dir(&mut transaction, mtime, &birth)?;
                    transaction.commit()?;
                    println!("{}", inode.dir_id()?);
                },
                DirCommand::Ls { just_names, dir_id, path, root } => {
                    match (dir_id, path) {
                        (Some(id), None) => {
                            let inode = db::inode::Inode::Dir(id);
                            let rows = db::dirent::list_dir(&mut transaction, inode)?;
                            for row in rows {
                                dbg!(row.basename);
                            }
                        },
                        (None, Some(path)) => {
                            let base_dir_id = root.ok_or_else(|| anyhow!("If specifying path, a root dir id must also be provided"))?;
                            let base_inode = db::inode::Inode::Dir(base_dir_id);
                            let path_components: Vec<&str> = if path == "" {
                                vec![]
                            } else {
                                path.split('/').collect()
                            };
                            let inode = walk_path(&mut transaction, base_inode, &path_components)?;
                            let rows = db::dirent::list_dir(&mut transaction, inode)?;
                            for row in rows {
                                dbg!(row.basename);
                            }
                        },
                        _ => {
                            bail!("One of id or path must be specified");
                        },
                    }
                },
            }
        },
        ExastashCommand::File(file) => {
        },
        ExastashCommand::Symlink(symlink) => {
        },
        ExastashCommand::Dirent(dirent) => {
            match dirent {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let child = db::dirent::InodeTuple(child_dir, child_file, child_symlink).to_inode();
                    let dirent = db::dirent::Dirent { basename, child };
                    let parent = db::inode::Inode::Dir(parent_dir_id);
                    db::dirent::create_dirent(&mut transaction, parent, &dirent)?;
                    transaction.commit()?;
                }
            }
        },
    }
    Ok(())
}
