use anyhow::{anyhow, bail, Result};
use structopt::StructOpt;
use exastash::db;
use chrono::Utc;
use postgres::Transaction;
use crate::db::inode::Inode;
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

    #[structopt(name = "info")]
    /// Show info for a dir, file, or symlink
    Info {
        #[structopt(flatten)]
        selector: InodeSelector,
    },

    #[structopt(name = "ls")]
    /// List a dir, file, or symlink
    Ls {
        #[structopt(short = "j")]
        /// Print just the filenames
        just_names: bool,

        #[structopt(flatten)]
        selector: InodeSelector,
    }
}

#[derive(StructOpt, Debug)]
struct InodeSelector {
    /// directory id.
    /// Only one of the four (dir, file, symlink, path) can be given.
    #[structopt(long, short = "d")]
    dir: Option<i64>,

    /// file id.
    /// Only one of the four (dir, file, symlink, path) can be given.
    #[structopt(long, short = "f")]
    file: Option<i64>,

    /// symlink id.
    /// Only one of the four (dir, file, symlink, path) can be given.
    #[structopt(long, short = "s")]
    symlink: Option<i64>,

    /// Path consisting only of slash-separated basenames, no leading / or . or ..
    /// Only one of the four (dir, file, symlink, path) can be given.
    #[structopt(long, short = "p")]
    path: Option<String>,

    /// A directory id specifying the dir from which to start path traversal.
    /// Must be provided if path is provided.
    #[structopt(long, short = "r")]
    root: Option<i64>,
}

impl InodeSelector {
    fn to_inode(&self, transaction: &mut Transaction<'_>) -> Result<Inode> {
        let inode = match (self.dir.or(self.file).or(self.symlink), &self.path) {
            (Some(_), None) => {
                let inode = db::dirent::InodeTuple(self.dir, self.file, self.symlink).to_inode()?;
                inode
            },
            (None, Some(path)) => {
                let root = self.root.ok_or_else(|| anyhow!("If path is specified, root dir id must also be specified"))?;
                let base_inode = db::inode::Inode::Dir(root);
                let path_components: Vec<&str> = if path == "" {
                    vec![]
                } else {
                    path.split('/').collect()
                };
                let inode = walk_path(transaction, base_inode, &path_components)?;
                inode
            },
            _ => {
                bail!("Either dir|file|symlink or path must be specified but not both");
            }
        };
        Ok(inode)
    }
}

#[derive(StructOpt, Debug)]
enum DirCommand {
    /// Create an unparented directory (for e.g. use as a root inode) and print its id to stdout
    #[structopt(name = "create")]
    Create,
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
            }
        },
        ExastashCommand::File(file) => {
        },
        ExastashCommand::Symlink(symlink) => {
        },
        ExastashCommand::Dirent(dirent) => {
            match dirent {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let child = db::dirent::InodeTuple(child_dir, child_file, child_symlink).to_inode()?;
                    let dirent = db::dirent::Dirent { basename, child };
                    let parent = db::inode::Inode::Dir(parent_dir_id);
                    db::dirent::create_dirent(&mut transaction, parent, &dirent)?;
                    transaction.commit()?;
                }
            }
        },
        ExastashCommand::Ls { just_names, selector } => {
            let inode = selector.to_inode(&mut transaction)?;
            let rows = db::dirent::list_dir(&mut transaction, inode)?;
            for row in rows {
                dbg!(row.basename);
            }
        },
        ExastashCommand::Info { selector } => {
            let inode = selector.to_inode(&mut transaction)?;
            // TODO
        },
    };

    Ok(())
}
