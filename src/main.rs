use anyhow::{anyhow, bail, Result};
use structopt::StructOpt;
use exastash::db;
use chrono::Utc;
use postgres::Transaction;
use crate::db::storage;
use crate::db::inode::{InodeId, Inode, File};
use crate::db::traversal::walk_path;

#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
#[structopt(help_message = "Print help information")]
#[structopt(version_message = "Print version information")]
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
    },

    #[structopt(name = "find")]
    /// Recursively list a dir, like `find`
    /// This cannot start at a file or symlink because it may have multiple names.
    Find {
        #[structopt(flatten)]
        selector: InodeSelector,
    },
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
    fn to_inode_id(&self, transaction: &mut Transaction<'_>) -> Result<InodeId> {
        let inode = match (self.dir.or(self.file).or(self.symlink), &self.path) {
            (Some(_), None) => {
                db::dirent::InodeTuple(self.dir, self.file, self.symlink).to_inode_id()?
            },
            (None, Some(path)) => {
                let root = self.root.ok_or_else(|| anyhow!("If path is specified, root dir id must also be specified"))?;
                let path_components: Vec<&str> = if path == "" {
                    vec![]
                } else {
                    path.split('/').collect()
                };
                walk_path(transaction, root, &path_components)?
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

fn find(transaction: &mut Transaction, segments: &[&str], dir_id: i64) -> Result<()> {
    let path_string = match segments {
        [] => "".into(),
        parts => format!("{}/", parts.join("/")),
    };
    let dirents = db::dirent::list_dir(transaction, dir_id)?;
    for dirent in dirents {
        println!("{}{}", path_string, dirent.basename);
        if let InodeId::Dir(dir_id) = dirent.child {
            let segments = [segments, &[&dirent.basename]].concat();
            find(transaction, &segments, dir_id)?;
        }
    }
    Ok(())
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
                    let dir_id = db::inode::NewDir { mtime, birth }.create(&mut transaction)?;
                    transaction.commit()?;
                    println!("{}", dir_id);
                }
            }
        }
        ExastashCommand::File(_file) => {
        }
        ExastashCommand::Symlink(_symlink) => {
        }
        ExastashCommand::Dirent(dirent) => {
            match dirent {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let child = db::dirent::InodeTuple(child_dir, child_file, child_symlink).to_inode_id()?;
                    let dirent = db::dirent::Dirent::new(parent_dir_id, basename, child);
                    dirent.create(&mut transaction)?;
                    transaction.commit()?;
                }
            }
        }
        ExastashCommand::Ls { just_names, selector } => {
            let inode_id = selector.to_inode_id(&mut transaction)?;
            let dirents = db::dirent::list_dir(&mut transaction, inode_id.dir_id()?)?;
            for dirent in dirents {
                if just_names {
                    println!("{}", dirent.basename);
                } else {
                    // TODO: print: size, mtime, filename[decoration]
                    println!("{}", dirent.basename);
                }
            }
        }
        ExastashCommand::Find { selector } => {
            let dir_id = selector.to_inode_id(&mut transaction)?.dir_id()?;
            find(&mut transaction, &[], dir_id)?;
        }
        ExastashCommand::Info { selector } => {
            let inode_id = selector.to_inode_id(&mut transaction)?;

            let inodes = Inode::find_by_inode_ids(&mut transaction, &[inode_id])?;
            assert!(inodes.len() <= 1);
            if inodes.is_empty() {
                bail!("inode {:?} does not exist in database", inode_id);
            }
            let inode = inodes.get(0).unwrap();
            println!("{:#?}", inode);

            // Only files may have storages
            if let Inode::File(File { id, .. }) = inode {
                for s in storage::get_storage(&mut transaction, &[*id])?.iter() {
                    println!("{:#?}", s);
                }
            }
        }
    };

    Ok(())
}
