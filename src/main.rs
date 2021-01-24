#![feature(format_args_capture)]

use tracing::info;
use yansi::Paint;
use async_recursion::async_recursion;
use clap::arg_enum;
use anyhow::{anyhow, bail, Result};
use structopt::StructOpt;
use chrono::Utc;
use tokio::fs;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;
use num::rational::Ratio;
use sqlx::{Postgres, Transaction};
use tracing_subscriber::EnvFilter;
use serde_json::json;
use exastash::db;
use exastash::db::storage::gdrive::{file::GdriveFile, GdriveFilePlacement};
use exastash::db::inode::{InodeId, Inode, File, Dir, NewDir, Symlink, NewSymlink};
use exastash::db::dirent::{Dirent, InodeTuple};
use exastash::db::google_auth::{GoogleApplicationSecret, GoogleServiceAccount};
use exastash::db::traversal;
use exastash::path;
use exastash::config;
use exastash::info::json_info;
use exastash::oauth;
use exastash::retry::Decayer;
use exastash::{storage_read, storage_write};
use yup_oauth2::ServiceAccountKey;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
#[structopt(help_message = "Print help information")]
#[structopt(version_message = "Print version information")]
/// exastash
enum ExastashCommand {
    /// Commands to work with directories
    #[structopt(name = "dir")]
    Dir(DirCommand),

    /// Commands to work with files
    #[structopt(name = "file")]
    File(FileCommand),

    /// Commands to work with symbolic links
    #[structopt(name = "symlink")]
    Symlink(SymlinkCommand),

    /// Commands to work with directory entries
    #[structopt(name = "dirent")]
    Dirent(DirentCommand),

    /// Commands to work with Google tokens and secrets
    #[structopt(name = "google")]
    Google(GoogleCommand),

    /// Commands to work with storage methods
    #[structopt(name = "storage")]
    Storage(StorageCommand),

    /// (nonfunctional) FUSE server
    #[structopt(name = "fuse")]
    Fuse(FuseCommand),

    /// Commands that operate based on paths relative to cwd. To resolve paths,
    /// exastash walks up to find a root directory that points to some stash
    /// dir inode. Root directories can be configured in ~/.config/exastash/config.toml
    #[structopt(name = "x")]
    Path(PathCommand),

    /// Print license information
    License,
}

#[derive(StructOpt, Debug)]
enum DirCommand {
    /// Create a new directory as a child of some directory and print its id to stdout
    #[structopt(name = "create")]
    Create {
        #[structopt(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[structopt(name = "BASENAME")]
        basename: String,
    },

    /// Remove an empty directory and its associated dirent where it is a child_dir
    #[structopt(name = "remove")]
    Remove {
        #[structopt(name = "DIR_ID")]
        dir_id: i64,
    },

    /// Print info in JSON format for zero or more dirs
    #[structopt(name = "info")]
    Info {
        /// dir id
        #[structopt(name = "ID")]
        ids: Vec<i64>,
    },

    /// Print a count of the number of dirs
    Count,
}

#[derive(StructOpt, Debug)]
enum FileCommand {
    /// Create an unparented file, based on a local file, and print its id to stdout
    #[structopt(name = "create")]
    Create {
        /// Local file from which content, mtime, and executable flag will be read
        #[structopt(name = "PATH")]
        path: String,

        /// Store the file data in the database itself. Can be specified with other --store-* options.
        #[structopt(long)]
        store_inline: bool,

        /// Store the file data in some google domain (specified by id).
        /// Can be specified multiple times and with other --store-* options.
        #[structopt(long)]
        store_gdrive: Vec<i16>,
    },

    /// Remove a file and its associated storages
    #[structopt(name = "remove")]
    Remove {
        #[structopt(name = "FILE_ID")]
        file_id: i64,
    },

    /// Print info in JSON format for zero or more dirs
    #[structopt(name = "info")]
    Info {
        /// file id
        #[structopt(name = "ID")]
        ids: Vec<i64>,
    },

    /// Commands for working with file content
    #[structopt(name = "content")]
    Content(ContentCommand),

    /// Print a count of the number of files
    Count,
}

#[derive(StructOpt, Debug)]
enum ContentCommand {
    /// Output a file's content to stdout
    #[structopt(name = "read")]
    Read {
        /// file id
        #[structopt(name = "ID")]
        id: i64,
    },
}

#[derive(StructOpt, Debug)]
enum SymlinkCommand {
    /// Create a symlink
    #[structopt(name = "create")]
    Create {
        #[structopt(name = "TARGET")]
        target: String,
    },

    /// Remove a symlink
    #[structopt(name = "remove")]
    Remove {
        #[structopt(name = "SYMLINK_ID")]
        symlink_id: i64,
    },

    /// Print info in JSON format for zero or more dirs
    #[structopt(name = "info")]
    Info {
        /// symlink id
        #[structopt(name = "ID")]
        ids: Vec<i64>,
    },

    /// Print a count of the number of symlinks
    Count,
}

arg_enum! {
    #[derive(Debug)]
    #[allow(non_camel_case_types)]
    enum ResolveKind {
        dir,
        file,
        symlink,
    }
}

#[derive(StructOpt, Debug)]
enum DirentCommand {
    /// Create a dirent. This does not follow the new_dirent_requirements set in config.toml.
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

    /// Remove a dirent. If dirent has a child_dir, use `es dir remove` instead.
    #[structopt(name = "remove")]
    Remove {
        #[structopt(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[structopt(name = "BASENAME")]
        basename: String,
    },

    /// List a dir's children in JSON format, for zero or more parent dirs
    #[structopt(name = "list")]
    List {
        /// dir id
        #[structopt(name = "ID")]
        ids: Vec<i64>,
    },

    /// Walk a dir recursively and print path info in JSON format
    #[structopt(name = "walk")]
    Walk {
        /// dir id
        #[structopt(name = "ID")]
        id: i64,
    },

    /// Resolve paths to dir, file, or symlink ids
    #[structopt(name = "resolve")]
    Resolve {
        /// Kind of entity to resolve. If a path resolves to another kind, it will be skipped.
        #[structopt(name = "KIND", possible_values = &ResolveKind::variants())]
        kind: ResolveKind,

        /// Dir id of root dir from which to resolve paths
        #[structopt(name = "ROOT_DIR_ID")]
        root: i64,

        /// Path consisting only of slash-separated basenames. There is no handling of
        /// '.', '..', duplicate '/', leading '/', or trailing '/'
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },

    /// Print a count of the number of dirents
    Count,
}


#[derive(StructOpt, Debug)]
enum ApplicationSecretCommand {
    /// Import an application secret from a .json file
    #[structopt(name = "import")]
    Import {
        #[structopt(name = "DOMAIN_ID")]
        domain_id: i16,

        #[structopt(name = "JSON_FILE")]
        json_file: String,
    },
}

#[derive(StructOpt, Debug)]
enum AccessTokenCommand {
    /// Create an OAuth 2.0 access token for an owner. Domain, owner,
    /// and application secret must already be in database.
    #[structopt(name = "create")]
    Create {
        #[structopt(name = "OWNER_ID")]
        owner_id: i32,
    },
}

#[derive(StructOpt, Debug)]
enum ServiceAccountCommand {
    /// Import a service account key from a .json file
    #[structopt(name = "import")]
    Import {
        #[structopt(name = "OWNER_ID")]
        owner_id: i32,

        #[structopt(name = "JSON_FILE")]
        json_file: String,
    },
}

#[derive(StructOpt, Debug)]
enum GoogleCommand {
    /// Manage OAuth 2.0 application secrets (used with the "installed" application flow)
    #[structopt(name = "app-secret")]
    ApplicationSecret(ApplicationSecretCommand),

    /// Manage OAuth 2.0 access tokens
    #[structopt(name = "access-token")]
    AccessToken(AccessTokenCommand),

    /// Manage Google service accounts
    #[structopt(name = "service-account")]
    ServiceAccount(ServiceAccountCommand),

    /// Run a loop that refreshes OAuth 2.0 access tokens every ~5 minutes
    #[structopt(name = "token-service")]
    TokenService,
}

#[derive(StructOpt, Debug)]
enum StorageCommand {
    /// gdrive storage
    #[structopt(name = "gdrive")]
    Gdrive(GdriveStorageCommand),
}

#[derive(StructOpt, Debug)]
enum GdriveStorageCommand {
    /// Internal commands for debugging
    #[structopt(name = "internal")]
    Internal(InternalCommand),

    /// gdrive file placement commands
    #[structopt(name = "placement")]
    Placement(PlacementCommand),
}

#[derive(StructOpt, Debug)]
enum InternalCommand {
    /// Create an unencrypted/unaltered Google Drive file based on some local
    /// file and record it in the database. Output the info of the new gdrive
    /// file to stdout as JSON.
    #[structopt(name = "create-file")]
    CreateFile {
        /// Path to the local file to upload
        #[structopt(name = "PATH")]
        path: PathBuf,

        /// google_domain to upload to
        #[structopt(name = "DOMAIN_ID")]
        domain_id: i16,

        /// gdrive_owner to upload as
        #[structopt(name = "OWNER_ID")]
        owner_id: i32,

        /// Google Drive folder ID to create the file in
        #[structopt(name = "PARENT")]
        parent: String,

        /// Google Drive filename for the new file
        #[structopt(name = "FILENAME")]
        filename: String,
    },

    /// Read the contents of a sequence of Google Drive files to stdout.
    #[structopt(name = "read-files")]
    ReadFiles {
        /// google_domain to read from
        #[structopt(name = "DOMAIN_ID")]
        domain_id: i16,

        /// ID of the Google Drive file to read
        #[structopt(name = "FILE_ID")]
        file_ids: Vec<String>,
    },
}

#[derive(StructOpt, Debug)]
enum PlacementCommand {
    /// Print file placement info in JSON format
    #[structopt(name = "list")]
    List {
        /// google_domain for which to list file placement information
        #[structopt(name = "DOMAIN_ID")]
        domain_id: i16,
    },
}

#[derive(StructOpt, Debug)]
enum FuseCommand {
    /// Run a FUSE server
    #[structopt(name = "run")]
    Run {
        /// Where to mount the exastash root
        #[structopt(name = "MOUNTPOINT")]
        mountpoint: String,
    }
}

arg_enum! {
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    #[allow(non_camel_case_types)]
    enum FindKind {
        d, // dir
        f, // file
        s, // symlink
    }
}

arg_enum! {
    #[derive(Debug, PartialEq, Eq)]
    #[allow(non_camel_case_types)]
    enum ExistingFileBehavior {
        stop,
        skip,
        replace,
    }
}

#[derive(StructOpt, Debug)]
enum PathCommand {
    /// Print info in JSON format for a path's inode
    #[structopt(name = "info")]
    Info {
        /// Path to an inode to print info for, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },

    /// Write the contents of a file to stdout
    #[structopt(name = "cat")]
    Cat {
        /// Path to a file to cat, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },

    /// Retrieve a dir, file, or symlink to the local filesystem.
    /// Not recursive.
    #[structopt(name = "get")]
    Get {
        /// Path to get from stash, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,

        /// Skip retrieval if the file exists locally with a matching size and mtime
        #[structopt(long, short = "s")]
        skip_if_exists: bool,
    },

    /// Create a stash file based on a local file. This also makes local file
    /// read-only to make it more obviously immutable like the stash file.
    #[structopt(name = "add")]
    Add {
        /// Path to add to stash, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,

        /// What to do if a directory entry already exists at the corresponding stash path
        #[structopt(long, short = "e", default_value = "stop")]
        existing_file_behavior: ExistingFileBehavior,

        /// Remove each local file after successfully storing it and creating a dirent
        #[structopt(long)]
        remove_local_files: bool,
    },

    /// List a directory
    #[structopt(name = "ls")]
    Ls {
        /// Path to list, relative to cwd
        #[structopt(name = "PATH")]
        path: Option<String>,

        /// Whether to print just the filenames
        #[structopt(long, short = "j")]
        just_names: bool,

        /// By which field to sort the output
        #[structopt(long, default_value = "name")]
        sort: SortOrder,

        /// Whether to sort in reverse
        #[structopt(long, short = "r")]
        reverse: bool,
    },

    /// Recursively list a directory like findutils find
    #[structopt(name = "find")]
    Find {
        /// Path to list recursively, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,

        /// Limit output to paths pointing to inodes of this type (d = dir, f = file, s = symlink)
        #[structopt(long, short = "t", possible_values = &FindKind::variants(), case_insensitive = false)]
        r#type: Option<FindKind>,

        /// Print filenames separated by NULL instead of LF
        #[structopt(short = "0")]
        null_sep: bool,
    },

    /// Create a directory. This does not follow the new_dirent_requirements set in config.toml.
    #[structopt(name = "mkdir")]
    Mkdir {
        /// Directory path to create, relative to cwd. Parent directories are
        /// also created as needed. For your convenience, the same directories
        /// are also created in cwd.
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },

    /// Delete a directory entry. Also deletes the corresponding dir when removing
    /// a child_dir dirent. Does not delete files or symlinks, even when removing
    /// the last dirent to a file or symlink.
    #[structopt(name = "rm")]
    Rm {
        /// Path to a dirent to remove, relative to cwd.
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },
}

arg_enum! {
    #[derive(Debug, PartialEq, Eq)]
    #[allow(non_camel_case_types)]
    enum SortOrder {
        name,
        mtime,
        size,
    }
}

async fn resolve_path(transaction: &mut Transaction<'_, Postgres>, root: i64, path: &str) -> Result<InodeId> {
    let path_components: Vec<&str> = if path == "" {
        vec![]
    } else {
        path.split('/').collect()
    };
    traversal::resolve_inode(transaction, root, &path_components).await
}

#[async_recursion]
async fn walk_dir(transaction: &mut Transaction<'_, Postgres>, root: i64, segments: &[&str], dir_id: i64) -> Result<()> {
    let path_string = match segments {
        [] => "".into(),
        parts => format!("{}/", parts.join("/")),
    };
    let dirents = Dirent::find_by_parents(transaction, &[dir_id]).await?;
    for dirent in dirents {
        let j = json!({
            "root":       root,
            "path":       format!("{}{}", path_string, dirent.basename),
            "dir_id":     if let InodeId::Dir(id)     = dirent.child { Some(id) } else { None },
            "file_id":    if let InodeId::File(id)    = dirent.child { Some(id) } else { None },
            "symlink_id": if let InodeId::Symlink(id) = dirent.child { Some(id) } else { None },
        });
        println!("{j}");
        if let InodeId::Dir(dir_id) = dirent.child {
            let segments = [segments, &[&dirent.basename]].concat();
            walk_dir(transaction, root, &segments, dir_id).await?;
        }
    }
    Ok(())
}

#[async_recursion]
async fn x_find(
    transaction: &mut Transaction<'_, Postgres>,
    segments: &[&str],
    dir_id: i64,
    r#type: Option<FindKind>,
    terminator: char
) -> Result<()> {
    let path_string = match segments {
        [] => "".into(),
        parts => format!("{}/", parts.join("/")),
    };
    let dirents = Dirent::find_by_parents(transaction, &[dir_id]).await?;
    for dirent in dirents {
        // No type filter means we output
        let mut do_output = false;
        if r#type.is_none() {
            do_output = true;
        } else {
            // Make sure the type matches
            match dirent.child {
                InodeId::Dir(_)     => if r#type == Some(FindKind::d) { do_output = true; },
                InodeId::File(_)    => if r#type == Some(FindKind::f) { do_output = true; },
                InodeId::Symlink(_) => if r#type == Some(FindKind::s) { do_output = true; },
            };
        }

        if do_output {
            print!("{}{}{}", path_string, dirent.basename, terminator);
        }

        if let InodeId::Dir(dir_id) = dirent.child {
            let segments = [segments, &[&dirent.basename]].concat();
            x_find(transaction, &segments, dir_id, r#type, terminator).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("warn"))
        .unwrap();
    let _subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    // Do this first for --help to work without a database connection
    let command = ExastashCommand::from_args();

    if let ExastashCommand::License = command {
        print!("{}", include_str!("../LICENSE"));
        return Ok(())
    }

    let mut pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    match command {
        ExastashCommand::License => {
            unreachable!();
        },
        ExastashCommand::Dir(command) => {
            match command {
                DirCommand::Create { parent_dir_id, basename } => {
                    let mtime = Utc::now();
                    let birth = db::inode::Birth::here_and_now();
                    let dir = NewDir { mtime, birth }.create(&mut transaction).await?;
                    Dirent::new(parent_dir_id, basename, InodeId::Dir(dir.id)).create(&mut transaction).await?;
                    transaction.commit().await?;
                    println!("{}", dir.id);
                }
                DirCommand::Remove { dir_id } => {
                    Dirent::remove_by_child_dir(&mut transaction, dir_id).await?;
                    Dir::remove(&mut transaction, &[dir_id]).await?;
                    transaction.commit().await?;
                }
                DirCommand::Info { ids } => {
                    let inode_ids: Vec<InodeId> = ids.into_iter().map(InodeId::Dir).collect();
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).ok_or_else(|| anyhow!("{:?} not found in database", inode_id))?;
                        println!("{}", json_info(&mut transaction, inode).await?);
                    }
                }
                DirCommand::Count => {
                    let count = Dir::count(&mut transaction).await?;
                    println!("{}", count);
                }
            }
        }
        ExastashCommand::File(command) => {
            match command {
                FileCommand::Create { path, store_inline, store_gdrive } => {
                    drop(transaction);
                    let desired_storage = storage_write::DesiredStorage { inline: store_inline, gdrive: store_gdrive };
                    let attr = fs::metadata(path.clone()).await?;
                    let metadata: storage_write::RelevantFileMetadata = attr.try_into()?;
                    let file_id = storage_write::write(path, &metadata, &desired_storage).await?;
                    println!("{}", file_id);
                }
                FileCommand::Remove { file_id } => {
                    db::storage::remove_storages(&mut transaction, &[file_id]).await?;
                    File::remove(&mut transaction, &[file_id]).await?;
                    transaction.commit().await?;
                }
                FileCommand::Info { ids } => {
                    let inode_ids: Vec<InodeId> = ids.into_iter().map(InodeId::File).collect();
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).ok_or_else(|| anyhow!("{:?} not found in database", inode_id))?;
                        println!("{}", json_info(&mut transaction, inode).await?);
                    }
                }
                FileCommand::Content(content) => {
                    match content {
                        ContentCommand::Read { id } => {
                            let (stream, _) = storage_read::read(id).await?;
                            let mut stdout = tokio::io::stdout();
                            storage_read::write_stream_to_sink(stream, &mut stdout).await?;
                        }
                    }
                }
                FileCommand::Count => {
                    let count = File::count(&mut transaction).await?;
                    println!("{}", count);
                }
            }
        }
        ExastashCommand::Symlink(command) => {
            match command {
                SymlinkCommand::Create { target } => {
                    let mtime = Utc::now();
                    let birth = db::inode::Birth::here_and_now();
                    let symlink = NewSymlink { mtime, birth, target }.create(&mut transaction).await?;
                    transaction.commit().await?;
                    println!("{}", symlink.id);
                }
                SymlinkCommand::Remove { symlink_id } => {
                    Symlink::remove(&mut transaction, &[symlink_id]).await?;
                    transaction.commit().await?;
                }
                SymlinkCommand::Info { ids } => {
                    let inode_ids: Vec<InodeId> = ids.into_iter().map(InodeId::Symlink).collect();
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).ok_or_else(|| anyhow!("{:?} not found in database", inode_id))?;
                        println!("{}", json_info(&mut transaction, inode).await?);
                    }
                }
                SymlinkCommand::Count => {
                    let count = Symlink::count(&mut transaction).await?;
                    println!("{}", count);
                }
            }
        }
        ExastashCommand::Dirent(command) => {
            match command {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let child = InodeTuple(child_dir, child_file, child_symlink).try_into()?;
                    Dirent::new(parent_dir_id, basename, child).create(&mut transaction).await?;
                    transaction.commit().await?;
                }
                DirentCommand::Remove { parent_dir_id, basename } => {
                    Dirent::remove_by_parent_basename(&mut transaction, parent_dir_id, &basename).await?;
                    transaction.commit().await?;
                }
                DirentCommand::List { ids } => {
                    let dirents = Dirent::find_by_parents(&mut transaction, &ids).await?;
                    for dirent in dirents {
                        let j = json!({
                            "parent":        dirent.parent,
                            "basename":      dirent.basename,
                            "child_dir":     if let InodeId::Dir(id)     = dirent.child { Some(id) } else { None },
                            "child_file":    if let InodeId::File(id)    = dirent.child { Some(id) } else { None },
                            "child_symlink": if let InodeId::Symlink(id) = dirent.child { Some(id) } else { None },
                        });
                        println!("{j}");
                    }
                }
                DirentCommand::Walk { id } => {
                    walk_dir(&mut transaction, id, &[], id).await?;
                }
                DirentCommand::Resolve { kind, root, paths } => {
                    for path in paths {
                        let inode = resolve_path(&mut transaction, root, &path).await?;
                        match kind {
                            ResolveKind::dir     => if let InodeId::Dir(id)     = inode { println!("{}", id) },
                            ResolveKind::file    => if let InodeId::File(id)    = inode { println!("{}", id) },
                            ResolveKind::symlink => if let InodeId::Symlink(id) = inode { println!("{}", id) },
                        }
                    }
                }
                DirentCommand::Count => {
                    let count = Dirent::count(&mut transaction).await?;
                    println!("{}", count);
                }
            }
        }
        ExastashCommand::Google(command) => {
            match command {
                GoogleCommand::ApplicationSecret(command) => {
                    match command {
                        ApplicationSecretCommand::Import { domain_id, json_file } => {
                            let content = fs::read(json_file).await?;
                            let json = serde_json::from_slice(&content)?;
                            GoogleApplicationSecret { domain_id, secret: json }.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GoogleCommand::AccessToken(command) => {
                    match command {
                        AccessTokenCommand::Create { owner_id } => {
                            oauth::create_access_token(transaction, owner_id).await?;
                        }
                    }
                }
                GoogleCommand::ServiceAccount(command) => {
                    match command {
                        ServiceAccountCommand::Import { owner_id, json_file } => {
                            let content = fs::read(json_file).await?;
                            let key: ServiceAccountKey = serde_json::from_slice(&content)?;
                            assert_eq!(key.key_type, Some("service_account".into()));
                            GoogleServiceAccount { owner_id, key }.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GoogleCommand::TokenService => {
                    drop(transaction);
                    let interval_sec = 305;
                    info!("will check access tokens every {} seconds", interval_sec);
                    loop {
                        oauth::refresh_access_tokens(&mut pool).await?;
                        tokio::time::sleep(std::time::Duration::new(interval_sec, 0)).await;
                    }
                }
            }
        }
        ExastashCommand::Storage(command) => {
            match command {
                StorageCommand::Gdrive(command) => {
                    match command {
                        GdriveStorageCommand::Placement(command) => {
                            match command {
                                PlacementCommand::List { domain_id } => {
                                    let placements = GdriveFilePlacement::find_by_domain(&mut transaction, domain_id, None).await?;
                                    for placement in placements {
                                        let j = serde_json::to_string(&placement)?;
                                        println!("{j}");
                                    }
                                }
                            }
                        }
                        GdriveStorageCommand::Internal(command) => {
                            match command {
                                InternalCommand::CreateFile { path, domain_id, owner_id, parent, filename } => {
                                    let attr = fs::metadata(&path).await?;
                                    let size = attr.len();

                                    let mut lfp = storage_write::LocalFileProducer::new(path.clone());
                                    lfp.set_read_size(65536);
                                    let gdrive_file = storage_write::create_gdrive_file_on_domain(
                                        lfp, size, domain_id, owner_id, &parent, &filename
                                    ).await?;
                                    gdrive_file.create(&mut transaction).await?;
                                    transaction.commit().await?;
                                    let j = serde_json::to_string_pretty(&gdrive_file)?;
                                    println!("{j}");
                                }
                                InternalCommand::ReadFiles { domain_id, file_ids } => {
                                    let gdrive_ids: Vec<&str> = file_ids.iter().map(String::as_str).collect();
                                    let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_ids).await?;
                                    for gdrive_file in &gdrive_files {
                                        let stream = Box::pin(storage_read::stream_gdrive_file(gdrive_file, domain_id).await?);
                                        let mut stdout = tokio::io::stdout();
                                        storage_read::write_stream_to_sink(stream, &mut stdout).await?;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        ExastashCommand::Fuse(command) => {
            match command {
                FuseCommand::Run { mountpoint: _ } => {
                    panic!("FUSE server was not built");
                    //fuse::run(mountpoint.into()).await?;
                }
            }
        }
        ExastashCommand::Path(command) => {
            match command {
                PathCommand::Info { paths: path_args } => {
                    let config = config::get_config()?;
                    let mut inode_ids = vec![];
                    for path_arg in path_args {
                        let inode_id = path::resolve_local_path_arg(&config, &mut transaction, Some(&path_arg)).await?;
                        inode_ids.push(inode_id);
                    }
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).unwrap();
                        println!("{}", json_info(&mut transaction, inode).await?);
                    }
                }
                PathCommand::Cat { paths: path_args } => {
                    let config = config::get_config()?;
                    let mut file_ids = vec![];
                    // Resolve all paths to inodes before doing the unpredictably-long read operations,
                    // during which files could be renamed.
                    for path_arg in path_args {
                        let file_id = path::resolve_local_path_arg(&config, &mut transaction, Some(&path_arg)).await?.file_id()?;
                        file_ids.push(file_id);
                    }
                    for file_id in file_ids {
                        let (stream, _) = storage_read::read(file_id).await?;
                        let mut stdout = tokio::io::stdout();
                        storage_read::write_stream_to_sink(stream, &mut stdout).await?;
                    }
                }
                PathCommand::Get { paths: path_args, skip_if_exists } => {
                    use std::os::unix::fs::PermissionsExt;

                    let config = config::get_config()?;
                    let mut retrievals = vec![];
                    // Resolve all paths to inodes before doing the unpredictably-long read operations,
                    // during which files could be renamed.
                    for path_arg in &path_args {
                        let inode_id = path::resolve_local_path_arg(&config, &mut transaction, Some(path_arg)).await?;
                        retrievals.push((inode_id, path_arg));
                    }
                    for (inode_id, path_arg) in retrievals {
                        match inode_id {
                            InodeId::Dir(_) => {
                                unimplemented!();
                            }
                            InodeId::File(file_id) => {
                                if skip_if_exists {
                                    match fs::metadata(path_arg).await {
                                        Err(err) => {
                                            if err.kind() != std::io::ErrorKind::NotFound {
                                                bail!(err);
                                            }
                                        }
                                        Ok(attr) => {
                                            let metadata: storage_write::RelevantFileMetadata = (&attr).try_into()?;
                                            let files = File::find_by_ids(&mut transaction, &[file_id]).await?;
                                            let file = files.get(0).ok_or_else(|| {
                                                anyhow!("database unexpectedly missing file id={}", file_id)
                                            })?;
                                            if file.mtime == metadata.mtime && file.size == metadata.size {
                                                info!("{:?} already exists locally with matching size and mtime", path_arg);

                                                let permissions = std::fs::Permissions::from_mode(
                                                    if file.executable { 0o770 } else { 0o660 }
                                                );
                                                fs::set_permissions(&path_arg, permissions).await?;

                                                continue;
                                            }
                                        }
                                    }
                                }

                                // Remove any existing file to reset permissions
                                if let Err(err) = tokio::fs::remove_file(&path_arg).await {
                                    if err.kind() != std::io::ErrorKind::NotFound {
                                        bail!(err);
                                    }
                                }

                                // TODO: do this properly and apply dir mtimes from the database
                                let path_buf = PathBuf::from(path_arg);
                                let dir_path = path_buf.parent().unwrap();
                                tokio::fs::create_dir_all(&dir_path).await?;

                                let mut local_file = tokio::fs::File::create(&path_arg).await?;
                                let (stream, file) = storage_read::read(file_id).await?;
                                storage_read::write_stream_to_sink(stream, &mut local_file).await?;

                                if file.executable {
                                    let permissions = std::fs::Permissions::from_mode(0o770);
                                    fs::set_permissions(&path_arg, permissions).await?;
                                }

                                let mtime = filetime::FileTime::from_system_time(file.mtime.into());
                                filetime::set_file_mtime(&path_arg, mtime)?;
                            }
                            InodeId::Symlink(_) => {
                                unimplemented!();
                            }
                        }
                    }
                }
                PathCommand::Add { paths: path_args, existing_file_behavior: already_exists_behavior, remove_local_files } => {
                    // We need one transaction per new directory below.
                    drop(transaction);

                    let config = config::get_config()?;
                    let policy = config::get_policy()?;
                    for path_arg in &path_args {
                        let mut transaction = pool.begin().await?;
                        let path_components = path::resolve_local_path_to_path_components(Some(path_arg))?;
                        let (path_roots_value, idx) = path::resolve_root_of_local_path(&config, &path_components)?;
                        let base_dir = path_roots_value.dir_id;
                        let remaining_components = &path_components[idx..];
                        path::validate_path_components(remaining_components, &path_roots_value.new_dirent_requirements)?;
                        let components_to_base_dir = traversal::get_path_segments_from_root_to_dir(&mut transaction, base_dir).await?;
                        let stash_path = [&components_to_base_dir, remaining_components].concat();

                        let attr = fs::metadata(path_arg).await?;
                        let metadata: storage_write::RelevantFileMetadata = (&attr).try_into()?;
                        if attr.is_file() {
                            let stash_path: Vec<&str> = stash_path.iter().map(String::as_str).collect();

                            let basename = remaining_components.last().unwrap();
                            let dir_components = &remaining_components[..remaining_components.len() - 1];
                            // TODO: do this properly and use the mtimes of the local dirs
                            let dir_id = traversal::make_dirs(&mut transaction, base_dir, dir_components).await?.dir_id()?;
                            if let Some(existing) = Dirent::find_by_parent_and_basename(&mut transaction, dir_id, basename).await? {
                                match already_exists_behavior {
                                    ExistingFileBehavior::stop => {
                                        bail!("{:?} already exists as {:?}", stash_path, existing);
                                    }
                                    ExistingFileBehavior::skip => {
                                        eprintln!("{:?} already exists as {:?}", stash_path, existing);
                                        continue;
                                    }
                                    ExistingFileBehavior::replace => {
                                        eprintln!("{:?} already exists as {:?} but replacing as requested", stash_path, existing);
                                        existing.remove(&mut transaction).await?;
                                    }
                                }
                            }
                            transaction.commit().await?;

                            let desired_storage = policy.new_file_storages(&stash_path, &metadata)?;

                            let initial_delay = std::time::Duration::new(5, 0);
                            let maximum_delay = std::time::Duration::new(1800, 0);
                            let mut decayer = Decayer::new(initial_delay, Ratio::new(3, 2), maximum_delay);
                            let mut tries = 30;
                            let file_id = loop {
                                match storage_write::write(path_arg.clone(), &metadata, &desired_storage).await {
                                    Ok(id) => break id,
                                    Err(err) => {
                                        tries -= 1;
                                        if tries == 0 {
                                            bail!(err);
                                        }
                                        let delay = decayer.decay();
                                        eprintln!("storage_write::write({:?}, ...) failed, {} tries left \
                                                   (next in {} sec): {:?}", path_arg, tries, delay.as_secs(), err);
                                        tokio::time::sleep(delay).await;
                                    }
                                }
                            };

                            let child = InodeId::File(file_id);
                            transaction = pool.begin().await?;
                            Dirent::new(dir_id, basename, child).create(&mut transaction).await?;
                        } else {
                            bail!("can only add a file right now")
                        }

                        transaction.commit().await?;

                        if remove_local_files {
                            info!("removing local file {:?} after committing to database", path_arg);
                            fs::remove_file(path_arg).await?;
                        }
                    }
                }
                PathCommand::Ls { path: path_arg, just_names, sort, reverse } => {
                    let config = config::get_config()?;
                    let inode_id = path::resolve_local_path_arg(&config, &mut transaction, path_arg.as_deref()).await?;
                    let dir_id = inode_id.dir_id()?;
                    let mut dirents = Dirent::find_by_parents(&mut transaction, &[dir_id]).await?;
                    // In this case, there is no need to retrieve the inodes
                    let inodes = if just_names && sort == SortOrder::name {
                        HashMap::new()
                    } else {
                        let children: Vec<InodeId> = dirents.iter().map(|dirent| dirent.child).collect();
                        Inode::find_by_inode_ids(&mut transaction, &children).await?
                    };
                    match sort {
                        SortOrder::name  => { dirents.sort_by(|d1, d2| d1.basename.cmp(&d2.basename)) },
                        SortOrder::mtime => { dirents.sort_by_key(|dirent| inodes.get(&dirent.child).unwrap().mtime()) },
                        SortOrder::size  => { dirents.sort_by_key(|dirent| inodes.get(&dirent.child).unwrap().size()) },
                    }
                    if reverse {
                        dirents.reverse();
                    }
                    for dirent in dirents {
                        if just_names {
                            println!("{}", dirent.basename);
                            continue;
                        }
                        match dirent.child {
                            inode @ InodeId::Dir(_) => {
                                let size = 0;
                                // We're in the same transaction, so database should really have
                                // returned all the inodes we asked for, therefore .unwrap()
                                let dir = inodes.get(&inode).unwrap().dir().unwrap();
                                let mtime = dir.mtime.format("%Y-%m-%d %H:%M");
                                println!("{:>18} {} {}/", size, mtime, Paint::blue(dirent.basename));
                            }
                            inode @ InodeId::File(_) => {
                                use num_format::{Locale, ToFormattedString};

                                let file = inodes.get(&inode).unwrap().file().unwrap();
                                let size = file.size.to_formatted_string(&Locale::en);
                                let mtime = file.mtime.format("%Y-%m-%d %H:%M");
                                if file.executable {
                                    println!("{:>18} {} {}*", size, mtime, Paint::green(dirent.basename).bold());
                                } else {
                                    println!("{:>18} {} {}", size, mtime, dirent.basename);
                                };
                            }
                            inode @ InodeId::Symlink(_) => {
                                let size = 0;
                                let symlink = inodes.get(&inode).unwrap().symlink().unwrap();
                                let mtime = symlink.mtime.format("%Y-%m-%d %H:%M");
                                println!("{:>18} {} {} -> {}", size, mtime, dirent.basename, symlink.target);
                            }
                        }
                    }
                }
                PathCommand::Find { paths: path_args, r#type, null_sep } => {
                    // find in cwd if no path args
                    let mut path_args = path_args.clone();
                    if path_args.is_empty() {
                        path_args.push(String::from("."));
                    }

                    let config = config::get_config()?;
                    let mut roots = vec![];
                    // Resolve all root paths to inodes before doing the walk operations,
                    // during which files could be renamed.
                    for path_arg in path_args {
                        let dir_id = path::resolve_local_path_arg(&config, &mut transaction, Some(&path_arg)).await?.dir_id()?;
                        roots.push((dir_id, path_arg));
                    }

                    let terminator = if null_sep { '\0' } else { '\n' };
                    for (dir_id, path_arg) in roots {
                        if r#type.is_none() || r#type == Some(FindKind::d) {
                            // Print the top-level dir like findutils find
                            print!("{}{}", path_arg, terminator);
                        }
                        x_find(&mut transaction, &[&path_arg], dir_id, r#type, terminator).await?;
                    }
                }
                PathCommand::Mkdir { paths: path_args } => {
                    // We need one transaction per new directory below.
                    drop(transaction);

                    let config = config::get_config()?;

                    for path_arg in path_args {
                        let mut transaction = pool.begin().await?;
                        let path_components = path::resolve_local_path_to_path_components(Some(&path_arg))?;
                        let (path_roots_value, idx) = path::resolve_root_of_local_path(&config, &path_components)?;
                        let base_dir = path_roots_value.dir_id;
                        let remaining_components = &path_components[idx..];
                        path::validate_path_components(remaining_components, &path_roots_value.new_dirent_requirements)?;
                        traversal::make_dirs(&mut transaction, base_dir, remaining_components).await?;
                        transaction.commit().await?;

                        // For convenience, also create the corresponding directory on the local filesystem
                        std::fs::create_dir_all(path_arg)?;
                    }
                }
                PathCommand::Rm { paths: path_args } => {
                    // We need one transaction per dirent removal below (at least for dirents with a child_dir).
                    drop(transaction);

                    let config = config::get_config()?;

                    for path_arg in path_args {
                        let mut transaction = pool.begin().await?;
                        let path_components = path::resolve_local_path_to_path_components(Some(&path_arg))?;
                        let (path_roots_value, idx) = path::resolve_root_of_local_path(&config, &path_components)?;
                        let base_dir = path_roots_value.dir_id;
                        let remaining_components = &path_components[idx..];

                        let dirent = traversal::resolve_dirent(&mut transaction, base_dir, remaining_components).await?;
                        dirent.remove(&mut transaction).await?;
                        if let InodeId::Dir(dir_id) = dirent.child {
                            Dir::remove(&mut transaction, &[dir_id]).await?;
                        }

                        transaction.commit().await?;
                    }
                }
            }
        }
    };

    Ok(())
}
