#![feature(lint_reasons)]
// pattern binding `s` is named the same as one of the variants of the type `FindKind`
#![allow(bindings_with_variant_name)]

use tracing::info;
use yansi::Paint;
use async_recursion::async_recursion;
use clap::{ValueEnum, Subcommand, Parser};
use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use tokio::fs;
use tokio_util::codec::FramedRead;
use std::collections::HashMap;
use std::convert::TryInto;
use std::path::PathBuf;
use num::rational::Ratio;
use sqlx::{Postgres, Transaction};
use tracing_subscriber::EnvFilter;
use exastash::util::{FixedReadSizeDecoder, commaify_i64};
use serde_json::json;
use exastash::db;
use exastash::db::storage::gdrive::{file::GdriveFile, GdriveFilePlacement};
use exastash::db::inode::{InodeId, Inode, File, Dir, NewDir, Symlink, NewSymlink};
use exastash::db::dirent::{Dirent, InodeTuple};
use exastash::db::google_auth::{GoogleApplicationSecret, GoogleServiceAccount};
use exastash::db::traversal;
use exastash::path;
use exastash::config;
use exastash::policy;
use exastash::info::json_info;
use exastash::oauth;
use exastash::retry::Decayer;
use exastash::storage;
use yup_oauth2::ServiceAccountKey;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[clap(name = "es", version)]
/// exastash
enum ExastashCommand {
    /// Commands to work with directories
    #[clap(subcommand, name = "dir")]
    Dir(DirCommand),

    /// Commands to work with files
    #[clap(subcommand, name = "file")]
    File(FileCommand),

    /// Commands to work with symbolic links
    #[clap(subcommand, name = "symlink")]
    Symlink(SymlinkCommand),

    /// Commands to work with directory entries
    #[clap(subcommand, name = "dirent")]
    Dirent(DirentCommand),

    /// Commands to work with Google tokens and secrets
    #[clap(subcommand, name = "google")]
    Google(GoogleCommand),

    /// Commands to work with storage methods
    #[clap(subcommand, name = "storage")]
    Storage(StorageCommand),

    /// Commands that operate based on paths relative to cwd. To resolve paths,
    /// exastash walks up to find a root directory that points to some stash
    /// dir inode. Root directories can be configured in ~/.config/exastash/config.toml
    #[clap(subcommand, name = "x")]
    Path(PathCommand),

    /// web server
    #[clap(name = "web")]
    Web {
        #[clap(long)]
        port: u16,
    },

    /// Print license information
    License,
}

#[derive(Subcommand, Debug)]
enum DirCommand {
    /// Create a new directory as a child of some directory and print its id to stdout
    #[clap(name = "create")]
    Create {
        #[clap(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[clap(name = "BASENAME")]
        basename: String,
    },

    /// Delete an empty directory and its associated dirent where it is a child_dir
    #[clap(name = "remove")]
    Delete {
        #[clap(name = "DIR_ID")]
        dir_id: i64,
    },

    /// Print info in JSON format for zero or more dirs
    #[clap(name = "info")]
    Info {
        /// dir id
        #[clap(name = "ID")]
        ids: Vec<i64>,
    },

    /// Print a count of the number of dirs
    Count,
}

#[derive(Subcommand, Debug)]
enum FileCommand {
    /// Create an unparented file, based on a local file, and print its id to stdout
    #[clap(name = "create")]
    Create {
        /// Local file from which content, mtime, and executable flag will be read
        #[clap(name = "PATH")]
        path: String,

        /// Store the file data in the database itself. Can be specified with other --store-* options.
        #[clap(long)]
        store_inline: bool,

        /// Store the file data in some fofs pile (specified by id).
        /// Can be specified multiple times and with other --store-* options.
        #[clap(long)]
        store_fofs: Vec<i32>,

        /// Store the file data in some google domain (specified by id).
        /// Can be specified multiple times and with other --store-* options.
        #[clap(long)]
        store_gdrive: Vec<i16>,
    },

    /// Add the given storages for stash files. Skips adding storages that already exists for a file.
    #[clap(name = "add-storages")]
    AddStorages {
        /// file id
        #[clap(name = "FILE_ID")]
        file_ids: Vec<i64>,

        /// Store the file data in the database itself. Can be specified with other --store-* options.
        #[clap(long)]
        store_inline: bool,

        /// Store the file data in some fofs pile (specified by id).
        /// Can be specified multiple times and with other --store-* options.
        #[clap(long, name = "FOFS_PILE_ID")]
        store_fofs: Vec<i32>,

        /// Store the file data in some google domain (specified by id).
        /// Can be specified multiple times and with other --store-* options.
        #[clap(long, name = "GOOGLE_DOMAIN_ID")]
        store_gdrive: Vec<i16>,
    },

    /// Delete the given storages for stash files. Skips deleting storages that are not present.
    /// DANGER: does not check if there any storages left before deleting the last one.
    #[clap(name = "delete-storages")]
    DeleteStorages {
        /// file id
        #[clap(name = "FILE_ID")]
        file_ids: Vec<i64>,

        /// Delete the inline storage from the database. Can be specified with other --delete-* options.
        #[clap(long)]
        delete_inline: bool,

        /// Delete the fofs storage from some pile (specified by id), both on disk and the database reference to it.
        /// Can be specified multiple times and with other --delete-* options.
        #[clap(long, name = "FOFS_PILE_ID")]
        delete_fofs: Vec<i32>,

        /// Delete the gdrive storage from some google domain (specified by id), both from Google and the database reference to it.
        /// Can be specified multiple times and with other --delete-* options.
        #[clap(long, name = "GOOGLE_DOMAIN_ID")]
        delete_gdrive: Vec<i16>,
    },

    /// Delete a file and all of its storages
    #[clap(name = "delete")]
    Delete {
        #[clap(name = "FILE_ID")]
        file_id: i64,
    },

    /// Print info in JSON format for zero or more files
    #[clap(name = "info")]
    Info {
        /// file id
        #[clap(name = "ID")]
        ids: Vec<i64>,
    },

    /// Commands for working with file content
    #[clap(subcommand, name = "content")]
    Content(ContentCommand),

    /// Print a count of the number of files
    Count,
}

#[derive(Subcommand, Debug)]
enum ContentCommand {
    /// Output a file's content to stdout
    #[clap(name = "read")]
    Read {
        /// file id
        #[clap(name = "ID")]
        id: i64,
    },
}

#[derive(Subcommand, Debug)]
enum SymlinkCommand {
    /// Create a symlink
    #[clap(name = "create")]
    Create {
        #[clap(name = "TARGET")]
        target: String,
    },

    /// Delete a symlink
    #[clap(name = "delete")]
    Delete {
        #[clap(name = "SYMLINK_ID")]
        symlink_id: i64,
    },

    /// Print info in JSON format for zero or more symlinks
    #[clap(name = "info")]
    Info {
        /// symlink id
        #[clap(name = "ID")]    
        ids: Vec<i64>,
    },

    /// Print a count of the number of symlinks
    Count,
}

#[derive(ValueEnum, Clone, Debug)]
#[expect(non_camel_case_types)]
enum ResolveKind {
    dir,
    file,
    symlink,
}

#[derive(Subcommand, Debug)]
enum DirentCommand {
    /// Create a dirent. This does not follow the new_dirent_requirements set in config.toml.
    #[clap(name = "create")]
    Create {
        #[clap(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[clap(name = "BASENAME")]
        basename: String,

        #[clap(long, short = 'd')]
        child_dir: Option<i64>,

        #[clap(long, short = 'f')]
        child_file: Option<i64>,

        #[clap(long, short = 's')]
        child_symlink: Option<i64>,
    },

    /// Remove a dirent. If dirent has a child_dir, use `es dir delete` instead.
    #[clap(name = "remove")]
    Remove {
        #[clap(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[clap(name = "BASENAME")]
        basename: String,
    },

    /// List a dir's children in JSON format, for zero or more parent dirs
    #[clap(name = "list")]
    List {
        /// dir id
        #[clap(name = "ID")]
        ids: Vec<i64>,
    },

    /// Walk a dir recursively and print path info in JSON format
    #[clap(name = "walk")]
    Walk {
        /// dir id
        #[clap(name = "ID")]
        id: i64,
    },

    /// Resolve paths to dir, file, or symlink ids
    #[clap(name = "resolve")]
    Resolve {
        /// Kind of entity to resolve. If a path resolves to another kind, it will be skipped.
        #[clap(value_enum, name = "KIND")]
        kind: ResolveKind,

        /// Dir id of root dir from which to resolve paths
        #[clap(name = "ROOT_DIR_ID")]
        root: i64,

        /// Path consisting only of slash-separated basenames. There is no handling of
        /// '.', '..', duplicate '/', leading '/', or trailing '/'
        #[clap(name = "PATH")]
        paths: Vec<String>,
    },

    /// Print a count of the number of dirents
    Count,
}


#[derive(Subcommand, Debug)]
enum ApplicationSecretCommand {
    /// Import an application secret from a .json file
    #[clap(name = "import")]
    Import {
        #[clap(name = "DOMAIN_ID")]
        domain_id: i16,

        #[clap(name = "JSON_FILE")]
        json_file: String,
    },
}

#[derive(Subcommand, Debug)]
enum AccessTokenCommand {
    /// Create an OAuth 2.0 access token for an owner. Domain, owner,
    /// and application secret must already be in database.
    #[clap(name = "create")]
    Create {
        #[clap(name = "OWNER_ID")]
        owner_id: i32,
    },
}

#[derive(Subcommand, Debug)]
enum ServiceAccountCommand {
    /// Import a service account key from a .json file
    #[clap(name = "import")]
    Import {
        #[clap(name = "OWNER_ID")]
        owner_id: i32,

        #[clap(name = "JSON_FILE")]
        json_file: String,
    },
}

#[derive(Subcommand, Debug)]
enum GoogleCommand {
    /// Manage OAuth 2.0 application secrets (used with the "installed" application flow)
    #[clap(subcommand, name = "app-secret")]
    ApplicationSecret(ApplicationSecretCommand),

    /// Manage OAuth 2.0 access tokens
    #[clap(subcommand, name = "access-token")]
    AccessToken(AccessTokenCommand),

    /// Manage Google service accounts
    #[clap(subcommand, name = "service-account")]
    ServiceAccount(ServiceAccountCommand),

    /// Run a loop that refreshes OAuth 2.0 access tokens every ~5 minutes
    #[clap(name = "token-service")]
    TokenService,
}

#[derive(Subcommand, Debug)]
enum StorageCommand {
    /// gdrive storage
    #[clap(subcommand, name = "gdrive")]
    Gdrive(GdriveStorageCommand),
}

#[derive(Subcommand, Debug)]
enum GdriveStorageCommand {
    /// Internal commands for debugging
    #[clap(subcommand, name = "internal")]
    Internal(InternalCommand),

    /// gdrive file placement commands
    #[clap(subcommand, name = "placement")]
    Placement(PlacementCommand),
}

#[derive(Subcommand, Debug)]
enum InternalCommand {
    /// Create an unencrypted/unaltered Google Drive file based on some local
    /// file and record it in the database. Output the info of the new gdrive
    /// file to stdout as JSON.
    #[clap(name = "create-file")]
    CreateFile {
        /// Path to the local file to upload
        #[clap(name = "PATH")]
        path: PathBuf,

        /// google_domain to upload to
        #[clap(name = "DOMAIN_ID")]
        domain_id: i16,

        /// gdrive_owner to upload as
        #[clap(name = "OWNER_ID")]
        owner_id: i32,

        /// Google Drive folder ID to create the file in
        #[clap(name = "PARENT")]
        parent: String,

        /// Google Drive filename for the new file
        #[clap(name = "FILENAME")]
        filename: String,
    },

    /// Read the contents of a sequence of Google Drive files to stdout.
    #[clap(name = "read-files")]
    ReadFiles {
        /// google_domain to read from
        #[clap(name = "DOMAIN_ID")]
        domain_id: i16,

        /// ID of the Google Drive file to read
        #[clap(name = "FILE_ID")]
        file_ids: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
enum PlacementCommand {
    /// Print file placement info in JSON format
    #[clap(name = "list")]
    List {
        /// google_domain for which to list file placement information
        #[clap(name = "DOMAIN_ID")]
        domain_id: i16,
    },
}

#[derive(ValueEnum, Copy, Clone, Debug, PartialEq, Eq)]
#[expect(non_camel_case_types)]
enum FindKind {
    d, // dir
    f, // file
    s, // symlink
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
#[expect(non_camel_case_types)]
enum ExistingFileBehavior {
    stop,
    skip,
    replace,
}

#[derive(Subcommand, Debug)]
enum PathCommand {
    /// Print info in JSON format for a path's inode
    #[clap(name = "info")]
    Info {
        /// Path to an inode to print info for, relative to cwd
        #[clap(name = "PATH")]
        paths: Vec<String>,
    },

    /// Write the contents of a file to stdout
    #[clap(name = "cat")]
    Cat {
        /// Path to a file to cat, relative to cwd
        #[clap(name = "PATH")]
        paths: Vec<String>,
    },

    /// Retrieve a dir, file, or symlink to the local filesystem.
    /// Not recursive.
    #[clap(name = "get")]
    Get {
        /// Path to get from stash, relative to cwd
        #[clap(name = "PATH")]
        paths: Vec<String>,

        /// Skip retrieval if the file exists locally with a matching size and mtime
        #[clap(long, short = 's')]
        skip_if_exists: bool,
    },

    /// Create a stash file based on a local file. This also makes local file
    /// read-only to make it more obviously immutable like the stash file.
    #[clap(name = "add")]
    Add {
        /// Path to add to stash, relative to cwd
        #[clap(name = "PATH")]
        paths: Vec<String>,

        /// What to do if a directory entry already exists at the corresponding stash path
        #[clap(value_enum, long, short = 'e', default_value = "stop")]
        existing_file_behavior: ExistingFileBehavior,

        /// Remove each local file after successfully storing it and creating a dirent
        #[clap(long)]
        remove_local_files: bool,
    },

    /// List a directory
    #[clap(name = "ls")]
    Ls {
        /// Path to list, relative to cwd
        #[clap(name = "PATH")]
        path: Option<String>,

        /// Whether to print just the filenames
        #[clap(long, short = 'j')]
        just_names: bool,

        /// By which field to sort the output
        #[clap(value_enum, long, default_value = "name")]
        sort: SortOrder,

        /// Whether to sort in reverse
        #[clap(long, short = 'r')]
        reverse: bool,
    },

    /// Recursively list a directory like findutils find
    #[clap(name = "find")]
    Find {
        /// Path to list recursively, relative to cwd
        #[clap(name = "PATH")]
        paths: Vec<String>,

        /// Limit output to paths pointing to inodes of this type (d = dir, f = file, s = symlink)
        #[clap(value_enum, long, short = 't')]
        r#type: Option<FindKind>,

        /// Print filenames separated by NULL instead of LF
        #[clap(short = '0')]
        null_sep: bool,
    },

    /// Create a directory. This does not follow the new_dirent_requirements set in config.toml.
    #[clap(name = "mkdir")]
    Mkdir {
        /// Directory path to create, relative to cwd. Parent directories are
        /// also created as needed. For your convenience, the same directories
        /// are also created in cwd.
        #[clap(name = "PATH")]
        paths: Vec<String>,
    },

    /// Remove a directory entry. Also deletes the corresponding dir when removing
    /// a child_dir dirent. Does not delete files or symlinks, even when removing
    /// the last dirent to a file or symlink.
    #[clap(name = "rm")]
    Rm {
        /// Path to a dirent to remove, relative to cwd.
        #[clap(name = "PATH")]
        paths: Vec<String>,
    },
}

#[derive(ValueEnum, Clone, Debug, PartialEq, Eq)]
#[expect(non_camel_case_types)]
enum SortOrder {
    name,
    mtime,
    size,
}

async fn resolve_path(transaction: &mut Transaction<'_, Postgres>, root: i64, path: &str) -> Result<InodeId> {
    let path_components: Vec<&str> = if path.is_empty() {
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
            "path":       format!("{path_string}{}", dirent.basename),
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
            print!("{path_string}{}{terminator}", dirent.basename);
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
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();

    // Do this first for --help to work without a database connection
    let command = ExastashCommand::parse();

    if let ExastashCommand::License = command {
        print!("{}", include_str!("../LICENSE"));
        return Ok(());
    }

    let mut pool = db::pgpool().await;
    match command {
        ExastashCommand::License => {
            // Handled above
            unreachable!();
        },
        ExastashCommand::Dir(command) => {
            match command {
                DirCommand::Create { parent_dir_id, basename } => {
                    let mut transaction = pool.begin().await?;
                    let mtime = Utc::now();
                    let birth = db::inode::Birth::here_and_now();
                    let dir = NewDir { mtime, birth }.create(&mut transaction).await?;
                    Dirent::new(parent_dir_id, basename, InodeId::Dir(dir.id)).create(&mut transaction).await?;
                    transaction.commit().await?;
                    println!("{}", dir.id);
                }
                DirCommand::Delete { dir_id } => {
                    let mut transaction = pool.begin().await?;
                    Dirent::remove_by_child_dir(&mut transaction, dir_id).await?;
                    Dir::delete(&mut transaction, &[dir_id]).await?;
                    transaction.commit().await?;
                }
                DirCommand::Info { ids } => {
                    let mut transaction = pool.begin().await?;
                    let inode_ids: Vec<InodeId> = ids.into_iter().map(InodeId::Dir).collect();
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).ok_or_else(|| anyhow!("{:?} not found in database", inode_id))?;
                        println!("{}", json_info(inode).await?);
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                DirCommand::Count => {
                    let mut transaction = pool.begin().await?;
                    let count = Dir::count(&mut transaction).await?;
                    transaction.commit().await?; // close read-only transaction
                    println!("{count}");
                }
            }
        }
        ExastashCommand::File(command) => {
            match command {
                FileCommand::Create { path, store_inline, store_fofs, store_gdrive } => {
                    let store_fofs = store_fofs.into_iter().collect();
                    let store_gdrive = store_gdrive.into_iter().collect();
                    let desired = storage::StoragesDescriptor { inline: store_inline, fofs: store_fofs, gdrive: store_gdrive };

                    let attr = fs::metadata(path.clone()).await?;
                    let metadata: storage::RelevantFileMetadata = attr.try_into()?;
                    let file_id = storage::write::create_stash_file_from_local_file(path, &metadata, &desired).await?;
                    println!("{file_id}");
                }
                FileCommand::AddStorages { file_ids, store_inline, store_fofs, store_gdrive } => {
                    let store_fofs = store_fofs.into_iter().collect();
                    let store_gdrive = store_gdrive.into_iter().collect();
                    let desired = storage::StoragesDescriptor { inline: store_inline, fofs: store_fofs, gdrive: store_gdrive };

                    let mut transaction = pool.begin().await?;
                    let files = File::find_by_ids(&mut transaction, &file_ids).await?;
                    transaction.commit().await?; // close read-only transaction
                    let mut map = HashMap::with_capacity(files.len());
                    for file in files {
                        map.insert(file.id, file);
                    }

                    for file_id in file_ids {
                        let file = map.get(&file_id).ok_or_else(|| anyhow!("no file with id={}", file_id))?;

                        let desired_new = storage::write::desired_storages_without_those_that_already_exist(file_id, &desired).await?;
                        if desired_new.is_empty() {
                            info!(file_id, "file is already present in all desired storages");
                            continue;
                        }

                        // Read to temporary file because we need an AsyncRead we can Send,
                        // and because when adding more than one storage, we want to avoid
                        // reading a file more than once from existing storage.
                        let (stream, _) = storage::read::read(file_id).await?;
                        let temp_path = tempfile::NamedTempFile::new()?.into_temp_path();
                        let path: PathBuf = (*temp_path).into();
                        let mut local_file = tokio::fs::File::create(path.clone()).await?;
                        storage::read::write_stream_to_sink(stream, &mut local_file).await?;

                        let mut readers = storage::write::readers_for_file(path, desired_new.len()).await?;
                        let producer = move || {
                            readers.pop().ok_or_else(|| anyhow!("no readers left"))
                        };
                        storage::write::add_storages(producer, file, &desired_new).await?;
                    }
                }
                FileCommand::DeleteStorages { file_ids, delete_inline, delete_fofs, delete_gdrive } => {
                    let delete_fofs = delete_fofs.into_iter().collect();
                    let delete_gdrive = delete_gdrive.into_iter().collect();
                    let undesired = storage::StoragesDescriptor { inline: delete_inline, fofs: delete_fofs, gdrive: delete_gdrive };
                    for file_id in file_ids {
                        storage::delete::delete_storages(file_id, &undesired).await?;
                    }
                }
                FileCommand::Delete { file_id } => {
                    let mut transaction = pool.begin().await?;
                    // TODO call something in storage::delete so we can delete the file if it has any storages
                    File::delete(&mut transaction, &[file_id]).await?;
                    transaction.commit().await?;
                }
                FileCommand::Info { ids } => {
                    let mut transaction = pool.begin().await?;
                    let inode_ids: Vec<InodeId> = ids.into_iter().map(InodeId::File).collect();
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).ok_or_else(|| anyhow!("{:?} not found in database", inode_id))?;
                        println!("{}", json_info(inode).await?);
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                FileCommand::Content(content) => {
                    match content {
                        ContentCommand::Read { id } => {
                            let (stream, _) = storage::read::read(id).await?;
                            let mut stdout = tokio::io::stdout();
                            storage::read::write_stream_to_sink(stream, &mut stdout).await?;
                        }
                    }
                }
                FileCommand::Count => {
                    let mut transaction = pool.begin().await?;
                    let count = File::count(&mut transaction).await?;
                    transaction.commit().await?; // close read-only transaction
                    println!("{count}");
                }
            }
        }
        ExastashCommand::Symlink(command) => {
            match command {
                SymlinkCommand::Create { target } => {
                    let mut transaction = pool.begin().await?;
                    let mtime = Utc::now();
                    let birth = db::inode::Birth::here_and_now();
                    let symlink = NewSymlink { mtime, birth, target }.create(&mut transaction).await?;
                    transaction.commit().await?;
                    println!("{}", symlink.id);
                }
                SymlinkCommand::Delete { symlink_id } => {
                    let mut transaction = pool.begin().await?;
                    Symlink::delete(&mut transaction, &[symlink_id]).await?;
                    transaction.commit().await?;
                }
                SymlinkCommand::Info { ids } => {
                    let mut transaction = pool.begin().await?;
                    let inode_ids: Vec<InodeId> = ids.into_iter().map(InodeId::Symlink).collect();
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).ok_or_else(|| anyhow!("{:?} not found in database", inode_id))?;
                        println!("{}", json_info(inode).await?);
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                SymlinkCommand::Count => {
                    let mut transaction = pool.begin().await?;
                    let count = Symlink::count(&mut transaction).await?;
                    transaction.commit().await?; // close read-only transaction
                    println!("{count}");
                }
            }
        }
        ExastashCommand::Dirent(command) => {
            match command {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let mut transaction = pool.begin().await?;
                    let child = InodeTuple(child_dir, child_file, child_symlink).try_into()?;
                    Dirent::new(parent_dir_id, basename, child).create(&mut transaction).await?;
                    transaction.commit().await?;
                }
                DirentCommand::Remove { parent_dir_id, basename } => {
                    let mut transaction = pool.begin().await?;
                    Dirent::remove_by_parent_basename(&mut transaction, parent_dir_id, &basename).await?;
                    transaction.commit().await?;
                }
                DirentCommand::List { ids } => {
                    let mut transaction = pool.begin().await?;
                    let dirents = Dirent::find_by_parents(&mut transaction, &ids).await?;
                    transaction.commit().await?; // close read-only transaction
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
                    let mut transaction = pool.begin().await?;
                    walk_dir(&mut transaction, id, &[], id).await?;
                    transaction.commit().await?; // close read-only transaction
                }
                DirentCommand::Resolve { kind, root, paths } => {
                    let mut transaction = pool.begin().await?;
                    for path in paths {
                        let inode = resolve_path(&mut transaction, root, &path).await?;
                        match kind {
                            ResolveKind::dir     => if let InodeId::Dir(id)     = inode { println!("{id}") },
                            ResolveKind::file    => if let InodeId::File(id)    = inode { println!("{id}") },
                            ResolveKind::symlink => if let InodeId::Symlink(id) = inode { println!("{id}") },
                        }
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                DirentCommand::Count => {
                    let mut transaction = pool.begin().await?;
                    let count = Dirent::count(&mut transaction).await?;
                    transaction.commit().await?; // close read-only transaction
                    println!("{count}");
                }
            }
        }
        ExastashCommand::Google(command) => {
            match command {
                GoogleCommand::ApplicationSecret(command) => {
                    match command {
                        ApplicationSecretCommand::Import { domain_id, json_file } => {
                            let mut transaction = pool.begin().await?;
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
                            let mut transaction = pool.begin().await?;
                            oauth::create_access_token(&mut transaction, owner_id).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GoogleCommand::ServiceAccount(command) => {
                    match command {
                        ServiceAccountCommand::Import { owner_id, json_file } => {
                            let content = fs::read(json_file).await?;
                            let key: ServiceAccountKey = serde_json::from_slice(&content)?;
                            assert_eq!(key.key_type, Some("service_account".into()));
                            let mut transaction = pool.begin().await?;
                            GoogleServiceAccount { owner_id, key }.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GoogleCommand::TokenService => {
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
                                    let mut transaction = pool.begin().await?;
                                    let placements = GdriveFilePlacement::find_by_domain(&mut transaction, domain_id, None).await?;
                                    for placement in placements {
                                        let j = serde_json::to_string(&placement)?;
                                        println!("{j}");
                                    }
                                    transaction.commit().await?; // close read-only transaction
                                }
                            }
                        }
                        GdriveStorageCommand::Internal(command) => {
                            match command {
                                InternalCommand::CreateFile { path, domain_id, owner_id, parent, filename } => {
                                    let attr = fs::metadata(&path).await?;
                                    let size = attr.len();

                                    let reader = fs::File::open(path.clone()).await?;
                                    // n.b. 'internal' bypasses encryption - so read size is unrelated to AES-GCM block size
                                    let decoder = FixedReadSizeDecoder::new(65536);
                                    let file_stream = FramedRead::new(reader, decoder);

                                    let gdrive_file = storage::write::create_gdrive_file_on_domain(
                                        file_stream, size, domain_id, owner_id, &parent, &filename
                                    ).await?;
                                    let mut transaction = pool.begin().await?;
                                    gdrive_file.create(&mut transaction).await?;
                                    transaction.commit().await?;
                                    let j = serde_json::to_string_pretty(&gdrive_file)?;
                                    println!("{j}");
                                }
                                InternalCommand::ReadFiles { domain_id, file_ids } => {
                                    let mut transaction = pool.begin().await?;
                                    let gdrive_ids: Vec<&str> = file_ids.iter().map(String::as_str).collect();
                                    let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_ids).await?;
                                    for gdrive_file in &gdrive_files {
                                        let stream = Box::pin(storage::read::stream_gdrive_file(gdrive_file, domain_id).await?);
                                        let mut stdout = tokio::io::stdout();
                                        storage::read::write_stream_to_sink(stream, &mut stdout).await?;
                                    }
                                    transaction.commit().await?; // close read-only transaction
                                }
                            }
                        }
                    }
                }
            }
        }
        ExastashCommand::Path(command) => {
            match command {
                PathCommand::Info { paths: path_args } => {
                    let config = config::get_config()?;
                    let mut inode_ids = vec![];
                    let mut transaction = pool.begin().await?;
                    for path_arg in path_args {
                        let inode_id = path::resolve_local_path_arg(&config, &mut transaction, Some(&path_arg)).await?;
                        inode_ids.push(inode_id);
                    }
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).unwrap();
                        println!("{}", json_info(inode).await?);
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                PathCommand::Cat { paths: path_args } => {
                    let config = config::get_config()?;
                    let mut file_ids = vec![];
                    let mut transaction = pool.begin().await?;
                    // Resolve all paths to inodes before doing the unpredictably-long read operations,
                    // during which files could be renamed.
                    for path_arg in path_args {
                        let file_id = path::resolve_local_path_arg(&config, &mut transaction, Some(&path_arg)).await?.file_id()?;
                        file_ids.push(file_id);
                    }
                    transaction.commit().await?; // close read-only transaction
                    for file_id in file_ids {
                        let (stream, _) = storage::read::read(file_id).await?;
                        let mut stdout = tokio::io::stdout();
                        storage::read::write_stream_to_sink(stream, &mut stdout).await?;
                    }
                }
                PathCommand::Get { paths: path_args, skip_if_exists } => {
                    use std::os::unix::fs::PermissionsExt;

                    let config = config::get_config()?;
                    let mut retrievals = vec![];
                    let mut transaction = pool.begin().await?;
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
                                            let metadata: storage::RelevantFileMetadata = attr.try_into()?;
                                            let files = File::find_by_ids(&mut transaction, &[file_id]).await?;
                                            let file = files.get(0).ok_or_else(|| {
                                                anyhow!("database unexpectedly missing file id={}", file_id)
                                            })?;
                                            if file.mtime == metadata.mtime && file.size == metadata.size {
                                                info!(?path_arg, "file already exists locally with matching size and mtime");

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
                                let (stream, file) = storage::read::read(file_id).await?;
                                storage::read::write_stream_to_sink(stream, &mut local_file).await?;

                                if file.executable {
                                    let permissions = std::fs::Permissions::from_mode(0o770);
                                    fs::set_permissions(&path_arg, permissions).await?;
                                }

                                let mtime = filetime::FileTime::from_system_time(file.mtime.into());
                                filetime::set_file_mtime(path_arg, mtime)?;
                            }
                            InodeId::Symlink(_) => {
                                unimplemented!();
                            }
                        }
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                PathCommand::Add { paths: path_args, existing_file_behavior: already_exists_behavior, remove_local_files } => {
                    // We need one transaction per new directory below, due to `dirents_check_insert_or_delete`.

                    let config = config::get_config()?;
                    let policy = policy::get_policy()?;
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
                        let metadata: storage::RelevantFileMetadata = (&attr).try_into()?;
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
                                        eprintln!("{stash_path:?} already exists as {existing:?}");
                                        continue;
                                    }
                                    ExistingFileBehavior::replace => {
                                        eprintln!("{stash_path:?} already exists as {existing:?} but replacing as requested");
                                        existing.remove(&mut transaction).await?;
                                    }
                                }
                            }
                            transaction.commit().await?;

                            let desired = policy.new_file_storages(&stash_path, &metadata)?;

                            let initial_delay = std::time::Duration::new(60, 0);
                            let maximum_delay = std::time::Duration::new(1800, 0);
                            let mut decayer = Decayer::new(initial_delay, Ratio::new(3, 2), maximum_delay);
                            let mut tries = 30;
                            let file_id = loop {
                                match storage::write::create_stash_file_from_local_file(path_arg.clone(), &metadata, &desired).await {
                                    Ok(id) => break id,
                                    Err(err) => {
                                        tries -= 1;
                                        if tries == 0 {
                                            bail!(err);
                                        }
                                        let delay = decayer.decay();
                                        eprintln!("storage::write::create_stash_file_from_local_file({path_arg:?}, ...) failed, {tries} tries left \
                                                   (next in {} sec): {err:?}", delay.as_secs());
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
                            info!(?path_arg, "removing local file after committing to database");
                            fs::remove_file(path_arg).await?;
                        }
                    }
                }
                PathCommand::Ls { path: path_arg, just_names, sort, reverse } => {
                    let config = config::get_config()?;
                    let mut transaction = pool.begin().await?;
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
                    transaction.commit().await?; // close read-only transaction
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
                                println!("{size:>18} {mtime} {}/", Paint::blue(dirent.basename));
                            }
                            inode @ InodeId::File(_) => {
                                let file = inodes.get(&inode).unwrap().file().unwrap();
                                let size = commaify_i64(file.size);
                                let mtime = file.mtime.format("%Y-%m-%d %H:%M");
                                if file.executable {
                                    println!("{size:>18} {mtime} {}*", Paint::green(dirent.basename).bold());
                                } else {
                                    println!("{size:>18} {mtime} {}", dirent.basename);
                                };
                            }
                            inode @ InodeId::Symlink(_) => {
                                let size = 0;
                                let symlink = inodes.get(&inode).unwrap().symlink().unwrap();
                                let mtime = symlink.mtime.format("%Y-%m-%d %H:%M");
                                println!("{size:>18} {mtime} {} -> {}", dirent.basename, symlink.target);
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
                    let mut transaction = pool.begin().await?;
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
                            print!("{path_arg}{terminator}");
                        }
                        x_find(&mut transaction, &[&path_arg], dir_id, r#type, terminator).await?;
                    }
                    transaction.commit().await?; // close read-only transaction
                }
                PathCommand::Mkdir { paths: path_args } => {
                    // We need one transaction per new directory below, due to `dirents_check_insert_or_delete`.

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
                            Dir::delete(&mut transaction, &[dir_id]).await?;
                        }

                        transaction.commit().await?;
                    }
                }
            }
        }
        ExastashCommand::Web { port } => {
            exastash::web::run(port).await?;
        }
    };

    pool.close().await;

    Ok(())
}
