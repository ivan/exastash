#![feature(format_args_capture)]

use tracing::info;
use yansi::Paint;
use async_recursion::async_recursion;
use clap::arg_enum;
use anyhow::{anyhow, Error, Result};
use structopt::StructOpt;
use chrono::Utc;
use futures::future::FutureExt;
use tokio::fs;
use std::convert::TryInto;
use std::path::PathBuf;
use sqlx::{Postgres, Transaction};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing_subscriber::EnvFilter;
use serde_json::json;
use exastash::db;
use exastash::db::storage::gdrive::file::GdriveFile;
use exastash::db::inode::{InodeId, Inode, File, Dir, NewDir, Symlink, NewSymlink};
use exastash::db::dirent::{Dirent, InodeTuple};
use exastash::db::google_auth::{GsuiteApplicationSecret, GsuiteServiceAccount};
use exastash::db::traversal;
//use exastash::fuse;
use exastash::ts;
use exastash::info::json_info;
use exastash::oauth;
use exastash::{storage_read, storage_write};
use futures::stream::TryStreamExt;
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

    /// Commands to work with G Suite
    #[structopt(name = "gsuite")]
    Gsuite(GsuiteCommand),

    /// Internal commands for debugging
    #[structopt(name = "internal")]
    Internal(InternalCommand),

    /// (nonfunctional) FUSE server
    #[structopt(name = "fuse")]
    Fuse(FuseCommand),

    /// terastash-like commands that take paths relative to cwd
    #[structopt(name = "ts")]
    Terastash(TerastashCommand),
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

        /// Store the file data in some gsuite domain (specified by id). Can be specified multiple times and with other --store-* options.
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
enum GsuiteCommand {
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
enum InternalCommand {
    /// Create a Google Drive file based on some local file
    #[structopt(name = "create-gdrive-file")]
    CreateGdriveFile {
        /// Path to the local file to upload
        #[structopt(name = "PATH")]
        path: PathBuf,

        /// gsuite_domain to upload to
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

    /// Read the contents of a sequence of Google Drive files to stdout
    #[structopt(name = "read-gdrive-files")]
    ReadGdriveFiles {
        /// gsuite_domain to read from
        #[structopt(name = "DOMAIN_ID")]
        domain_id: i16,

        /// ID of the Google Drive file to read
        #[structopt(name = "FILE_ID")]
        file_ids: Vec<String>,
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

#[derive(StructOpt, Debug)]
enum TerastashCommand {
    /// Print info in JSON format for a path's inode
    #[structopt(name = "info")]
    Info {
        /// Path to a file, dir, or symlink, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },

    /// Write the contents of a file to stdout
    #[structopt(name = "cat")]
    Cat {
        /// Path to a file, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },

    /// List a directory like terastash
    #[structopt(name = "ls")]
    Ls {
        /// Path to list, relative to cwd
        #[structopt(name = "PATH")]
        path: Option<String>,

        /// Whether to print just the filenames
        #[structopt(long, short = "j")]
        just_names: bool,
    },

    /// Recursively list a directory like findutils find
    #[structopt(name = "find")]
    Find {
        /// Path to list, relative to cwd
        #[structopt(name = "PATH")]
        paths: Vec<String>,

        /// Limit output to paths pointing to inodes of this type (d = dir, f = file, s = symlink)
        #[structopt(long, short = "t", possible_values = &FindKind::variants(), case_insensitive = false)]
        r#type: Option<FindKind>,

        /// Print filenames separated by NULL instead of LF
        #[structopt(short = "0")]
        null_sep: bool,
    },

    /// Create a directory
    #[structopt(name = "mkdir")]
    Mkdir {
        /// Directory path to create, relative to cwd. Parent directories are
        /// also created as needed. For your convenience, the same directories
        /// are also created in cwd.
        #[structopt(name = "PATH")]
        paths: Vec<String>,
    },
}

async fn resolve_path(transaction: &mut Transaction<'_, Postgres>, root: i64, path: &str) -> Result<InodeId> {
    let path_components: Vec<&str> = if path == "" {
        vec![]
    } else {
        path.split('/').collect()
    };
    traversal::walk_path(transaction, root, &path_components).await
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
async fn ts_find(transaction: &mut Transaction<'_, Postgres>, segments: &[&str], dir_id: i64, r#type: Option<FindKind>, terminator: char) -> Result<()> {
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
            ts_find(transaction, &segments, dir_id, r#type, terminator).await?;
        }
    }
    Ok(())
}

async fn write_stream_to_stdout(stream: storage_read::ReadStream) -> Result<()> {
    let mut read = stream
        .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();
    let mut stdout = tokio::io::stdout();
    tokio::io::copy(&mut read, &mut stdout).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let _subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // Do this first for --help to work without a database connection
    let cmd = ExastashCommand::from_args();
    let mut pool = db::pgpool().await;
    let mut transaction = pool.begin().await?;
    match cmd {
        ExastashCommand::Dir(dir) => {
            match dir {
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
        ExastashCommand::File(file) => {
            match file {
                FileCommand::Create { path, store_inline, store_gdrive } => {
                    drop(transaction);
                    let file_id = storage_write::write(path, store_inline, &store_gdrive).await?;
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
                            let stream = storage_read::read(id).await?;
                            write_stream_to_stdout(stream).await?;
                        }
                    }
                }
                FileCommand::Count => {
                    let count = File::count(&mut transaction).await?;
                    println!("{}", count);
                }
            }
        }
        ExastashCommand::Symlink(symlink) => {
            match symlink {
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
        ExastashCommand::Dirent(dirent) => {
            match dirent {
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
        ExastashCommand::Gsuite(command) => {
            match &command {
                GsuiteCommand::ApplicationSecret(command) => {
                    match command {
                        ApplicationSecretCommand::Import { domain_id, json_file } => {
                            let content = fs::read(json_file).await?;
                            let json = serde_json::from_slice(&content)?;
                            GsuiteApplicationSecret { domain_id: *domain_id, secret: json }.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GsuiteCommand::AccessToken(command) => {
                    match command {
                        AccessTokenCommand::Create { owner_id } => {
                            oauth::create_access_token(transaction, *owner_id).await?;
                        }
                    }
                }
                GsuiteCommand::ServiceAccount(command) => {
                    match command {
                        ServiceAccountCommand::Import { owner_id, json_file } => {
                            let content = fs::read(json_file).await?;
                            let key: ServiceAccountKey = serde_json::from_slice(&content)?;
                            assert_eq!(key.key_type, Some("service_account".into()));
                            GsuiteServiceAccount { owner_id: *owner_id, key }.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GsuiteCommand::TokenService => {
                    drop(transaction);
                    let interval_sec = 305;
                    info!("will check access tokens every {} seconds", interval_sec);
                    loop {
                        oauth::refresh_access_tokens(&mut pool).await?;
                        tokio::time::delay_for(std::time::Duration::new(interval_sec, 0)).await;
                    }
                }
            }
        }
        ExastashCommand::Internal(command) => {
            match &command {
                InternalCommand::CreateGdriveFile { path, domain_id, owner_id, parent, filename } => {
                    let attr = fs::metadata(&path).await?;
                    let size = attr.len();
                    let file_stream_fn = |offset| {
                        // TODO: support non-0 offset if we implement upload retries
                        assert_eq!(offset, 0);
                        fs::read(path.clone()).into_stream().map_ok(|vec| vec.into())
                    };
                    let gdrive_file = storage_write::create_gdrive_file_on_domain(file_stream_fn, size, *domain_id, *owner_id, parent, filename).await?;
                    let j = serde_json::to_string_pretty(&gdrive_file)?;
                    println!("{j}");
                }
                InternalCommand::ReadGdriveFiles { domain_id, file_ids } => {
                    let gdrive_ids: Vec<&str> = file_ids.iter().map(String::as_str).collect();
                    let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, &gdrive_ids).await?;
                    for gdrive_file in &gdrive_files {
                        let stream = Box::pin(storage_read::stream_gdrive_file(gdrive_file, *domain_id).await?);
                        write_stream_to_stdout(stream).await?;
                    }
                }
            }
        }
        ExastashCommand::Fuse(command) => {
            match &command {
                FuseCommand::Run { mountpoint: _ } => {
                    panic!("FUSE server was not built");
                    //fuse::run(mountpoint.into()).await?;
                }
            }
        }
        ExastashCommand::Terastash(command) => {
            match &command {
                TerastashCommand::Info { paths: path_args } => {
                    let config = ts::get_config()?;
                    let mut inode_ids = vec![];
                    for path_arg in path_args {
                        let inode_id = ts::resolve_local_path_arg(&config, &mut transaction, Some(path_arg)).await?;
                        inode_ids.push(inode_id);
                    }
                    let inodes = Inode::find_by_inode_ids(&mut transaction, &inode_ids).await?;
                    for inode_id in inode_ids {
                        let inode = inodes.get(&inode_id).unwrap();
                        println!("{}", json_info(&mut transaction, inode).await?);
                    }
                }
                TerastashCommand::Cat { paths: path_args } => {
                    let config = ts::get_config()?;
                    let mut file_ids = vec![];
                    // Resolve all paths to inodes before doing the unpredictably-long read operations,
                    // during which files could be renamed.
                    for path_arg in path_args {
                        let file_id = ts::resolve_local_path_arg(&config, &mut transaction, Some(path_arg)).await?.file_id()?;
                        file_ids.push(file_id);
                    }
                    for file_id in file_ids {
                        let stream = storage_read::read(file_id).await?;
                        write_stream_to_stdout(stream).await?;
                    }
                }
                TerastashCommand::Ls { path: path_arg, just_names } => {
                    let config = ts::get_config()?;
                    let inode_id = ts::resolve_local_path_arg(&config, &mut transaction, path_arg.as_deref()).await?;
                    let dir_id = inode_id.dir_id()?;
                    if *just_names {
                        let dirents = Dirent::find_by_parents(&mut transaction, &[dir_id]).await?;
                        for dirent in dirents {
                            println!("{}", dirent.basename);
                        }
                    } else {
                        let dirents = Dirent::find_by_parents(&mut transaction, &[dir_id]).await?;
                        let children: Vec<InodeId> = dirents.iter().map(|dirent| dirent.child).collect();
                        let inodes = Inode::find_by_inode_ids(&mut transaction, &children).await?;
                        for dirent in dirents {
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
                                    println!("{:>18} {} {}", size, mtime, dirent.basename);
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
                }
                TerastashCommand::Find { paths: path_args, r#type, null_sep } => {
                    // find in cwd if no path args
                    let mut path_args = path_args.clone();
                    if path_args.is_empty() {
                        path_args.push(String::from("."));
                    }

                    let config = ts::get_config()?;
                    let mut roots = vec![];
                    // Resolve all root paths to inodes before doing the walk operations,
                    // during which files could be renamed.
                    for path_arg in path_args {
                        let dir_id = ts::resolve_local_path_arg(&config, &mut transaction, Some(&path_arg)).await?.dir_id()?;
                        roots.push((dir_id, path_arg));
                    }

                    let terminator = if *null_sep { '\0' } else { '\n' };
                    for (dir_id, path_arg) in roots {
                        if r#type.is_none() || *r#type == Some(FindKind::d) {
                            // Print the top-level dir like findutils find
                            print!("{}{}", path_arg, terminator);
                        }
                        ts_find(&mut transaction, &[&path_arg], dir_id, *r#type, terminator).await?;
                    }
                }
                TerastashCommand::Mkdir { paths: path_args } => {
                    // We need one transaction per new directory below.
                    drop(transaction);

                    let config = ts::get_config()?;

                    for path_arg in path_args {
                        let mut transaction = pool.begin().await?;
                        let path_components = ts::resolve_local_path_to_path_components(Some(path_arg))?;
                        let (base_dir, idx) = ts::resolve_root_of_local_path(&config, &path_components)?;
                        let remaining_components = &path_components[idx..];
                        traversal::make_dirs(&mut transaction, base_dir, remaining_components).await?;
                        transaction.commit().await?;

                        // For convenience, also create the corresponding directory on the local filesystem
                        std::fs::create_dir_all(path_arg)?;
                    }
                }
            }
        }
    };

    Ok(())
}
