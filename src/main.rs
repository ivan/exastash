#![feature(format_args_capture)]

use tracing::info;
use async_recursion::async_recursion;
use clap::arg_enum;
use anyhow::{anyhow, bail, ensure, Error, Result};
use structopt::StructOpt;
use chrono::Utc;
use futures::future::FutureExt;
use tokio::fs;
use std::convert::TryInto;
use std::path::PathBuf;
use std::collections::HashMap;
use sqlx::{Postgres, Transaction};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing_subscriber::EnvFilter;
use serde_json::json;
use exastash::db;
use exastash::db::storage::get_storage;
use exastash::db::storage::gdrive::file::GdriveFile;
use exastash::db::inode::{InodeId, Inode, File, Dir, NewDir, Symlink, NewSymlink};
use exastash::db::dirent::{Dirent, InodeTuple};
use exastash::db::google_auth::{GsuiteApplicationSecret, GsuiteServiceAccount};
use exastash::db::traversal::walk_path;
use exastash::fuse;
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

    /// FUSE
    #[structopt(name = "fuse")]
    Fuse(FuseCommand),
}

#[derive(StructOpt, Debug)]
enum DirCommand {
    /// Create an unparented directory (for e.g. use as a root inode) and print its id to stdout
    #[structopt(name = "create")]
    Create {
        #[structopt(name = "PARENT_DIR_ID")]
        parent_dir_id: i64,

        #[structopt(name = "BASENAME")]
        basename: String,
    },

    /// Print info in JSON format for zero or more dirs
    #[structopt(name = "info")]
    Info {
        /// dir id
        #[structopt(name = "ID")]
        ids: Vec<i64>,
    },
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

    /// Print info in JSON format for zero or more dirs
    #[structopt(name = "info")]
    Info {
        /// symlink id
        #[structopt(name = "ID")]
        ids: Vec<i64>,
    },
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

async fn resolve_path(transaction: &mut Transaction<'_, Postgres>, root: i64, path: &str) -> Result<InodeId> {
    let path_components: Vec<&str> = if path == "" {
        vec![]
    } else {
        path.split('/').collect()
    };
    walk_path(transaction, root, &path_components).await
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
                DirCommand::Info { ids } => {
                    let dirs = Dir::find_by_ids(&mut transaction, &ids).await?;
                    let mut map: HashMap<i64, Dir> = dirs.into_iter().map(|dir| (dir.id, dir)).collect();
                    for id in ids {
                        let dir = map.remove(&id)
                            .ok_or_else(|| anyhow!("dir with id={} not in database, or duplicate id given", id))?;
                        println!("{}", json_info(&mut transaction, Inode::Dir(dir)).await?);
                    }
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
                FileCommand::Info { ids } => {
                    let files = File::find_by_ids(&mut transaction, &ids).await?;
                    let mut map: HashMap<i64, File> = files.into_iter().map(|file| (file.id, file)).collect();
                    for id in ids {
                        let file = map.remove(&id)
                            .ok_or_else(|| anyhow!("file with id={} not in database, or duplicate id given", id))?;
                        println!("{}", json_info(&mut transaction, Inode::File(file)).await?);
                    }
                }
                FileCommand::Content(content) => {
                    match content {
                        ContentCommand::Read { id } => {
                            let files = File::find_by_ids(&mut transaction, &[id]).await?;
                            ensure!(files.len() == 1, "no such file with id={}", id);
                            let file = &files[0];
        
                            let storages = get_storage(&mut transaction, &[id]).await?;
                            match storages.get(0) {
                                Some(storage) => {
                                    let stream = storage_read::read(&file, &storage).await?;
                                    let mut read = stream
                                        .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                                        .into_async_read()
                                        .compat();
                                    let mut stdout = tokio::io::stdout();
                                    tokio::io::copy(&mut read, &mut stdout).await?;
                                }
                                None => bail!("file with id={} has no storage", id)
                            }
                        }
                    }
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
                SymlinkCommand::Info { ids } => {
                    let symlinks = Symlink::find_by_ids(&mut transaction, &ids).await?;
                    let mut map: HashMap<i64, Symlink> = symlinks.into_iter().map(|symlink| (symlink.id, symlink)).collect();
                    for id in ids {
                        let symlink = map.remove(&id)
                            .ok_or_else(|| anyhow!("symlink with id={} not in database, or duplicate id given", id))?;
                        println!("{}", json_info(&mut transaction, Inode::Symlink(symlink)).await?);
                    }
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
                        let stream = storage_read::stream_gdrive_file(gdrive_file, *domain_id).await?;
                        let mut read = stream
                            .map_err(|e: Error| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                            .into_async_read()
                            .compat();
                        let mut stdout = tokio::io::stdout();
                        tokio::io::copy(&mut read, &mut stdout).await?;
                    }
                }
            }
        }
        ExastashCommand::Fuse(command) => {
            match &command {
                FuseCommand::Run { mountpoint } => {
                    fuse::run(mountpoint.into()).await?;
                }
            }
        }
    };

    Ok(())
}
