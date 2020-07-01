use tracing::info;
use async_recursion::async_recursion;
use anyhow::{anyhow, bail, ensure, Error, Result};
use structopt::StructOpt;
use chrono::Utc;
use futures::future::FutureExt;
use tokio::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tokio_postgres::Transaction;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tracing_subscriber::EnvFilter;
use exastash::db;
use exastash::db::storage::get_storage;
use exastash::db::storage::gdrive::file::GdriveFile;
use exastash::db::inode::{InodeId, Inode, File, Dir, Symlink};
use exastash::db::google_auth::{GsuiteApplicationSecret, GsuiteServiceAccount};
use exastash::db::traversal::walk_path;
use exastash::info::json_info;
use exastash::oauth;
use exastash::{storage_read, storage_write};
use futures::stream::TryStreamExt;
use yup_oauth2::ServiceAccountKey;


#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
#[structopt(help_message = "Print help information")]
#[structopt(version_message = "Print version information")]
/// exastash
enum ExastashCommand {
    /// Commands to work with dirs
    #[structopt(name = "dir")]
    Dir(DirCommand),

    /// Commands to work with files
    #[structopt(name = "file")]
    File(FileCommand),

    /// Commands to work with symlinks
    #[structopt(name = "symlink")]
    Symlink(SymlinkCommand),

    /// Commands to work with dirents
    #[structopt(name = "dirent")]
    Dirent(DirentCommand),

    #[structopt(name = "gsuite")]
    /// Commands to work with G Suite
    Gsuite(GsuiteCommand),

    #[structopt(name = "internal")]
    /// Internal commands for debugging
    Internal(InternalCommand),
}

async fn resolve_path(transaction: &mut Transaction<'_>, root: Option<i64>, path: &str) -> Result<InodeId> {
    let root = root.ok_or_else(|| anyhow!("If path is specified, root dir id must also be specified"))?;
    let path_components: Vec<&str> = if path == "" {
        vec![]
    } else {
        path.split('/').collect()
    };
    walk_path(transaction, root, &path_components).await
}

#[derive(StructOpt, Debug)]
struct PathOrFileId {
    /// file id.
    /// One of {file id, path} must be given.
    #[structopt(long, short = "i")]
    file_id: Option<i64>,

    /// Path consisting only of slash-separated basenames, no leading / or . or ..
    /// One of {file id, path} must be given.
    #[structopt(long, short = "p")]
    path: Option<String>,

    /// A dir id specifying the dir from which to start path traversal.
    /// Must be provided if path is provided.
    #[structopt(long, short = "r")]
    root: Option<i64>,
}

impl PathOrFileId {
    async fn to_file_id(&self, transaction: &mut Transaction<'_>) -> Result<i64> {
        Ok(match (self.file_id, &self.path) {
            (Some(id), None) => id,
            (None, Some(path)) => resolve_path(transaction, self.root, path).await?.file_id()?,
            _ => bail!("either file_id or path must be specified but not both"),
        })
    }
}

#[derive(StructOpt, Debug)]
struct PathOrDirId {
    /// dir id.
    /// One of {dir id, path} must be given.
    #[structopt(long, short = "i")]
    dir_id: Option<i64>,

    /// Path consisting only of slash-separated basenames, no leading / or . or ..
    /// One of {dir id, path} must be given.
    #[structopt(long, short = "p")]
    path: Option<String>,

    /// A dir id specifying the dir from which to start path traversal.
    /// Must be provided if path is provided.
    #[structopt(long, short = "r")]
    root: Option<i64>,
}

impl PathOrDirId {
    async fn to_dir_id(&self, transaction: &mut Transaction<'_>) -> Result<i64> {
        Ok(match (self.dir_id, &self.path) {
            (Some(id), None) => id,
            (None, Some(path)) => resolve_path(transaction, self.root, path).await?.dir_id()?,
            _ => bail!("either dir_id or path must be specified but not both"),
        })
    }
}

#[derive(StructOpt, Debug)]
struct PathOrSymlinkId {
    /// symlink id.
    /// One of {symlink id, path} must be given.
    #[structopt(long, short = "i")]
    symlink_id: Option<i64>,

    /// Path consisting only of slash-separated basenames, no leading / or . or ..
    /// One of {symlink id, path} must be given.
    #[structopt(long, short = "p")]
    path: Option<String>,

    /// A dir id specifying the dir from which to start path traversal.
    /// Must be provided if path is provided.
    #[structopt(long, short = "r")]
    root: Option<i64>,
}

impl PathOrSymlinkId {
    async fn to_symlink_id(&self, transaction: &mut Transaction<'_>) -> Result<i64> {
        Ok(match (self.symlink_id, &self.path) {
            (Some(id), None) => id,
            (None, Some(path)) => resolve_path(transaction, self.root, path).await?.symlink_id()?,
            _ => bail!("either symlink_id or path must be specified but not both"),
        })
    }
}

#[derive(StructOpt, Debug)]
enum DirCommand {
    /// Create an unparented directory (for e.g. use as a root inode) and print its id to stdout
    #[structopt(name = "create")]
    Create,

    #[structopt(name = "info")]
    /// Show info for a dir
    Info {
        #[structopt(flatten)]
        selector: PathOrDirId,
    },

    #[structopt(name = "ls")]
    /// List a dir, file, or symlink
    Ls {
        #[structopt(short = "j")]
        /// Print just the filenames
        just_names: bool,

        #[structopt(flatten)]
        selector: PathOrDirId,
    },

    #[structopt(name = "find")]
    /// Recursively list a dir, like `find`
    /// This cannot start at a file or symlink because it may have multiple names.
    Find {
        #[structopt(flatten)]
        selector: PathOrDirId,
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

    /// Show info for a dir
    #[structopt(name = "info")]
    Info {
        #[structopt(flatten)]
        selector: PathOrFileId,
    },

    /// Commands for working with file content
    #[structopt(name = "content")]
    Content(ContentCommand),
}

#[derive(StructOpt, Debug)]
enum ContentCommand {
    #[structopt(name = "read")]
    /// Output a file's content to stdout
    Read {
        #[structopt(flatten)]
        selector: PathOrFileId,
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

    #[structopt(name = "info")]
    /// Show info for a symlink
    Info {
        #[structopt(flatten)]
        selector: PathOrSymlinkId,
    },
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
    /// Write a sequence of Google Drive files to stdout
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

#[async_recursion]
async fn find(transaction: &mut Transaction<'_>, segments: &[&str], dir_id: i64) -> Result<()> {
    let path_string = match segments {
        [] => "".into(),
        parts => format!("{}/", parts.join("/")),
    };
    let dirents = db::dirent::list_dir(transaction, dir_id).await?;
    for dirent in dirents {
        println!("{}{}", path_string, dirent.basename);
        if let InodeId::Dir(dir_id) = dirent.child {
            let segments = [segments, &[&dirent.basename]].concat();
            find(transaction, &segments, dir_id).await?;
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
    let mut client = db::postgres_client_production().await?;
    let mut transaction = db::start_transaction(&mut client).await?;
    match cmd {
        ExastashCommand::Dir(dir) => {
            match dir {
                DirCommand::Create => {
                    let mtime = Utc::now();
                    let birth = db::inode::Birth::here_and_now();
                    let dir = db::inode::NewDir { mtime, birth }.create(&mut transaction).await?;
                    transaction.commit().await?;
                    println!("{}", dir.id);
                }
                DirCommand::Info { selector } => {
                    let id = selector.to_dir_id(&mut transaction).await?;
                    let mut dirs = Dir::find_by_ids(&mut transaction, &[id]).await?;
                    ensure!(!dirs.is_empty(), "dir id={:?} does not exist in database", id);
                    let dir = dirs.pop().unwrap();
                    println!("{}", json_info(&mut transaction, Inode::Dir(dir)).await?);
                }
                DirCommand::Ls { just_names, selector } => {
                    let dir_id = selector.to_dir_id(&mut transaction).await?;
                    let dirents = db::dirent::list_dir(&mut transaction, dir_id).await?;
                    for dirent in dirents {
                        if just_names {
                            println!("{}", dirent.basename);
                        } else {
                            // TODO: print: inode, size, mtime, filename[decoration]
                            println!("{}", dirent.basename);
                        }
                    }
                }
                DirCommand::Find { selector } => {
                    let dir_id = selector.to_dir_id(&mut transaction).await?;
                    find(&mut transaction, &[], dir_id).await?;
                }
            }
        }
        ExastashCommand::File(file) => {
            match file {
                FileCommand::Create { path, store_inline, store_gdrive } => {
                    let attr = fs::metadata(&path).await?;
                    let mtime = attr.modified()?.into();
                    let birth = db::inode::Birth::here_and_now();
                    let size = attr.len();
                    let permissions = attr.permissions();
                    let executable = permissions.mode() & 0o111 != 0;
                    let file = db::inode::NewFile { mtime, birth, size: size as i64, executable }.create(&mut transaction).await?;
                    if size > 0 && !store_inline && store_gdrive.is_empty() {
                        bail!("a file with size > 0 needs storage, please specify a --store- option");
                    }
                    if store_inline {
                        let content = fs::read(path.clone()).await?;
                        db::storage::inline::Storage { file_id: file.id, content }.create(&mut transaction).await?;
                    }
                    if !store_gdrive.is_empty() {
                        let file_stream_fn = |offset| {
                            // TODO: support non-0 offset if we implement upload retries
                            assert_eq!(offset, 0);
                            fs::read(path.clone()).into_stream()
                        };
                        for domain in store_gdrive {
                            storage_write::write_to_gdrive(&mut transaction, file_stream_fn, &file, domain).await?;
                        }
                    }
                    transaction.commit().await?;
                    println!("{}", file.id);
                }
                FileCommand::Info { selector } => {
                    let id = selector.to_file_id(&mut transaction).await?;
                    let mut files = File::find_by_ids(&mut transaction, &[id]).await?;
                    ensure!(!files.is_empty(), "file id={:?} does not exist in database", id);
                    let file = files.pop().unwrap();
                    println!("{}", json_info(&mut transaction, Inode::File(file)).await?);
                }
                FileCommand::Content(content) => {
                    match content {
                        ContentCommand::Read { selector } => {
                            let file_id = selector.to_file_id(&mut transaction).await?;
                            let files = File::find_by_ids(&mut transaction, &[file_id]).await?;
                            ensure!(files.len() == 1, "no such file with id={}", file_id);
                            let file = &files[0];
        
                            let storages = get_storage(&mut transaction, &[file_id]).await?;
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
                                None => bail!("file with id={} has no storage", file_id)
                            }
                        }
                    }
                }
            }
        }
        ExastashCommand::Symlink(symlink) => {
            match symlink {
                SymlinkCommand::Create { target } => {
                    todo!()
                }
                SymlinkCommand::Info { selector } => {
                    let id = selector.to_symlink_id(&mut transaction).await?;
                    let mut symlinks = Symlink::find_by_ids(&mut transaction, &[id]).await?;
                    ensure!(!symlinks.is_empty(), "symlink id={:?} does not exist in database", id);
                    let symlink = symlinks.pop().unwrap();
                    println!("{}", json_info(&mut transaction, Inode::Symlink(symlink)).await?);
                }
            }
        }
        ExastashCommand::Dirent(dirent) => {
            match dirent {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let child = db::dirent::InodeTuple(child_dir, child_file, child_symlink).to_inode_id()?;
                    db::dirent::Dirent::new(parent_dir_id, basename, child).create(&mut transaction).await?;
                    transaction.commit().await?;
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
                        oauth::refresh_access_tokens(&mut client).await?;
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
                    println!("{}", j);
                }
                InternalCommand::ReadGdriveFiles { domain_id, file_ids } => {
                    let gdrive_files = GdriveFile::find_by_ids_in_order(&mut transaction, file_ids).await?;
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
    };

    Ok(())
}
