use async_recursion::async_recursion;
use anyhow::{anyhow, bail, ensure, Error, Result};
use structopt::StructOpt;
use chrono::Utc;
use tokio::fs;
use tokio_postgres::Transaction;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use serde::Serialize;
use chrono::DateTime;
use tracing_subscriber::EnvFilter;
use exastash::db;
use exastash::db::storage::{Storage, get_storage};
use exastash::db::storage::gdrive::file::GdriveOwner;
use exastash::db::inode::{InodeId, Inode, File, Dir, Symlink, Birth};
use exastash::db::google_auth::{GsuiteApplicationSecret, GsuiteAccessToken, GsuiteServiceAccount};
use exastash::db::traversal::walk_path;
use exastash::storage_read;
use futures::stream::TryStreamExt;
use yup_oauth2::{ApplicationSecret, InstalledFlowAuthenticator, InstalledFlowReturnMethod, ServiceAccountKey};


#[derive(StructOpt, Debug)]
#[structopt(name = "es")]
#[structopt(help_message = "Print help information")]
#[structopt(version_message = "Print version information")]
/// exastash
enum ExastashCommand {
    /// Subcommands to work with dirs
    #[structopt(name = "dir")]
    Dir(DirCommand),

    /// Subcommands to work with dirents
    #[structopt(name = "dirent")]
    Dirent(DirentCommand),

    #[structopt(name = "info")]
    /// Show info for a dir, file, or symlink
    Info {
        #[structopt(flatten)]
        selector: InodeSelector,
    },

    #[structopt(name = "cat")]
    /// Output a file's content to stdout
    Cat {
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

    #[structopt(name = "gsuite")]
    /// G Suite-related commands
    Gsuite(GsuiteCommand),
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
    async fn to_inode_id(&self, transaction: &mut Transaction<'_>) -> Result<InodeId> {
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
                walk_path(transaction, root, &path_components).await?
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
    #[structopt(name = "app-secret")]
    /// Manage OAuth 2.0 application secrets (used with the "installed" application flow)
    ApplicationSecret(ApplicationSecretCommand),

    #[structopt(name = "access-token")]
    /// Manage OAuth 2.0 access tokens
    AccessToken(AccessTokenCommand),

    #[structopt(name = "service-account")]
    /// Manage Google service accounts
    ServiceAccount(ServiceAccountCommand),
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
                    let dir_id = db::inode::NewDir { mtime, birth }.create(&mut transaction).await?;
                    transaction.commit().await?;
                    println!("{}", dir_id);
                }
            }
        }
        ExastashCommand::Dirent(dirent) => {
            match dirent {
                DirentCommand::Create { parent_dir_id, basename, child_dir, child_file, child_symlink } => {
                    let child = db::dirent::InodeTuple(child_dir, child_file, child_symlink).to_inode_id()?;
                    let dirent = db::dirent::Dirent::new(parent_dir_id, basename, child);
                    dirent.create(&mut transaction).await?;
                    transaction.commit().await?;
                }
            }
        }
        ExastashCommand::Ls { just_names, selector } => {
            let inode_id = selector.to_inode_id(&mut transaction).await?;
            let dirents = db::dirent::list_dir(&mut transaction, inode_id.dir_id()?).await?;
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
            let dir_id = selector.to_inode_id(&mut transaction).await?.dir_id()?;
            find(&mut transaction, &[], dir_id).await?;
        }
        ExastashCommand::Info { selector } => {
            let inode_id = selector.to_inode_id(&mut transaction).await?;

            let mut inodes = Inode::find_by_inode_ids(&mut transaction, &[inode_id]).await?;
            assert!(inodes.len() <= 1);
            if inodes.is_empty() {
                bail!("inode {:?} does not exist in database", inode_id);
            }
            let inode = inodes.pop().unwrap();

            #[derive(Serialize)]
            struct FileWithStorages {
                id: i64,
                mtime: DateTime<Utc>,
                birth: Birth,
                size: i64,
                executable: bool,
                storages: Vec<Storage>,
            }

            #[derive(Serialize)]
            #[serde(tag = "type")]
            enum InodeWithStorages {
                #[serde(rename = "dir")]
                Dir(Dir),
                #[serde(rename = "file")]
                File(FileWithStorages),
                #[serde(rename = "symlink")]
                Symlink(Symlink),
            }

            let inode = match inode {
                Inode::File(file) => {
                    let storages = get_storage(&mut transaction, &[file.id]).await?;
                    InodeWithStorages::File(FileWithStorages {
                        id: file.id,
                        mtime: file.mtime,
                        birth: file.birth,
                        size: file.size,
                        executable: file.executable,
                        storages,
                    })
                }
                Inode::Dir(dir) => InodeWithStorages::Dir(dir),
                Inode::Symlink(symlink) => InodeWithStorages::Symlink(symlink),
            };

            let j = serde_json::to_string_pretty(&inode)?;
            println!("{}", j);
        }
        ExastashCommand::Cat { selector } => {
            let inode_id = selector.to_inode_id(&mut transaction).await?;
            match inode_id {
                InodeId::File(file_id) => {
                    let files = File::find_by_ids(&mut transaction, &[file_id]).await?;
                    ensure!(files.len() == 1, "No such file with id={}", file_id);
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
                        None => bail!("File with id={} has no storage", file_id)
                    }
                }
                InodeId::Dir(_) => {
                    bail!("Cannot cat a dir");
                }
                InodeId::Symlink(_) => {
                    // TODO
                    bail!("Cannot cat a symlink");
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
                            let secret = GsuiteApplicationSecret { domain_id: *domain_id, secret: json };
                            secret.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GsuiteCommand::AccessToken(command) => {
                    match command {
                        AccessTokenCommand::Create { owner_id } => {
                            let owners = GdriveOwner::find_by_owner_ids(&mut transaction, &[*owner_id]).await?;
                            if owners.is_empty() {
                                bail!("owner id {} not in database", owner_id);
                            }
                            let owner = &owners[0];
                            let secrets = GsuiteApplicationSecret::find_by_domain_ids(&mut transaction, &[owner.domain]).await?;
                            if secrets.is_empty() {
                                bail!("application secret not in database for domain {}", owner.domain);
                            }
                            let secret = secrets[0].secret["installed"].clone();
                            let app_secret: ApplicationSecret = serde_json::from_value(secret)?;
                            let auth = InstalledFlowAuthenticator::builder(app_secret, InstalledFlowReturnMethod::Interactive)
                                .build()
                                .await
                                .unwrap();
                            let scopes = &["https://www.googleapis.com/auth/drive"];
                            let token = auth.token(scopes).await?;
                            let info = token.info();
                            GsuiteAccessToken {
                                owner_id: *owner_id,
                                access_token: info.access_token.clone(),
                                refresh_token: info.refresh_token.clone().unwrap(),
                                expires_at: info.expires_at.unwrap(),
                            }.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
                GsuiteCommand::ServiceAccount(command) => {
                    match command {
                        ServiceAccountCommand::Import { owner_id, json_file } => {
                            let content = fs::read(json_file).await?;
                            let key: ServiceAccountKey = serde_json::from_slice(&content)?;
                            assert_eq!(key.key_type, Some("service_account".into()));
                            let account = GsuiteServiceAccount { owner_id: *owner_id, key };
                            account.create(&mut transaction).await?;
                            transaction.commit().await?;
                        }
                    }
                }
            }

        }
    };

    Ok(())
}
