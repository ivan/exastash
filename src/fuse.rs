//! FUSE server

use polyfuse::{
    io::{Reader, Writer},
    op,
    reply::{ReplyAttr, ReplyEntry},
    Context, DirEntry, FileAttr, Filesystem, Operation,
};
use std::{io, path::PathBuf, time::Duration};
use std::ffi::OsStr;
use crate::db;
use crate::db::dirent::Dirent;
use crate::db::inode::{InodeId, Dir, File, Symlink};

fn ino_to_inodeid(ino: u64) -> Option<InodeId> {
    // Slice up the 2^63 space into 9223 quadrillion-sized regions for different types
    match ino {
        n if n < 1 => None,
        n if n < 1_000_000_000_000_000 => Some(InodeId::Dir(n as i64)),
        n if n < 2_000_000_000_000_000 => Some(InodeId::File(n as i64 - 1_000_000_000_000_000)),
        n if n < 3_000_000_000_000_000 => Some(InodeId::Symlink(n as i64 - 2_000_000_000_000_000)),
        _ => None,
    }
}

fn inodeid_to_ino(inode: InodeId) -> u64 {
    match inode {
        InodeId::Dir(id)     => id as u64,
        InodeId::File(id)    => id as u64 + 1_000_000_000_000_000,
        InodeId::Symlink(id) => id as u64 + 2_000_000_000_000_000,
    }
}

/// Create a `DirEntry` for a symbolic link.
fn symlink(name: impl AsRef<OsStr>, ino: u64, off: u64) -> DirEntry {
    let mut ent = DirEntry::new(name, ino, off);
    ent.set_typ(libc::DT_LNK as u32);
    ent
}

const TTL: Duration = Duration::from_secs(0);
const ROOT_INO: u64 = 1;
const HELLO_INO: u64 = 2;
const HELLO_FILENAME: &str = "hello.txt";
const HELLO_CONTENT: &[u8] = b"Hello, world!\n";

/// Run the FUSE server
pub async fn run(mountpoint: PathBuf) -> anyhow::Result<()> {
    anyhow::ensure!(mountpoint.is_dir(), "the mountpoint must be a directory");
    polyfuse_tokio::mount(Server::new(), mountpoint, &[]).await?;
    Ok(())
}

struct Server {
    root_attr: FileAttr,
    hello_attr: FileAttr,
}

impl Server {
    fn new() -> Self {
        let root_attr = root_attr();
        let mut hello_attr = hello_attr();
        hello_attr.set_size(HELLO_CONTENT.len() as u64);

        Self {
            root_attr,
            hello_attr,
        }
    }

    async fn do_lookup<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Lookup<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let parent = op.parent();
        let basename = op.name().to_str();
        if basename.is_none() {
            // All files in database have UTF-8 filenames
            cx.reply_err(libc::ENOENT).await?
        }

        let pool = db::pgpool().await;
        let mut transaction = pool.begin().await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let child = Dirent::find_by_parent_and_basename(&mut transaction, parent as i64, basename.unwrap()).await
            .map_err(|e| io::Error::new(io::ErrorKind::NotFound, e.to_string()))?
            .map(|dirent| dirent.child);
        drop(transaction);
        drop(pool);

        match child {
            None => cx.reply_err(libc::ENOENT).await?,
            Some(child) => {
                let ino = inodeid_to_ino(child);

                let mut attr = FileAttr::default();
                attr.set_ino(ino);
                attr.set_uid(unsafe { libc::getuid() });
                attr.set_gid(unsafe { libc::getgid() });
                match child {
                    InodeId::Dir(_) => {
                        attr.set_mode(libc::S_IFDIR | 0o555);
                        // It's OK to report nlinks=1 for all dirs instead of doing legacy
                        // `2 + number of directory children`. btrfs always reports 1:
                        // https://www.spinics.net/lists/linux-btrfs/msg98932.html
                        attr.set_nlink(1);
                    }
                    InodeId::File(_) => {
                        attr.set_mode(libc::S_IFREG | 0o555);
                        // TODO: add support for a mode that returns an accurate number of
                        // 'number of other rows that reference the file'.
                        attr.set_nlink(1);
                    }
                    InodeId::Symlink(_) => {
                        attr.set_mode(libc::S_IFLNK | 0o555);
                        attr.set_nlink(1);
                    }
                }

                cx.reply(
                    ReplyEntry::default()
                        .ino(ino)
                        .attr(attr)
                        .ttl_attr(TTL)
                        .ttl_entry(TTL),
                )
                .await?;
            }
        }

        Ok(())
    }

    async fn do_getattr<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Getattr<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let ino = op.ino();

        let pool = db::pgpool().await;
        let mut transaction = pool.begin().await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let inode = ino_to_inodeid(ino);
        if inode.is_none() {
            return cx.reply_err(libc::ENOENT).await;
        }

        let mut attr = FileAttr::default();
        attr.set_ino(ino);
        attr.set_nlink(1);
        attr.set_uid(unsafe { libc::getuid() });
        attr.set_gid(unsafe { libc::getgid() });

        match inode.unwrap() {
            InodeId::Dir(id) => {
                let dir = Dir::find_by_ids(&mut transaction, &[id]).await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                    .pop();
                if dir.is_none() {
                    return cx.reply_err(libc::ENOENT).await;
                }
                let dir = dir.unwrap();
                attr.set_mode(libc::S_IFDIR | 0o550);
                attr.set_mtime(dir.mtime.into());
            },
            InodeId::File(id) => {
                let file = File::find_by_ids(&mut transaction, &[id]).await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                    .pop();
                if file.is_none() {
                    return cx.reply_err(libc::ENOENT).await;
                }
                let file = file.unwrap();
                attr.set_mode(libc::S_IFREG | 0o440);
                attr.set_size(file.size as u64);
                attr.set_mtime(file.mtime.into());
            },
            InodeId::Symlink(id) => {
                let symlink = Symlink::find_by_ids(&mut transaction, &[id]).await
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
                    .pop();
                if symlink.is_none() {
                    return cx.reply_err(libc::ENOENT).await;
                }
                let symlink = symlink.unwrap();
                attr.set_mode(libc::S_IFLNK | 0o440);
                attr.set_mtime(symlink.mtime.into());
            },
        }
        drop(transaction);
        drop(pool);

        cx.reply(ReplyAttr::new(attr).ttl_attr(TTL)).await?;

        Ok(())
    }

    async fn do_read<T: ?Sized>(&self, cx: &mut Context<'_, T>, op: op::Read<'_>) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        match op.ino() {
            ROOT_INO => return cx.reply_err(libc::EISDIR).await,
            HELLO_INO => (),
            _ => return cx.reply_err(libc::ENOENT).await,
        }

        let offset = op.offset() as usize;
        if offset >= HELLO_CONTENT.len() {
            return cx.reply(&[]).await;
        }

        let size = op.size() as usize;
        let data = &HELLO_CONTENT[offset..];
        let data = &data[..std::cmp::min(data.len(), size)];
        cx.reply(data).await?;

        Ok(())
    }

    async fn do_readdir<T: ?Sized>(
        &self,
        cx: &mut Context<'_, T>,
        op: op::Readdir<'_>,
    ) -> io::Result<()>
    where
        T: Writer + Unpin,
    {
        let offset = op.offset() as usize;
        let size = op.size() as usize;

        let pool = crate::db::pgpool().await;
        let mut transaction = pool.begin().await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // TODO cache these results somewhere (key on some cx id?) so that we get consistent results on second non-0 offset request
        // TODO test if we even need to return results more than once (assert offset 0?)
        let dirents = Dirent::find_by_parents(&mut transaction, &[op.ino() as i64]).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // TODO return error if inode doesn't exist
        // if op.ino() != ROOT_INO {
        //     return cx.reply_err(libc::ENOTDIR).await;
        // }

        let parent_dirent = Dirent::find_by_child_dir(&mut transaction, op.ino() as i64).await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "dir did not have a parent?"))?;

        drop(transaction);
        drop(pool);

        let parent_ino = inodeid_to_ino(InodeId::Dir(parent_dirent.parent));
        let mut dir_entries = Vec::with_capacity(dirents.len() + 2);
        dir_entries.push(DirEntry::dir(".", op.ino(), 1));
        dir_entries.push(DirEntry::dir("..", parent_ino, 2));
        let mut next_idx = 3_u64;
        for dirent in dirents.iter() {
            let ino = inodeid_to_ino(dirent.child);
            match dirent.child {
                InodeId::Dir(_)     => dir_entries.push(DirEntry::dir(&dirent.basename, ino, next_idx)),
                InodeId::File(_)    => dir_entries.push(DirEntry::file(&dirent.basename, ino, next_idx)),
                InodeId::Symlink(_) => dir_entries.push(symlink(&dirent.basename, ino, next_idx)),
            }
            next_idx += 1;
        }

        let mut entries = Vec::with_capacity(dir_entries.len() - offset);
        let mut total_len = 0usize;
        for entry in dir_entries.iter().skip(offset) {
            let entry = entry.as_ref();
            if total_len + entry.len() > size {
                break;
            }
            entries.push(entry);
            total_len += entry.len();
        }

        cx.reply(entries).await?;

        Ok(())
    }
}

#[polyfuse::async_trait]
impl Filesystem for Server {
    async fn call<'a, 'cx, T: ?Sized>(
        &'a self,
        cx: &'a mut Context<'cx, T>,
        op: Operation<'cx>,
    ) -> io::Result<()>
    where
        T: Reader + Writer + Unpin + Send,
    {
        match op {
            Operation::Lookup(op) => self.do_lookup(cx, op).await,
            Operation::Getattr(op) => self.do_getattr(cx, op).await,
            Operation::Read(op) => self.do_read(cx, op).await,
            Operation::Readdir(op) => self.do_readdir(cx, op).await,
            _ => Ok(()),
        }
    }
}

fn root_attr() -> FileAttr {
    let mut attr = FileAttr::default();
    attr.set_mode(libc::S_IFDIR | 0o555);
    attr.set_ino(ROOT_INO);
    attr.set_nlink(2);
    attr.set_uid(unsafe { libc::getuid() });
    attr.set_gid(unsafe { libc::getgid() });
    attr
}

fn hello_attr() -> FileAttr {
    let mut attr = FileAttr::default();
    attr.set_mode(libc::S_IFREG | 0o444);
    attr.set_ino(HELLO_INO);
    attr.set_nlink(1);
    attr.set_uid(unsafe { libc::getuid() });
    attr.set_gid(unsafe { libc::getgid() });
    attr
}
