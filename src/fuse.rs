//! FUSE server

#![allow(clippy::unnecessary_mut_passed)]
#![deny(clippy::unimplemented)]

use polyfuse::{
    io::{Reader, Writer},
    op,
    reply::{ReplyAttr, ReplyEntry},
    Context, DirEntry, FileAttr, Filesystem, Operation,
};
use std::{io, os::unix::prelude::*, path::PathBuf, time::Duration};

const TTL: Duration = Duration::from_secs(60 * 60 * 24 * 365);
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
    dir_entries: Vec<DirEntry>,
}

impl Server {
    fn new() -> Self {
        let root_attr = root_attr();
        let mut hello_attr = hello_attr();
        hello_attr.set_size(HELLO_CONTENT.len() as u64);

        let dir_entries = {
            let mut entries = Vec::with_capacity(3);
            entries.push(DirEntry::dir(".", 1, 1));
            entries.push(DirEntry::dir("..", 1, 2));
            entries.push(DirEntry::file(HELLO_FILENAME, 2, 3));
            entries
        };

        Self {
            root_attr,
            hello_attr,
            dir_entries,
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
        match op.parent() {
            ROOT_INO if op.name().as_bytes() == HELLO_FILENAME.as_bytes() => {
                cx.reply(
                    ReplyEntry::default()
                        .ino(HELLO_INO)
                        .attr(self.hello_attr) //
                        .ttl_attr(TTL)
                        .ttl_entry(TTL),
                )
                .await?;
            }
            _ => cx.reply_err(libc::ENOENT).await?,
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
        let attr = match op.ino() {
            ROOT_INO => self.root_attr,
            HELLO_INO => self.hello_attr,
            _ => return cx.reply_err(libc::ENOENT).await,
        };

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
        if op.ino() != ROOT_INO {
            return cx.reply_err(libc::ENOTDIR).await;
        }

        let offset = op.offset() as usize;
        let size = op.size() as usize;

        let mut entries = Vec::with_capacity(3);
        let mut total_len = 0usize;
        for entry in self.dir_entries.iter().skip(offset) {
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
