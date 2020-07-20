//! CRUD operations for exastash entities in PostgreSQL

pub mod inode;
pub mod dirent;
pub mod storage;
pub mod traversal;
pub mod google_auth;

use anyhow::Result;
use sqlx::Executor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use futures::future::{FutureExt, Shared};
use once_cell::sync::Lazy;
use std::pin::Pin;
use std::future::Future;
use std::time::Duration;
use std::env;
use crate::util::env_var;

/// Return a `PgPool` that connects to the given `postgres://` URI,
/// and starts all transactions in `search_path` = `stash` and with
/// isolation level `REPEATABLE READ`.
pub async fn new_pgpool(uri: &str, max_connections: u32) -> Result<PgPool> {
    Ok(
        PgPoolOptions::new()
        .after_connect(|conn| Box::pin(async move {
            conn.execute("SET search_path TO stash;").await?;
            // We generally want point-in-time consistency, e.g. when we do separate
            // reads on files and a storage table
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;").await?;

            Ok(())
        }))
        .connect_timeout(Duration::from_secs(30))
        .max_connections(max_connections)
        .connect(&uri).await?
    )
}

/// PgPool Future initialized once by the first caller
static PGPOOL: Lazy<Shared<Pin<Box<dyn Future<Output=PgPool> + Send>>>> = Lazy::new(|| async {
    let database_uri = env_var("EXASTASH_POSTGRES_URI").unwrap();
    let max_connections = env::var("EXASTASH_POSTGRES_MAX_CONNECTIONS")
        .map(|s| s.parse::<u32>().expect("could not parse EXASTASH_POSTGRES_MAX_CONNECTIONS as a u32"))
        .unwrap_or(16); // default
    new_pgpool(&database_uri, max_connections).await.unwrap()
}.boxed().shared());

/// Return the global `PgPool`.  It must not be used in more than one tokio runtime.
pub async fn pgpool() -> PgPool {
    PGPOOL.clone().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use once_cell::sync::Lazy;
    use sqlx::{Postgres, Transaction};

    fn postgres_temp_instance_uri() -> String {
        let mut command = Command::new("pg_tmp");
        // "Shut down and remove the database after the specified timeout. If one or more clients
        // are still connected then pg_tmp sleeps and retries again after the same interval."
        let timeout = 10;
        let args = &["-w", &timeout.to_string()];
        let stdout = command.args(args).output().expect("failed to execute pg_tmp").stdout;
        let database_uri = String::from_utf8(stdout).expect("could not parse pg_tmp output as UTF-8");
        // Add a &user= to fix "no PostgreSQL user name specified in startup packet"
        let user = env_var("USER").unwrap();
        let database_uri = format!("{database_uri}&user={user}");
        database_uri
    }

    fn apply_ddl(uri: &str, sql_file: &str) {
        let mut command = Command::new("psql");
        let psql = command
            .arg("--no-psqlrc")
            .arg("--quiet")
            .arg("-f").arg(sql_file)
            .arg(uri);
        let code = psql.status().expect("failed to execute psql");
        if !code.success() {
            panic!("psql exited with code {:?}", code.code());
        }
    }

    static PRIMARY_POOL_URI: Lazy<String> = Lazy::new(|| {
        let uri = postgres_temp_instance_uri();
        apply_ddl(&uri, "schema/schema.sql");
        uri
    });

    /// Return a new `PgPool` connected to the `pg_tmp` for most tests.
    /// We do not return a shared `PgPool` because each `#[tokio::test]` has its own tokio runtime.
    pub(crate) async fn new_primary_pool() -> PgPool {
        new_pgpool(&*PRIMARY_POOL_URI, 4).await.unwrap()
    }

    /// PgPool Future initialized once by the first caller
    static SECONDARY_POOL_URI: Lazy<String> = Lazy::new(|| {
        let uri = postgres_temp_instance_uri();
        apply_ddl(&uri, "schema/schema.sql");
        uri
    });

    /// Return a new `PgPool` connected to the pg_tmp for `TRUNCATE` tests.
    /// We do not return a shared `PgPool` because each `#[tokio::test]` has its own tokio runtime.
    pub(crate) async fn new_secondary_pool() -> PgPool {
        new_pgpool(&*SECONDARY_POOL_URI, 4).await.unwrap()
    }

    /// Note that TRUNCATE tests should be run on the secondary pool because they
    /// will otherwise frequently cause other running transactions to raise
    /// `deadlock detected`. That happens on the non-`TRUNCATE` transaction
    /// frequently because we have a mutual FK set up between dirs and dirents.
    pub(crate) async fn assert_cannot_truncate(transaction: &mut Transaction<'_, Postgres>, table: &str) {
        let statement = format!("TRUNCATE {table} CASCADE");
        let result = sqlx::query(&statement).execute(transaction).await;
        let msg = result.err().expect("expected an error").to_string();
        assert_eq!(msg, "error returned from database: truncate is forbidden");
    }
}
