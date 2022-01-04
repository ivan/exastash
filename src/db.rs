//! CRUD operations for exastash entities in PostgreSQL

pub mod inode;
pub mod dirent;
pub mod storage;
pub mod traversal;
pub mod google_auth;

use anyhow::Result;
use sqlx::Executor;
use log::LevelFilter;
use std::sync::Arc;
use sqlx::postgres::{PgPool, PgPoolOptions, PgConnectOptions};
use sqlx::{ConnectOptions, Postgres, Transaction};
use futures::future::{FutureExt, Shared};
use once_cell::sync::Lazy;
use std::pin::Pin;
use std::future::Future;
use std::time::Duration;
use std::process::Command;
use std::env;
use std::path::Path;
use crate::util::env_var;

/// Return a `PgPool` that connects to the given `postgres://` URI,
/// and starts all transactions in `search_path` = search_path and with
/// isolation level `REPEATABLE READ`.
pub async fn new_pgpool(uri: &str, max_connections: u32, connect_timeout_sec: u64, search_path: &str) -> Result<PgPool> {
    let search_path = Arc::new(String::from(search_path));
    let mut options: PgConnectOptions = uri.parse()?;
    // By default, sqlx logs statements that take > 1 sec as a warning
    options.log_slow_statements(LevelFilter::Info, Duration::from_secs(5));
    let pool = PgPoolOptions::new()
        .after_connect(move |conn| {
            let search_path = search_path.clone();
            Box::pin(async move {
                let stmt = format!("SET search_path TO {search_path}");
                conn.execute(stmt.as_str()).await?;

                // We generally want point-in-time consistency, e.g. when we do separate
                // reads on files and a storage table
                conn.execute("SET default_transaction_isolation TO 'repeatable read'").await?;

                Ok(())
            })
        })
        .connect_timeout(Duration::from_secs(connect_timeout_sec))
        .max_connections(max_connections)
        .connect_with(options).await?;
    Ok(pool)
}

/// PgPool Future initialized once by the first caller
static PGPOOL: Lazy<Shared<Pin<Box<dyn Future<Output=PgPool> + Send>>>> = Lazy::new(|| async {
    let database_uri = env_var("EXASTASH_POSTGRES_URI").unwrap();
    let max_connections = env::var("EXASTASH_POSTGRES_MAX_CONNECTIONS")
        .map(|s| s.parse::<u32>().expect("could not parse EXASTASH_POSTGRES_MAX_CONNECTIONS as a u32"))
        .unwrap_or(16); // default
    let connect_timeout_sec = env::var("EXASTASH_POSTGRES_CONNECT_TIMEOUT_SEC")
        .map(|s| s.parse::<u64>().expect("could not parse EXASTASH_POSTGRES_CONNECT_TIMEOUT_SEC as a u64"))
        .unwrap_or(30); // default

    // TODO: allow in tests but ensure 1) localhost URL 2) no 'production database' flag in db
    if cfg!(test) {
        panic!("Refusing to create pgpool to EXASTASH_POSTGRES_URI={} in tests", database_uri);
    }

    new_pgpool(&database_uri, max_connections, connect_timeout_sec, "public").await.unwrap()
}.boxed().shared());

/// Return the global `PgPool`.  It must not be used in more than one tokio runtime.
pub async fn pgpool() -> PgPool {
    PGPOOL.clone().await
}

/// Return the output of `SELECT nextval(...)` on some PostgreSQL sequence.
pub async fn nextval(transaction: &mut Transaction<'_, Postgres>, sequence: &str) -> Result<i64> {
    let row = sqlx::query_scalar_unchecked!("SELECT nextval($1)", sequence).fetch_one(transaction).await?;
    let id: i64 = row.unwrap();
    Ok(id)
}

/// Turn off synchronous_commit for this transaction, enabling writes to
/// faster, but with the increased possibility of losing the most recent
/// writes on server crashes.
pub async fn disable_synchronous_commit(transaction: &mut Transaction<'_, Postgres>) -> Result<()> {
    sqlx::query_unchecked!("SET LOCAL synchronous_commit TO OFF").execute(transaction).await?;
    Ok(())
}

// Test helper functions below are also used outside exastash

/// Return a PostgreSQL connection string to an ephemeralpg instance
pub fn postgres_temp_instance_uri() -> String {
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

/// Connect to PostgreSQL server at `uri` and apply SQL DDL from file `sql_file`
pub fn apply_ddl<P: AsRef<Path>>(uri: &str, cwd: P, sql_file: &str) {
    let mut command = Command::new("psql");
    let psql = command
        .current_dir(cwd)
        .arg("--no-psqlrc")
        .arg("--quiet")
        .arg("-f").arg(sql_file)
        .arg(uri);
    let code = psql.status().expect("failed to execute psql");
    if !code.success() {
        panic!("psql exited with code {:?}", code.code());
    }
}

/// Connect to PostgreSQL server at `uri` and apply exastash's SQL DDL
pub fn apply_exastash_ddl(uri: &str) {
    apply_ddl(uri, env!("CARGO_MANIFEST_DIR"), "schema/extensions.sql");
    apply_ddl(uri, env!("CARGO_MANIFEST_DIR"), "schema/schema.sql");
}

/// Note that TRUNCATE tests should be run on the secondary pool because they
/// will otherwise frequently cause other running transactions to raise
/// `deadlock detected`. That happens on the non-`TRUNCATE` transaction
/// frequently because we have a mutual FK set up between dirs and dirents.
pub async fn assert_cannot_truncate(transaction: &mut Transaction<'_, Postgres>, table: &str) {
    let statement = format!("TRUNCATE {table} CASCADE");
    let result = sqlx::query(&statement).execute(transaction).await;
    let msg = result.err().expect("expected an error").to_string();
    assert_eq!(msg, "error returned from database: truncate is forbidden");
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use once_cell::sync::Lazy;

    static PRIMARY_POOL_URI: Lazy<String> = Lazy::new(|| {
        let uri = postgres_temp_instance_uri();
        apply_exastash_ddl(&uri);
        uri
    });

    /// Return a new `PgPool` connected to the `pg_tmp` for most tests.
    /// We do not return a shared `PgPool` because each `#[tokio::test]` has its own tokio runtime.
    pub(crate) async fn new_primary_pool() -> PgPool {
        new_pgpool(&*PRIMARY_POOL_URI, 4, 30, "public").await.unwrap()
    }

    /// PgPool Future initialized once by the first caller
    static SECONDARY_POOL_URI: Lazy<String> = Lazy::new(|| {
        let uri = postgres_temp_instance_uri();
        apply_exastash_ddl(&uri);
        uri
    });

    /// Return a new `PgPool` connected to the pg_tmp for `TRUNCATE` tests.
    /// We do not return a shared `PgPool` because each `#[tokio::test]` has its own tokio runtime.
    pub(crate) async fn new_secondary_pool() -> PgPool {
        new_pgpool(&*SECONDARY_POOL_URI, 4, 30, "public").await.unwrap()
    }
}
