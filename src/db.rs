//! CRUD operations for exastash entities in PostgreSQL

pub mod inode;
pub mod dirent;
pub mod storage;
pub mod traversal;

use anyhow::Result;
use tokio_postgres::{Client, Transaction, NoTls};
use crate::util::env_var;

/// Return a `postgres::Client` connected to the `postgres://` URI in
/// env var `EXASTASH_POSTGRES_URI`.
pub async fn postgres_client_production() -> Result<Client> {
    let database_uri = env_var("EXASTASH_POSTGRES_URI")?;
    let (client, connection) = tokio_postgres::connect(&database_uri, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

/// Return a transaction with search_path set to 'stash' and isolation level REPEATABLE READ.
pub async fn start_transaction<'a>(client: &'a mut Client) -> Result<Transaction<'a>> {
    let transaction = client.build_transaction().start().await?;
    transaction.execute("SET search_path TO stash", &[]).await?;
    // We generally want point-in-time consistency, e.g. when we do separate
    // reads on files and a storage table
    transaction.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[]).await?;
    Ok(transaction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use once_cell::sync::{OnceCell, Lazy};
    use postgres::NoTls;

    fn postgres_temp_instance_uri() -> String {
        let mut command = Command::new("pg_tmp");
        let stdout = command.output().expect("failed to execute pg_tmp").stdout;
        let database_uri = String::from_utf8(stdout).expect("could not parse pg_tmp output as UTF-8");
        // Add a &user= to fix "no PostgreSQL user name specified in startup packet"
        let user = env_var("USER").unwrap();
        let database_uri = format!("{}&user={}", database_uri, user);
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

    static DATABASE_URI: Lazy<String> = Lazy::new(postgres_temp_instance_uri);
    static DDL_APPLIED: OnceCell<bool> = OnceCell::new();

    pub(crate) async fn get_client() -> Client {
        let uri = &*DATABASE_URI;
        DDL_APPLIED.get_or_init(|| {
            apply_ddl(uri, "schema/schema.sql");
            true
        });

        let (client, connection) = tokio_postgres::connect(&uri, NoTls).await.unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        client
    }

    pub(crate) async fn assert_cannot_truncate(transaction: &mut Transaction<'_>, table: &str) {
        let statement = format!("TRUNCATE {} CASCADE", table);
        let result = transaction.execute(statement.as_str(), &[]).await;
        let msg = result.err().expect("expected an error").to_string();
        // Also allow "deadlock detected" because of concurrent transactions: the
        // BEFORE TRUNCATE trigger does not run before PostgreSQL's lock checks
        assert!(
            msg == "db error: ERROR: truncate is forbidden" ||
            msg == "db error: ERROR: deadlock detected", msg);
    }

    #[tokio::test]
    async fn test_start_transaction() -> Result<()> {
        let mut client = get_client().await;
        let _ = start_transaction(&mut client).await?;
        Ok(())
    }
}
