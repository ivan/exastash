use std::env;
use anyhow::{anyhow, Context, Result};
use postgres::{Client, Transaction, NoTls, IsolationLevel};
use chrono::{DateTime, Utc};

fn env_var(var: &str) -> Result<String> {
    env::var(var).with_context(|| anyhow!("Could not get variable {:?} from environment", var))
}

fn postgres_client_production() -> Result<Client> {
    let database_uri = env_var("EXASTASH_POSTGRESQL_URI")?;
    Ok(Client::connect(&database_uri, NoTls)?)
}

/// birth_time, birth_version, and birth_hostname for a dir/file/symlink
pub(crate) struct Birth {
    /// The time at which a dir, file, or symlink was created
    time: DateTime<Utc>,
    /// The exastash version with which a dir, file, or symlink was a created
    version: u16,
    /// The hostname of the machine on which a dir, file, or symlink was a created
    hostname: String,
}

fn start_transaction(client: &mut Client) -> Result<Transaction> {
    // PostgreSQL's default Read Committed isolation level allows for too many
    // anomalies, e.g. "two successive SELECT commands can see different data"
    // https://www.postgresql.org/docs/12/transaction-iso.html
    //
    // The foreign-key-on-array-elements implementation in storage_gdrive.sql
    // may require Serializable isolation; it hasn't been tested otherwise.
    let mut transaction = client.build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()?;
    transaction.execute("SET search_path TO stash", &[])?;
    Ok(transaction)
}

pub(crate) fn create_dir(client: &mut Client, mtime: DateTime<Utc>) -> Result<()> {
    let mut transaction = start_transaction(client)?;
    transaction.execute("INSERT INTO dirs (mtime) VALUES ($1::timestamptz)", &[&mtime])?;
    transaction.commit()?;
    Ok(())
}

pub(crate) fn create_file(client: &mut Client, mtime: DateTime<Utc>, size: u64, executable: bool) -> Result<()> {
    Ok(())
}

pub(crate) fn create_symlink(client: &mut Client, mtime: DateTime<Utc>, target: &str) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::{OnceCell, Lazy};
    use std::process::Command;

    static DATABASE_URI: Lazy<String> = Lazy::new(|| {
        let mut command = Command::new("pg_tmp");
        let stdout = command.output().expect("failed to execute pg_tmp").stdout;
        let database_uri = String::from_utf8(stdout).expect("could not parse pg_tmp output as UTF-8");
        // Add a &user= to fix "no PostgreSQL user name specified in startup packet"
        let user = env_var("USER").unwrap();
        let database_uri = format!("{}&user={}", database_uri, user);
        database_uri
    });

    static DDL_APPLIED: OnceCell<bool> = OnceCell::new();

    fn apply_ddl() {
        let uri = &*DATABASE_URI;
        let mut command = Command::new("psql");
        let psql = command
            .arg("--no-psqlrc")
            .arg("--quiet")
            .arg("-f").arg("schema/schema.sql")
            .arg(uri);
        let code = psql.status().expect("failed to execute psql");
        if !code.success() {
            panic!("psql exited with code {:?}", code.code());
        }
    }

    fn get_client() -> Client {
        let uri = &*DATABASE_URI;
        DDL_APPLIED.get_or_init(|| {
            apply_ddl();
            true
        });
        Client::connect(uri, NoTls).unwrap()
    }

    #[test]
    fn test_cannot_change_dir_immutables() -> Result<()> {
        let mut client = get_client();
        let mut transaction = client.transaction()?;
        transaction.execute("SET search_path TO stash", &[])?;
        transaction.commit()?;
        Ok(())
    }
}
