use std::env;
use std::convert::TryFrom;
use anyhow::{anyhow, Context, Result};
use postgres::{Client, Transaction, NoTls, IsolationLevel};
use chrono::{DateTime, Utc};
use crate::EXASTASH_VERSION;

fn get_hostname() -> String {
    let os_string = gethostname::gethostname();
    let hostname = os_string.to_str().expect("hostname on this machine was not valid UTF-8");
    hostname.to_owned()
}

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

impl Birth {
    pub(crate) fn here_and_now() -> Birth {
        Birth { time: Utc::now(), version: EXASTASH_VERSION, hostname: get_hostname() }
    }
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

pub(crate) fn create_dir(client: &mut Client, mtime: DateTime<Utc>, birth: &Birth) -> Result<u64> {
    let mut transaction = start_transaction(client)?;
    let rows = transaction.query(
        "INSERT INTO dirs (mtime, birth_time, birth_version, birth_hostname)
         VALUES ($1::timestamptz, $2::timestamptz, $3::smallint, $4::text)
         RETURNING id", &[
            &mtime,
            &birth.time,
            &i16::try_from(birth.version).unwrap(),
            &birth.hostname
        ]
    )?;
    let id: i64 = rows[0].get(0);
    let id = u64::try_from(id).with_context(|| anyhow!("id {} out of expected u64 range", id))?;
    transaction.commit()?;
    Ok(id)
}

pub(crate) fn create_file(client: &mut Client, mtime: DateTime<Utc>, size: u64, executable: bool, birth: &Birth) -> Result<u64> {
    let mut transaction = start_transaction(client)?;
    let rows = transaction.query(
        "INSERT INTO files (mtime, size, executable, birth_time, birth_version, birth_hostname)
         VALUES ($1::timestamptz, $2::bigint, $3::boolean, $4::timestamptz, $5::smallint, $6::text)
         RETURNING id", &[
            &mtime,
            &i64::try_from(size).with_context(|| anyhow!("size {} out of expected i64 range", size))?,
            &executable,
            &birth.time,
            &i16::try_from(birth.version).unwrap(),
            &birth.hostname,
        ]
    )?;
    let id: i64 = rows[0].get(0);
    let id = u64::try_from(id).with_context(|| anyhow!("id {} out of expected u64 range", id))?;
    transaction.commit()?;
    Ok(id)
}

pub(crate) fn create_symlink(client: &mut Client, mtime: DateTime<Utc>, target: &str, birth: &Birth) -> Result<u64> {
    let mut transaction = start_transaction(client)?;
    let rows = transaction.query(
        "INSERT INTO symlinks (mtime, symlink_target, birth_time, birth_version, birth_hostname)
         VALUES ($1::timestamptz, $2::text, $3::timestamptz, $4::smallint, $5::text)
         RETURNING id", &[
            &mtime,
            &target,
            &birth.time,
            &i16::try_from(birth.version).unwrap(),
            &birth.hostname
        ]
    )?;
    let id: i64 = rows[0].get(0);
    let id = u64::try_from(id).with_context(|| anyhow!("id {} out of expected u64 range", id))?;
    transaction.commit()?;
    Ok(id)
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
        let id = create_dir(&mut client, Utc::now(), &Birth::here_and_now())?;
        dbg!(id);
        Ok(())
    }
}
