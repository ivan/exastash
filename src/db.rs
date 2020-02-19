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

/// Create an entry for a directory in the database and return its id
pub(crate) fn create_dir(client: &mut Client, mtime: DateTime<Utc>, birth: &Birth) -> Result<i64> {
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
    assert!(id >= 1);
    transaction.commit()?;
    Ok(id)
}

/// Create an entry for a file in the database and return its id
pub(crate) fn create_file(client: &mut Client, mtime: DateTime<Utc>, size: u64, executable: bool, birth: &Birth) -> Result<i64> {
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
    assert!(id >= 1);
    transaction.commit()?;
    Ok(id)
}

/// Create an entry for a symlink in the database and return its id
pub(crate) fn create_symlink(client: &mut Client, mtime: DateTime<Utc>, target: &str, birth: &Birth) -> Result<i64> {
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
    assert!(id >= 1);
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

    // Testing our .sql from Rust, not testing our Rust
    mod schema_internals {
        use super::*;

        /// Can change mtime on a dir
        #[test]
        fn test_can_change_dir_mutables() -> Result<()> {
            let mut client = get_client();
            let id = create_dir(&mut client, Utc::now(), &Birth::here_and_now())?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE dirs SET mtime = now() WHERE id = $1::bigint", &[&id])?;
            transaction.commit()?;
            Ok(())
        }

        // Cannot change id, birth_time, birth_version, or birth_hostname on a dir
        #[test]
        fn test_cannot_change_dir_immutables() -> Result<()> {
            let mut client = get_client();
            let id = create_dir(&mut client, Utc::now(), &Birth::here_and_now())?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE dirs SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_* columns");
            }
            Ok(())
        }

        /// Can change size, mtime, and executable on a file
        #[test]
        fn test_can_change_file_mutables() -> Result<()> {
            let mut client = get_client();
            let size = 0;
            let executable = false;
            let id = create_file(&mut client, Utc::now(), size, executable, &Birth::here_and_now())?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET mtime = now() WHERE id = $1::bigint", &[&id])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET size = 100000 WHERE id = $1::bigint", &[&id])?;
            transaction.commit()?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE files SET executable = true WHERE id = $1::bigint", &[&id])?;
            transaction.commit()?;
            Ok(())
        }

        // Cannot change id, birth_time, birth_version, or birth_hostname on a file
        #[test]
        fn test_cannot_change_file_immutables() -> Result<()> {
            let mut client = get_client();
            let size = 0;
            let executable = false;
            let id = create_file(&mut client, Utc::now(), size, executable, &Birth::here_and_now())?;
            for (column, value) in [("id", "100"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE files SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id or birth_* columns");
            }
            Ok(())
        }

        /// Can change mtime on a symlink
        #[test]
        fn test_can_change_symlink_mutables() -> Result<()> {
            let mut client = get_client();
            let target = "old";
            let id = create_symlink(&mut client, Utc::now(), target, &Birth::here_and_now())?;
            let mut transaction = start_transaction(&mut client)?;
            transaction.execute("UPDATE symlinks SET mtime = now() WHERE id = $1::bigint", &[&id])?;
            transaction.commit()?;
            Ok(())
        }

        // Cannot change id, symlink_target, birth_time, birth_version, or birth_hostname on a symlink
        #[test]
        fn test_cannot_change_symlink_immutables() -> Result<()> {
            let mut client = get_client();
            let target = "old";
            let id = create_symlink(&mut client, Utc::now(), target, &Birth::here_and_now())?;
            for (column, value) in [("id", "100"), ("symlink_target", "'new'"), ("birth_time", "now()"), ("birth_version", "1"), ("birth_hostname", "'dummy'")].iter() {
                let mut transaction = start_transaction(&mut client)?;
                let query = format!("UPDATE symlinks SET {} = {} WHERE id = $1::bigint", column, value);
                let result = transaction.execute(query.as_str(), &[&id]);
                assert_eq!(result.err().expect("expected an error").to_string(), "db error: ERROR: cannot change id, symlink_target, or birth_* columns");
            }
            Ok(())
        }
    }
}
