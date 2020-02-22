mod inode;
mod dirent;
mod storage;

use anyhow::Result;
use postgres::{Client, Transaction, NoTls, IsolationLevel};
use crate::util::env_var;

fn postgres_client_production() -> Result<Client> {
    let database_uri = env_var("EXASTASH_POSTGRESQL_URI")?;
    Ok(Client::connect(&database_uri, NoTls)?)
}

/// Returns a transaction with isolation level serializable and
/// search_path set to stash.
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

    pub(crate) fn get_client() -> Client {
        let uri = &*DATABASE_URI;
        DDL_APPLIED.get_or_init(|| {
            apply_ddl(uri, "schema/schema.sql");
            true
        });
        Client::connect(uri, NoTls).unwrap()
    }

    #[test]
    fn test_start_transaction() -> Result<()> {
        let mut client = get_client();
        let _ = start_transaction(&mut client)?;
        Ok(())
    }
}
