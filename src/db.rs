mod inode;

use std::process::Command;
use anyhow::Result;
use postgres::{Client, Transaction, NoTls, IsolationLevel};
use crate::util::env_var;

fn postgres_client_production() -> Result<Client> {
    let database_uri = env_var("EXASTASH_POSTGRESQL_URI")?;
    Ok(Client::connect(&database_uri, NoTls)?)
}

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
