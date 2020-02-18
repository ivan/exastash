use std::env;
use anyhow::{anyhow, bail, Context, Result};
use postgres::{Client, Transaction, NoTls};

fn env_var(var: &str) -> Result<String> {
    env::var(var).with_context(|| anyhow!("Could not get variable {:?} from environment", var))
}

fn postgres_client_production() -> Result<Client> {
    let database_url = env_var("EXASTASH_POSTGRESQL_URL")?;
    Ok(Client::connect(&database_url, NoTls)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use std::process::Command;

    static DATABASE_URL: Lazy<String> = Lazy::new(|| {
        let mut pg_tmp = Command::new("pg_tmp");
        let stdout = pg_tmp.output().expect("failed to execute pg_tmp").stdout;
        let database_url = String::from_utf8(stdout).expect("could not parse pg_tmp output as UTF-8");
        // Add a &user= to fix: "no PostgreSQL user name specified in startup packet"
        let user = env_var("USER").unwrap();
        let database_url = format!("{}&user={}", database_url, user);
        dbg!(&database_url);
        database_url
    });

    fn get_client() -> Client {
        Client::connect(&*DATABASE_URL, NoTls).unwrap()
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
