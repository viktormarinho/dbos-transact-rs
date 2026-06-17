//! The DBOS system database: connection, schema migrations, and (in later milestones) all the
//! workflow/step/queue/notification SQL operations.

pub mod migrate;

pub use migrate::{run_migrations, should_migrate, SCHEMA_VERSION};

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::error::Result;

/// Connect to the system database, returning a pooled connection.
pub async fn connect(database_url: &str, max_connections: u32) -> Result<PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database_url)
        .await?;
    Ok(pool)
}

#[cfg(test)]
pub(crate) mod testutil {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The Postgres URL the integration tests connect to. Defaults to the local Docker container
    /// (`docker run ... -p 5439:5432 postgres:16`); override with `DBOS_SYSTEM_DATABASE_URL`.
    pub fn test_database_url() -> String {
        std::env::var("DBOS_SYSTEM_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:dbos@localhost:5439/dbos".to_string())
    }

    pub async fn connect() -> PgPool {
        PgPoolOptions::new()
            .max_connections(3)
            .connect(&test_database_url())
            .await
            .expect("connect to the test Postgres (is the docker container running?)")
    }

    /// A unique schema name so tests can run in parallel without colliding.
    pub fn unique_schema(prefix: &str) -> String {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        format!("test_{prefix}_{}_{n}", std::process::id())
    }

    pub async fn drop_schema(pool: &PgPool, schema: &str) {
        let _ = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
            .execute(pool)
            .await;
    }
}
