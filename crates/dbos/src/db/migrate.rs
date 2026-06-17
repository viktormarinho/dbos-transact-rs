//! System-database schema migration runner.
//!
//! Ports the Go reference runner (`system_database.go`) for the **Postgres** path. The reference
//! `.sql` files are embedded verbatim and rendered with the (sanitized) schema name. The runner
//! reproduces the reference's two execution modes:
//!
//! - **Catalog (non-online) migrations** run their SQL and the version bump in a single
//!   transaction — atomic, so a failure never advances the recorded version.
//! - **Online migrations** contain `CREATE/DROP INDEX CONCURRENTLY` and must run *outside* a
//!   transaction (autocommit); the version bump is a separate round-trip, so every online file
//!   guards itself with `IF [NOT] EXISTS` to be safe to re-apply.
//!
//! Before the first pending online migration, invalid indexes left by a crashed `CONCURRENTLY`
//! build are dropped and rebuilt.

use sqlx::PgPool;

use crate::error::Result;

/// The schema version produced by applying all embedded migrations.
pub const SCHEMA_VERSION: i64 = 40;

const MIGRATIONS_TABLE: &str = "dbos_migrations";

struct Migration {
    version: i64,
    sql: String,
    /// Runs `CONCURRENTLY` index DDL; must execute in autocommit (no transaction).
    online: bool,
}

/// Quote an identifier like pgx `Identifier.Sanitize`: wrap in double quotes, doubling any
/// embedded double quote.
fn sanitize_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Render a migration template using Go `fmt.Sprintf` semantics for the subset we use:
/// `%%` → a literal `%` (needed for plpgsql `format('… %%s …')`), and `%s` → the next argument.
fn render(template: &str, mut arg: impl FnMut() -> String) -> String {
    let mut out = String::with_capacity(template.len() + 32);
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '%' {
            match chars.peek() {
                Some('%') => {
                    chars.next();
                    out.push('%');
                }
                Some('s') => {
                    chars.next();
                    out.push_str(&arg());
                }
                _ => out.push('%'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Every `%s` becomes `schema`.
fn render_uniform(template: &str, schema: &str) -> String {
    render(template, || schema.to_string())
}

/// The first `%s` becomes `first`; every subsequent `%s` becomes `rest`. Used for online files
/// (`first = "CONCURRENTLY"`) and migration 10 (`first = raw schema`, `rest = sanitized schema`).
fn render_first_rest(template: &str, first: &str, rest: &str) -> String {
    let mut used = false;
    render(template, || {
        if used {
            rest.to_string()
        } else {
            used = true;
            first.to_string()
        }
    })
}

// Embedded Postgres migration files (cockroach/sqlite variants are intentionally excluded).
const M1_SCHEMA: &str = include_str!("../../migrations/1_initial_dbos_schema.sql");
const M1_LISTEN_NOTIFY: &str = include_str!("../../migrations/1_initial_dbos_schema_listen_notify.sql");
const M10: &str = include_str!("../../migrations/10_add_notifications_pkey.sql");
const M38_BASE: &str = include_str!("../../migrations/38_update_enqueue_workflow.sql");
const M38_SEARCH_PATH: &str = include_str!("../../migrations/38_set_enqueue_workflow_search_path.sql");

/// Versions 2–9, 11–37, 39, 40 (everything except the multi-file/special versions 1, 10, 38).
/// `(version, template, online)`.
const SIMPLE: &[(i64, &str, bool)] = &[
    (2, include_str!("../../migrations/2_add_queue_partition_key.sql"), false),
    (3, include_str!("../../migrations/3_add_workflow_status_index.sql"), false),
    (4, include_str!("../../migrations/4_add_forked_from.sql"), false),
    (5, include_str!("../../migrations/5_add_step_timestamps.sql"), false),
    (6, include_str!("../../migrations/6_add_workflow_events_history.sql"), false),
    (7, include_str!("../../migrations/7_add_owner_xid.sql"), false),
    (8, include_str!("../../migrations/8_add_parent_workflow_id.sql"), false),
    (9, include_str!("../../migrations/9_add_workflow_schedules.sql"), false),
    (11, include_str!("../../migrations/11_add_serialization_columns.sql"), false),
    (12, include_str!("../../migrations/12_add_notifications_consumed.sql"), false),
    (13, include_str!("../../migrations/13_add_application_versions.sql"), false),
    (14, include_str!("../../migrations/14_add_pgsql_client_functions.sql"), false),
    (15, include_str!("../../migrations/15_add_workflow_schedule_columns.sql"), false),
    (16, include_str!("../../migrations/16_add_delay_until.sql"), false),
    (17, include_str!("../../migrations/17_add_workflow_schedule_queue_name.sql"), false),
    (18, include_str!("../../migrations/18_add_was_forked_from.sql"), false),
    (19, include_str!("../../migrations/19_add_operation_outputs_completed_at_index.sql"), false),
    (20, include_str!("../../migrations/20_set_function_search_path.sql"), false),
    (21, include_str!("../../migrations/21_create_queues_table.sql"), false),
    (22, include_str!("../../migrations/22_drop_forked_from_index.sql"), true),
    (23, include_str!("../../migrations/23_create_partial_forked_from_index.sql"), true),
    (24, include_str!("../../migrations/24_drop_parent_workflow_id_index.sql"), true),
    (25, include_str!("../../migrations/25_create_partial_parent_workflow_id_index.sql"), true),
    (26, include_str!("../../migrations/26_drop_executor_id_index.sql"), true),
    (27, include_str!("../../migrations/27_create_partial_dedup_id_index.sql"), true),
    (28, include_str!("../../migrations/28_drop_dedup_id_constraint.sql"), false),
    (29, include_str!("../../migrations/29_create_pending_index.sql"), true),
    (30, include_str!("../../migrations/30_create_failed_index.sql"), true),
    (31, include_str!("../../migrations/31_drop_status_index.sql"), true),
    (32, include_str!("../../migrations/32_create_in_flight_index.sql"), true),
    (33, include_str!("../../migrations/33_add_rate_limited.sql"), false),
    (34, include_str!("../../migrations/34_create_rate_limited_index.sql"), true),
    (35, include_str!("../../migrations/35_drop_queue_status_started_index.sql"), true),
    (36, include_str!("../../migrations/36_add_completed_at.sql"), false),
    (37, include_str!("../../migrations/37_create_started_at_index.sql"), true),
    (39, include_str!("../../migrations/39_create_streams_trigger.sql"), false),
    (40, include_str!("../../migrations/40_add_attributes.sql"), false),
];

/// Build the ordered list of migrations rendered for `schema`.
fn build_migrations(schema: &str) -> Vec<Migration> {
    let s = sanitize_ident(schema);
    let mut migrations: Vec<Migration> = Vec::with_capacity(SIMPLE.len() + 3);

    // v1: initial schema + (Postgres-only) LISTEN/NOTIFY triggers, applied as one unit.
    let v1 = format!("{}\n{}", M1_SCHEMA, M1_LISTEN_NOTIFY);
    migrations.push(Migration {
        version: 1,
        sql: render_uniform(&v1, &s),
        online: false,
    });

    for &(version, template, online) in SIMPLE {
        let sql = if online {
            render_first_rest(template, "CONCURRENTLY", &s)
        } else {
            render_uniform(template, &s)
        };
        migrations.push(Migration { version, sql, online });
    }

    // v10: the catalog comparison uses the RAW schema string; the ALTER uses the sanitized one.
    migrations.push(Migration {
        version: 10,
        sql: render_first_rest(M10, schema, &s),
        online: false,
    });

    // v38: function redefinition + (Postgres-only) search_path hardening, applied as one unit.
    let v38 = format!("{}\n{}", M38_BASE, M38_SEARCH_PATH);
    migrations.push(Migration {
        version: 38,
        sql: render_uniform(&v38, &s),
        online: false,
    });

    migrations.sort_by_key(|m| m.version);
    migrations
}

/// True if `schema` needs migrations (missing schema, missing migrations table, or a version below
/// [`SCHEMA_VERSION`]).
pub async fn should_migrate(pool: &PgPool, schema: &str) -> Result<bool> {
    if !schema_exists(pool, schema).await? {
        return Ok(true);
    }
    if !migrations_table_exists(pool, schema).await? {
        return Ok(true);
    }
    Ok(read_version(pool, schema).await? < SCHEMA_VERSION)
}

/// Apply all pending migrations to bring `schema` to [`SCHEMA_VERSION`]. Idempotent.
pub async fn run_migrations(pool: &PgPool, schema: &str) -> Result<()> {
    let sanitized = sanitize_ident(schema);
    let migrations = build_migrations(schema);

    // Phase A: ensure the schema + migrations table exist and read the current version, in one tx.
    let mut current_version: i64 = {
        let mut tx = pool.begin().await?;
        if !schema_exists(&mut *tx, schema).await? {
            sqlx::raw_sql(&format!("CREATE SCHEMA {sanitized}"))
                .execute(&mut *tx)
                .await?;
        }
        if !migrations_table_exists(&mut *tx, schema).await? {
            sqlx::raw_sql(&format!(
                "CREATE TABLE {sanitized}.{MIGRATIONS_TABLE} (version BIGINT NOT NULL PRIMARY KEY)"
            ))
            .execute(&mut *tx)
            .await?;
        }
        let v = read_version_opt(&mut *tx, schema).await?;
        tx.commit().await?;
        v.unwrap_or(0)
    };

    // Phase B: apply each pending migration.
    let mut invalid_indexes_cleaned = false;
    for migration in &migrations {
        if migration.version <= current_version {
            continue;
        }
        if migration.online {
            if !invalid_indexes_cleaned {
                cleanup_invalid_indexes(pool, schema, &sanitized).await?;
                invalid_indexes_cleaned = true;
            }
            sqlx::raw_sql(&migration.sql).execute(pool).await?;
            write_version(pool, &sanitized, migration.version, current_version).await?;
        } else {
            let mut tx = pool.begin().await?;
            if !migration.sql.trim().is_empty() {
                sqlx::raw_sql(&migration.sql).execute(&mut *tx).await?;
            }
            write_version(&mut *tx, &sanitized, migration.version, current_version).await?;
            tx.commit().await?;
        }
        current_version = migration.version;
    }
    Ok(())
}

async fn schema_exists<'e, E: sqlx::PgExecutor<'e>>(exec: E, schema: &str) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)",
    )
    .bind(schema)
    .fetch_one(exec)
    .await?;
    Ok(exists)
}

async fn migrations_table_exists<'e, E: sqlx::PgExecutor<'e>>(
    exec: E,
    schema: &str,
) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
    )
    .bind(schema)
    .bind(MIGRATIONS_TABLE)
    .fetch_one(exec)
    .await?;
    Ok(exists)
}

async fn read_version_opt<'e, E: sqlx::PgExecutor<'e>>(
    exec: E,
    schema: &str,
) -> Result<Option<i64>> {
    let sanitized = sanitize_ident(schema);
    let v: Option<i64> =
        sqlx::query_scalar(&format!("SELECT version FROM {sanitized}.{MIGRATIONS_TABLE} LIMIT 1"))
            .fetch_optional(exec)
            .await?;
    Ok(v)
}

async fn read_version<'e, E: sqlx::PgExecutor<'e>>(exec: E, schema: &str) -> Result<i64> {
    Ok(read_version_opt(exec, schema).await?.unwrap_or(0))
}

/// Insert the version row the first time (`last_applied == 0`), update it thereafter. Matches the
/// Go reference's single-row tracking table.
async fn write_version<'e, E: sqlx::PgExecutor<'e>>(
    exec: E,
    sanitized_schema: &str,
    version: i64,
    last_applied: i64,
) -> Result<()> {
    let sql = if last_applied == 0 {
        format!("INSERT INTO {sanitized_schema}.{MIGRATIONS_TABLE} (version) VALUES ($1)")
    } else {
        format!("UPDATE {sanitized_schema}.{MIGRATIONS_TABLE} SET version = $1")
    };
    sqlx::query(&sql).bind(version).execute(exec).await?;
    Ok(())
}

/// Drop indexes left `indisvalid = false` by a crashed `CREATE INDEX CONCURRENTLY`, so the
/// following online migration can rebuild them. Runs in autocommit (the drops are `CONCURRENTLY`).
async fn cleanup_invalid_indexes(pool: &PgPool, schema: &str, sanitized_schema: &str) -> Result<()> {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT i.relname \
         FROM pg_index ix \
         JOIN pg_class i ON i.oid = ix.indexrelid \
         JOIN pg_class t ON t.oid = ix.indrelid \
         JOIN pg_namespace n ON n.oid = t.relnamespace \
         WHERE NOT ix.indisvalid AND n.nspname = $1",
    )
    .bind(schema)
    .fetch_all(pool)
    .await?;

    for name in names {
        tracing::warn!("DBOS: dropping invalid index {schema}.{name}");
        let drop = format!(
            "DROP INDEX CONCURRENTLY IF EXISTS {sanitized_schema}.{}",
            sanitize_ident(&name)
        );
        sqlx::raw_sql(&drop).execute(pool).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::testutil::{connect, drop_schema, unique_schema};

    async fn table_exists(pool: &PgPool, schema: &str, table: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)",
        )
        .bind(schema)
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn function_exists(pool: &PgPool, schema: &str, func: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=$1 AND p.proname=$2)",
        )
        .bind(schema)
        .bind(func)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn version_row_count(pool: &PgPool, schema: &str) -> i64 {
        sqlx::query_scalar::<_, i64>(&format!(
            "SELECT count(*) FROM {}.{MIGRATIONS_TABLE}",
            sanitize_ident(schema)
        ))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn set_version(pool: &PgPool, schema: &str, v: i64) {
        sqlx::query(&format!(
            "UPDATE {}.{MIGRATIONS_TABLE} SET version = $1",
            sanitize_ident(schema)
        ))
        .bind(v)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn fresh_migration_builds_full_schema_at_version_40() {
        let pool = connect().await;
        let schema = unique_schema("fresh");
        run_migrations(&pool, &schema).await.unwrap();

        assert_eq!(read_version(&pool, &schema).await.unwrap(), 40);
        assert_eq!(version_row_count(&pool, &schema).await, 1, "exactly one version row");

        // A representative set of tables and pg functions from across the migrations.
        for t in [
            "workflow_status",
            "operation_outputs",
            "notifications",
            "workflow_events",
            "streams",
            "queues",
            "dbos_migrations",
        ] {
            assert!(table_exists(&pool, &schema, t).await, "missing table {t}");
        }
        for f in ["enqueue_workflow", "notifications_function", "streams_function"] {
            assert!(function_exists(&pool, &schema, f).await, "missing function {f}");
        }

        drop_schema(&pool, &schema).await;
    }

    #[tokio::test]
    async fn rerun_is_a_noop() {
        let pool = connect().await;
        let schema = unique_schema("rerun");
        run_migrations(&pool, &schema).await.unwrap();
        // Second run must not error and must not add a version row.
        run_migrations(&pool, &schema).await.unwrap();
        assert_eq!(read_version(&pool, &schema).await.unwrap(), 40);
        assert_eq!(version_row_count(&pool, &schema).await, 1);
        drop_schema(&pool, &schema).await;
    }

    #[tokio::test]
    async fn should_migrate_transitions() {
        let pool = connect().await;
        let schema = unique_schema("should");
        // Missing schema → needs migration.
        assert!(should_migrate(&pool, &schema).await.unwrap());
        run_migrations(&pool, &schema).await.unwrap();
        // Fully migrated → no.
        assert!(!should_migrate(&pool, &schema).await.unwrap());
        // Behind by one → yes.
        set_version(&pool, &schema, SCHEMA_VERSION - 1).await;
        assert!(should_migrate(&pool, &schema).await.unwrap());
        drop_schema(&pool, &schema).await;
    }

    #[tokio::test]
    async fn online_migrations_are_idempotent() {
        let pool = connect().await;
        let schema = unique_schema("online");
        run_migrations(&pool, &schema).await.unwrap();
        // Rewind to just before the first online migration (v22) and re-run; the IF [NOT] EXISTS
        // guards must make every online migration safe to re-apply.
        set_version(&pool, &schema, 21).await;
        run_migrations(&pool, &schema).await.unwrap();
        assert_eq!(read_version(&pool, &schema).await.unwrap(), 40);
        drop_schema(&pool, &schema).await;
    }

    #[tokio::test]
    async fn version_not_bumped_on_failure() {
        let pool = connect().await;
        let schema = unique_schema("failure");
        run_migrations(&pool, &schema).await.unwrap();
        // Rewind to 20; replaying v21 (CREATE TABLE queues) fails because the table already exists.
        set_version(&pool, &schema, 20).await;
        let err = run_migrations(&pool, &schema).await.unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "unexpected error: {err}"
        );
        // The atomic catalog tx rolled back, so the version is still 20.
        assert_eq!(read_version(&pool, &schema).await.unwrap(), 20);

        // Remove the conflict and re-run to completion.
        sqlx::raw_sql(&format!("DROP TABLE {}.queues", sanitize_ident(&schema)))
            .execute(&pool)
            .await
            .unwrap();
        run_migrations(&pool, &schema).await.unwrap();
        assert_eq!(read_version(&pool, &schema).await.unwrap(), 40);
        drop_schema(&pool, &schema).await;
    }

    #[tokio::test]
    async fn resumes_after_invalid_index() {
        let pool = connect().await;
        let schema = unique_schema("invalididx");
        run_migrations(&pool, &schema).await.unwrap();

        // Forge an INVALID index (as a crashed CREATE INDEX CONCURRENTLY would leave behind).
        let idx = format!("{}.idx_workflow_status_in_flight", sanitize_ident(&schema));
        sqlx::raw_sql(&format!(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{idx}'::regclass"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let valid_before: bool = sqlx::query_scalar(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = $1::regclass",
        )
        .bind(&idx)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(!valid_before, "index should be invalid after forging");

        // Rewind to before the migration that builds it (v32) and re-run.
        set_version(&pool, &schema, 31).await;
        run_migrations(&pool, &schema).await.unwrap();

        let valid_after: bool = sqlx::query_scalar(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = $1::regclass",
        )
        .bind(&idx)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(valid_after, "cleanup should have dropped+rebuilt the index");
        assert_eq!(read_version(&pool, &schema).await.unwrap(), 40);
        drop_schema(&pool, &schema).await;
    }

    #[test]
    fn render_handles_escaped_percent_and_placeholders() {
        // plpgsql format string: %%s must survive as a literal %s, real %s gets the schema.
        let out = render_uniform("format('q %%s', x); CREATE TABLE %s.t ()", "\"dbos\"");
        assert_eq!(out, "format('q %s', x); CREATE TABLE \"dbos\".t ()");
    }

    #[test]
    fn build_migrations_is_ordered_1_to_40() {
        let m = build_migrations("dbos");
        assert_eq!(m.len(), 40);
        for (i, mig) in m.iter().enumerate() {
            assert_eq!(mig.version, (i as i64) + 1, "out of order at index {i}");
        }
        assert_eq!(m.last().unwrap().version, SCHEMA_VERSION);
        // Online set per the reference.
        let online: Vec<i64> = m.iter().filter(|x| x.online).map(|x| x.version).collect();
        assert_eq!(online, vec![22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 34, 35, 37]);
    }
}
