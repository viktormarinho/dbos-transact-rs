//! Configuration, dialect detection, and credential masking.
//!
//! Ports the Go reference `Config` / `processConfig` / `detectDialect` (`dbos.go`, `dialect.go`)
//! including its exact default values, environment-variable precedence, validation error strings,
//! and password-masking behavior.

use std::time::Duration;

use crate::error::{DbosError, Result};

pub const DEFAULT_SYSTEM_DB_SCHEMA: &str = "dbos";
pub const DEFAULT_ADMIN_SERVER_PORT: u16 = 3001;
pub const DEFAULT_EXECUTOR_ID: &str = "local";
pub const DEFAULT_SCHEDULER_POLLING_INTERVAL: Duration = Duration::from_secs(30);
/// Sentinel application version used when patching is enabled but no version is given.
pub const PATCHING_ENABLED_VERSION: &str = "PATCHING_ENABLED";

// Environment variables (note: most use a double underscore; `DBOS_DOMAIN` uses a single one).
const ENV_APP_VERSION: &str = "DBOS__APPVERSION";
const ENV_VMID: &str = "DBOS__VMID";
const ENV_APP_ID: &str = "DBOS__APPID";

/// The SQL dialect of the system database.
///
/// Cockroach is **not** detected from the URL scheme (it shares `postgres`/`postgresql` with
/// Postgres) — it is identified at runtime by querying the server. v1 targets Postgres only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Cockroach,
    Sqlite,
}

impl Dialect {
    pub fn as_str(self) -> &'static str {
        match self {
            Dialect::Postgres => "postgres",
            Dialect::Cockroach => "cockroach",
            Dialect::Sqlite => "sqlite",
        }
    }
}

/// libpq key=value DSN prefixes. If a connection string (lowercased+trimmed) starts with one of
/// these, it is a Postgres key=value DSN and we skip URL parsing. Matches Go `looksLikePostgresKVDSN`.
const PG_KV_PREFIXES: &[&str] = &[
    "user=",
    "host=",
    "hostaddr=",
    "port=",
    "dbname=",
    "database=",
    "password=",
    "sslmode=",
    "application_name=",
    "options=",
];

/// Detect the dialect from a database URL. Returns the raw error message on failure (the caller
/// wraps it into an [`DbosError::initialization`]). Mirrors Go `detectDialect`.
pub fn detect_dialect(raw_url: &str) -> std::result::Result<Dialect, String> {
    if raw_url.is_empty() {
        return Err("database URL is empty".to_string());
    }
    let lower_trim = raw_url.trim().to_lowercase();
    if PG_KV_PREFIXES.iter().any(|p| lower_trim.starts_with(p)) {
        return Ok(Dialect::Postgres);
    }
    match extract_scheme(raw_url).as_deref() {
        Some("sqlite") | Some("sqlite3") => Ok(Dialect::Sqlite),
        Some("postgres") | Some("postgresql") => Ok(Dialect::Postgres),
        Some(other) => Err(format!(
            "unsupported database scheme \"{other}\" (want sqlite: or postgres:)"
        )),
        None => Err(format!("database URL has no scheme: \"{raw_url}\"")),
    }
}

/// Extract a URL scheme the way Go's `net/url` does: the substring before the first `:` if it is a
/// valid scheme (letter followed by `[A-Za-z0-9+.-]*`), lowercased. Otherwise `None` (no scheme).
fn extract_scheme(s: &str) -> Option<String> {
    let idx = s.find(':')?;
    let scheme = &s[..idx];
    let mut chars = scheme.chars();
    let first = chars.next()?;
    if !first.is_ascii_alphabetic() {
        return None;
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '-' || c == '.') {
        return None;
    }
    Some(scheme.to_ascii_lowercase())
}

/// Mask the password in a connection string for safe logging. Handles both URL form
/// (`postgres://user:secret@host/db` → `postgres://user:***@host/db`) and libpq key=value form
/// (`password=secret` → `password=***`, case-insensitive, tolerant of spaces around `=`).
pub fn mask_password(conn: &str) -> String {
    use regex::Regex;
    use std::sync::OnceLock;

    static URL_RE: OnceLock<Regex> = OnceLock::new();
    static KV_RE: OnceLock<Regex> = OnceLock::new();

    if conn.contains("://") {
        let re = URL_RE.get_or_init(|| Regex::new(r"(?i)(://[^/?#@:]+:)[^/?#@]*@").unwrap());
        re.replace_all(conn, "${1}***@").into_owned()
    } else {
        let re = KV_RE.get_or_init(|| Regex::new(r"(?i)password\s*=\s*\S+").unwrap());
        re.replace_all(conn, "password=***").into_owned()
    }
}

/// Compute the application version as the lowercase hex SHA-256 of the running executable.
/// On any error, logs and returns an empty string (matches Go `getBinaryHash`).
pub fn compute_application_version() -> String {
    match try_binary_hash() {
        Ok(h) => h,
        Err(e) => {
            tracing::warn!("DBOS: Failed to compute binary hash: {e}");
            String::new()
        }
    }
}

fn try_binary_hash() -> std::io::Result<String> {
    use sha2::{Digest, Sha256};
    let exe = std::fs::canonicalize(std::env::current_exe()?)?;
    let meta = std::fs::metadata(&exe)?;
    if !meta.is_file() {
        return Err(std::io::Error::other("executable is not a regular file"));
    }
    let mut file = std::fs::File::open(&exe)?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
}

/// A source of environment variables, injectable so config processing is testable without touching
/// the real process environment.
pub trait EnvSource {
    fn get(&self, key: &str) -> Option<String>;
}

/// Reads from the real process environment. Empty values are treated as unset (matches Go).
pub struct SystemEnv;

impl EnvSource for SystemEnv {
    fn get(&self, key: &str) -> Option<String> {
        std::env::var(key).ok().filter(|v| !v.is_empty())
    }
}

/// In-memory environment for tests.
#[derive(Default, Clone)]
pub struct MapEnv(pub std::collections::HashMap<String, String>);

impl MapEnv {
    pub fn from<const N: usize>(pairs: [(&str, &str); N]) -> Self {
        MapEnv(
            pairs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }
}

impl EnvSource for MapEnv {
    fn get(&self, key: &str) -> Option<String> {
        self.0.get(key).filter(|v| !v.is_empty()).cloned()
    }
}

/// User-supplied configuration. Optional fields fall back to defaults during processing.
#[derive(Debug, Default, Clone)]
pub struct Config {
    /// Application name (required).
    pub app_name: String,
    /// System-database connection string. Exactly one of `database_url` / `system_db_pool` must be
    /// provided.
    pub database_url: Option<String>,
    /// A pre-built Postgres pool, used instead of `database_url` when set.
    pub system_db_pool: Option<sqlx::PgPool>,
    /// System-database schema name (default `"dbos"`).
    pub database_schema: Option<String>,
    /// Enable the admin HTTP server (default `false`).
    pub admin_server: bool,
    /// Admin HTTP server port (default `3001`).
    pub admin_server_port: Option<u16>,
    /// Application version (overridden by `DBOS__APPVERSION`; defaults to the binary hash).
    pub application_version: Option<String>,
    /// Executor ID (overridden by `DBOS__VMID`; defaults to `"local"`).
    pub executor_id: Option<String>,
    /// Enable the `Patch`/`DeprecatePatch` system.
    pub enable_patching: bool,
    /// How often dynamic schedules reconcile with the DB (default 30s).
    pub scheduler_polling_interval: Option<Duration>,
    /// DBOS conductor service URL (`wss://...`). Defaults from `DBOS_DOMAIN` when an API key is set.
    pub conductor_url: Option<String>,
    /// DBOS conductor API key. When set (or via the `DBOS__CLOUD` env trio), the conductor client
    /// connects on launch.
    pub conductor_api_key: Option<String>,
    /// Arbitrary JSON metadata reported to the conductor in `executor_info`.
    pub conductor_executor_metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

impl Config {
    /// Convenience constructor for the common case (app name + database URL).
    pub fn new(app_name: impl Into<String>, database_url: impl Into<String>) -> Self {
        Config {
            app_name: app_name.into(),
            database_url: Some(database_url.into()),
            ..Default::default()
        }
    }
}

/// Configuration after defaults, environment overrides, and validation have been applied.
#[derive(Debug, Clone)]
pub struct ProcessedConfig {
    pub app_name: String,
    pub database_url: Option<String>,
    pub system_db_pool: Option<sqlx::PgPool>,
    pub database_schema: String,
    pub admin_server: bool,
    pub admin_server_port: u16,
    pub application_version: String,
    pub executor_id: String,
    pub application_id: String,
    pub enable_patching: bool,
    pub scheduler_polling_interval: Duration,
    pub dialect: Dialect,
    pub conductor_url: Option<String>,
    pub conductor_api_key: Option<String>,
    pub conductor_executor_metadata: Option<serde_json::Map<String, serde_json::Value>>,
}

/// Validate and resolve a [`Config`] using the real process environment.
pub fn process_config(cfg: Config) -> Result<ProcessedConfig> {
    process_config_with_env(cfg, &SystemEnv)
}

/// Validate and resolve a [`Config`] against an injectable environment. The validation order and
/// error strings match the Go reference exactly.
pub fn process_config_with_env(cfg: Config, env: &dyn EnvSource) -> Result<ProcessedConfig> {
    // 1) A database source must be provided (checked before app_name, matching Go).
    let has_url = cfg
        .database_url
        .as_deref()
        .map(|u| !u.is_empty())
        .unwrap_or(false);
    if !has_url && cfg.system_db_pool.is_none() {
        return Err(DbosError::initialization(
            "one of databaseURL, systemDBPool, or sqliteSystemDB must be provided",
        ));
    }
    // 2) App name is required.
    if cfg.app_name.is_empty() {
        return Err(DbosError::initialization(
            "missing required config field: appName",
        ));
    }
    // 3) Determine dialect (a custom pool is assumed Postgres).
    let dialect = if cfg.system_db_pool.is_some() {
        Dialect::Postgres
    } else {
        let url = cfg.database_url.as_deref().unwrap_or("");
        detect_dialect(url).map_err(DbosError::initialization)?
    };

    // application_version precedence: env > explicit (or patching sentinel) > computed hash.
    let mut app_version = cfg.application_version.filter(|v| !v.is_empty());
    if app_version.is_none() && cfg.enable_patching {
        app_version = Some(PATCHING_ENABLED_VERSION.to_string());
    }
    if let Some(v) = env.get(ENV_APP_VERSION) {
        app_version = Some(v);
    }
    let application_version = match app_version.filter(|v| !v.is_empty()) {
        Some(v) => v,
        None => compute_application_version(),
    };

    // executor_id precedence: env > explicit > "local".
    let mut executor = cfg.executor_id.filter(|v| !v.is_empty());
    if let Some(v) = env.get(ENV_VMID) {
        executor = Some(v);
    }
    let executor_id = executor
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_EXECUTOR_ID.to_string());

    let application_id = env.get(ENV_APP_ID).unwrap_or_default();

    Ok(ProcessedConfig {
        app_name: cfg.app_name,
        database_url: cfg.database_url,
        system_db_pool: cfg.system_db_pool,
        database_schema: cfg
            .database_schema
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_SYSTEM_DB_SCHEMA.to_string()),
        admin_server: cfg.admin_server,
        admin_server_port: cfg
            .admin_server_port
            .filter(|p| *p != 0)
            .unwrap_or(DEFAULT_ADMIN_SERVER_PORT),
        application_version,
        executor_id,
        application_id,
        enable_patching: cfg.enable_patching,
        scheduler_polling_interval: cfg
            .scheduler_polling_interval
            .filter(|d| !d.is_zero())
            .unwrap_or(DEFAULT_SCHEDULER_POLLING_INTERVAL),
        dialect,
        conductor_url: cfg.conductor_url,
        conductor_api_key: cfg.conductor_api_key,
        conductor_executor_metadata: cfg.conductor_executor_metadata,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_dialect_table() {
        // sqlite forms
        for url in [
            "sqlite:/tmp/x.db",
            "sqlite:///tmp/x.db",
            "sqlite::memory:",
            "sqlite3:relative.db",
            "SQLITE:/tmp/x.db",
            "sqlite:file:/abs/x.db?_pragma=foreign_keys(1)",
        ] {
            assert_eq!(detect_dialect(url), Ok(Dialect::Sqlite), "{url}");
        }
        // postgres forms (URL + KV DSN)
        for url in [
            "postgres://u:p@h:5432/d",
            "postgresql://u:p@h/d",
            "postgres://[::1]:5432/dbos",
            "postgres://localhost",
            "postgresql://user=foo:5432/db", // scheme wins over KV heuristic
            "postgres://",
            "host=localhost user='postgres' application_name='dbos worker'",
            "host=localhost port=5432 dbname=dbos",
            "user='User Name-123@acme.com#$%&!' password='a!b@c' database=dbos host=localhost",
        ] {
            assert_eq!(detect_dialect(url), Ok(Dialect::Postgres), "{url}");
        }
        // errors (substring match)
        assert!(detect_dialect("").unwrap_err().contains("empty"));
        assert!(detect_dialect("file:/abs/x.db")
            .unwrap_err()
            .contains("unsupported database scheme"));
        assert!(detect_dialect("postgress://typo")
            .unwrap_err()
            .contains("unsupported database scheme"));
        assert!(detect_dialect("mysql://h/d")
            .unwrap_err()
            .contains("unsupported database scheme"));
        assert!(detect_dialect("justastring")
            .unwrap_err()
            .contains("no scheme"));
        assert!(detect_dialect("foo=bar host=localhost")
            .unwrap_err()
            .contains("no scheme"));
    }

    #[test]
    fn mask_password_url_and_kv() {
        assert_eq!(
            mask_password("postgres://user:secret@host/db?x=1"),
            "postgres://user:***@host/db?x=1"
        );
        // No password in URL → unchanged, no leak.
        let no_pw = mask_password("postgres://user@host/db");
        assert_eq!(no_pw, "postgres://user@host/db");
        assert!(!no_pw.contains("***"));

        let pw = "TEST_PASSWORD_UNIQUE_12345!@#$%";
        for form in [
            format!("user=u password={pw} database=d host=h"),
            format!("user=u password ={pw} database=d host=h"),
            format!("user=u password= {pw} database=d host=h"),
            format!("user=u password = {pw} database=d host=h"),
            format!("user=u PASSWORD={pw} database=d host=h"),
            format!("user=u Password={pw} database=d host=h"),
        ] {
            let masked = mask_password(&form);
            assert!(masked.contains("***"), "{form}");
            assert!(!masked.contains(pw), "{form}");
        }
    }

    fn base_cfg() -> Config {
        Config::new("myapp", "postgres://u:p@localhost:5432/dbos")
    }

    #[test]
    fn env_overrides_config_values() {
        let mut cfg = base_cfg();
        cfg.application_version = Some("config-v1.2.3".to_string());
        cfg.executor_id = Some("config-executor-123".to_string());
        let env = MapEnv::from([
            ("DBOS__APPVERSION", "env-v2.0.0"),
            ("DBOS__VMID", "env-executor-456"),
            ("DBOS__APPID", "test-app-id"),
        ]);
        let pc = process_config_with_env(cfg, &env).unwrap();
        assert_eq!(pc.application_version, "env-v2.0.0");
        assert_eq!(pc.executor_id, "env-executor-456");
        assert_eq!(pc.application_id, "test-app-id");
    }

    #[test]
    fn uses_config_values_when_env_empty() {
        let mut cfg = base_cfg();
        cfg.application_version = Some("config-v1.2.3".to_string());
        cfg.executor_id = Some("config-executor-123".to_string());
        let pc = process_config_with_env(cfg, &MapEnv::default()).unwrap();
        assert_eq!(pc.application_version, "config-v1.2.3");
        assert_eq!(pc.executor_id, "config-executor-123");
        assert_eq!(pc.application_id, "");
    }

    #[test]
    fn uses_defaults_when_empty() {
        let pc = process_config_with_env(base_cfg(), &MapEnv::default()).unwrap();
        assert_eq!(pc.executor_id, "local");
        assert_eq!(pc.database_schema, "dbos");
        assert_eq!(pc.admin_server_port, 3001);
        assert_eq!(pc.scheduler_polling_interval, Duration::from_secs(30));
        // Application version defaults to the (non-empty) hash of the test binary.
        assert!(!pc.application_version.is_empty());
        assert_eq!(pc.dialect, Dialect::Postgres);
    }

    #[test]
    fn patching_sentinel_version() {
        let mut cfg = base_cfg();
        cfg.enable_patching = true;
        let pc = process_config_with_env(cfg, &MapEnv::default()).unwrap();
        assert_eq!(pc.application_version, "PATCHING_ENABLED");
    }

    #[test]
    fn missing_db_source_errors_first() {
        // Only app_name set: DB-source check fires before app_name check.
        let cfg = Config {
            app_name: "myapp".to_string(),
            ..Default::default()
        };
        let err = process_config_with_env(cfg, &MapEnv::default()).unwrap_err();
        assert_eq!(err.code(), 2);
        assert!(err
            .to_string()
            .contains("one of databaseURL, systemDBPool, or sqliteSystemDB must be provided"));
    }

    #[test]
    fn missing_app_name_errors() {
        let cfg = Config {
            database_url: Some("postgres://u:p@h/d".to_string()),
            ..Default::default()
        };
        let err = process_config_with_env(cfg, &MapEnv::default()).unwrap_err();
        assert_eq!(err.code(), 2);
        assert!(err
            .to_string()
            .contains("missing required config field: appName"));
    }
}
