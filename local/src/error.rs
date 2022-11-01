pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),
    #[error("Error opening database: {0}")]
    OpenDatabase(rusqlite::Error),
    #[error("Error opening database: {0}")]
    DeadpoolConfig(#[from] deadpool_sqlite::ConfigError),
    #[error("Error opening database: {0}")]
    PoolBuildError(#[from] deadpool_sqlite::BuildError),
    #[error("Error acquiring database connection: {0}")]
    PoolError(#[from] deadpool_sqlite::PoolError),
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Unexpected value type for {1}: {0}")]
    ColumnType(#[source] rusqlite::Error, &'static str),
    #[error("Unexpected value type for {1}: {0}")]
    FromSql(#[source] rusqlite::types::FromSqlError, &'static str),
    #[error("Internal error: {0}")]
    Panic(#[from] tokio::task::JoinError),
    #[error("Internal error: {0}")]
    DbInteract(#[from] deadpool_sqlite::InteractError),
    #[error("Job not found")]
    NotFound,
    #[error("Error decoding job run info {0}")]
    InvalidJobRunInfo(serde_json::Error),
    #[error("Error processing payload: {0}")]
    PayloadError(serde_json::Error),
    #[error("Timestamp {0} out of range")]
    TimestampOutOfRange(&'static str),
    #[error("Attempted to finish already-finished job")]
    JobAlreadyConsumed,
    #[error("Timed out")]
    Timeout,
    #[error("Job expired")]
    Expired,
    #[error("Job expired while recording success")]
    ExpiredWhileRecordingSuccess,
    #[error("Worker {0} not found")]
    WorkerNotFound(u64),
}
