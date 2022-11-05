use deadpool_sqlite::InteractError;

/// A [std::result::Result] whose error type defaults to [Error].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors that can be returned from the queue.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// An error occurred while updating the database to a new schema version.
    #[error("Migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),
    /// An error occurred while opening the database.
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
    DbInteract(String),
    /// When requesting a job status, the job ID was not found.
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
    /// The operation timed out. This is mostly used when the queue fails to shut down in a timely
    /// fashion.
    #[error("Timed out")]
    Timeout,
    /// The current job has expired.
    #[error("Job expired")]
    Expired,
    #[error("Worker {0} not found")]
    WorkerNotFound(u64),
    #[error("Queue closed unexpectedly")]
    QueueClosed,
}

impl From<InteractError> for Error {
    fn from(e: InteractError) -> Self {
        Error::DbInteract(e.to_string())
    }
}
