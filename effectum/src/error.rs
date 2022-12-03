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
    OpenDatabase(eyre::Report),
    /// Failed to acquire a database connection for reading.
    #[error("Error acquiring database connection: {0}")]
    PoolError(#[from] deadpool_sqlite::PoolError),
    /// Encountered an error communicating with the database.
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    /// The database contained invalid data.
    #[error("Unexpected value type for {1}: {0}")]
    ColumnType(#[source] rusqlite::Error, &'static str),
    /// An internal task panicked.
    #[error("Internal error: {0}")]
    Panic(#[from] tokio::task::JoinError),
    /// An internal error occurred while reading the database.
    #[error("Internal error: {0}")]
    DbInteract(String),
    /// When requesting a job status, the job ID was not found.
    #[error("Job not found")]
    NotFound,
    /// A job had an unknown state value
    #[error("Invalid job state {0}")]
    InvalidJobState(String),
    /// Failed to serialize or deserialize information when recording information about a job run.
    #[error("Error decoding job run info {0}")]
    InvalidJobRunInfo(serde_json::Error),
    /// Failed to serialize or deserialize a job payload
    #[error("Error processing payload: {0}")]
    PayloadError(serde_json::Error),
    /// Invalid value for a job timestamp
    #[error("Timestamp {0} out of range")]
    TimestampOutOfRange(&'static str),
    /// A worker attempted to finish a job more than once.
    #[error("Attempted to finish already-finished job")]
    JobAlreadyConsumed,
    /// The operation timed out. This is mostly used when the queue fails to shut down in a timely
    /// fashion.
    #[error("Timed out")]
    Timeout,
    /// The current job has expired.
    #[error("Job expired")]
    Expired,
    /// An unregistered worker tried to communicate with the queue.
    #[error("Worker {0} not found")]
    WorkerNotFound(u64),
    /// Indicates that the queue has closed, and so the attempted operation could not be completed.
    #[error("Queue closed unexpectedly")]
    QueueClosed,
}

impl From<InteractError> for Error {
    fn from(e: InteractError) -> Self {
        Error::DbInteract(e.to_string())
    }
}

impl Error {
    pub(crate) fn open_database(err: impl Into<eyre::Report>) -> Self {
        Error::OpenDatabase(err.into())
    }
}
