pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),
    #[error("Error opening database: {0}")]
    OpenDatabase(rusqlite::Error),
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Internal error: {0}")]
    Panic(#[from] tokio::task::JoinError),
}
