pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Migration error: {0}")]
    Migration(#[from] rusqlite_migration::Error),
}
