// #![warn(missing_docs)]

//! Outbox pattern implementation for Effectum

#[cfg(feature = "postgres")]
pub mod postgres;

use effectum::{Job, JobUpdate};
#[cfg(feature = "postgres")]
pub use postgres::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "op")]
pub enum QueueOperation {
    Add { job: Job },
    Remove { job_id: Uuid },
    Update { job: JobUpdate },
}

#[derive(sqlx::FromRow)]
pub struct OutboxRow {
    id: i64,
    code_version: Option<String>,
    payload: sqlx::types::Json<QueueOperation>,
    added_at: time::OffsetDateTime,
}
