use std::sync::atomic::AtomicI64;

use rusqlite::{named_params, OptionalExtension};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::shared_state::SharedState;
use crate::{Error, Queue, Result};

pub struct Job {
    pub id: Uuid,
    job_id: i64,
    worker_id: i64,
    heartbeat_increment: i64,
    pub job_type: String,
    pub priority: i64,
    pub payload: Vec<u8>,
    pub expires: AtomicI64,

    queue: SharedState,
}

impl Job {
    /// Checkpoint the task, replacing the payload with the passed in value.
    pub async fn checkpoint_blob(&mut self, new_payload: Vec<u8>) -> Result<OffsetDateTime> {
        // This counts as a heartbeat, so update the expiration.
        // Update the checkpoint_payload.
        let job_id = self.job_id;
        let worker_id = self.worker_id;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let new_expire_time = now + self.heartbeat_increment;

        let actual_new_expire_time = self
            .queue
            .write_db(move |db| {
                let mut stmt = db.prepare_cached(
                    r##"UPDATE running_jobs
                SET checkpoint_payload=$payload, 
                    last_heartbeat=$now,
                    expires_at = MAX(expires_at, $new_expire_time)
                WHERE job_id=$job_id AND worker_id=$worker_id
                RETURNING expires_at"##,
                )?;

                let actual_new_expire_time: Option<i64> = stmt
                    .query_row(
                        named_params! {
                            "$payload": new_payload,
                            "$new_expire_time": new_expire_time,
                            "$now": now,
                            "$job_id": job_id,
                            "$worker_id": worker_id,
                        },
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?;

                Ok(actual_new_expire_time)
            })
            .await?;

        actual_new_expire_time.ok_or(Error::Expired).and_then(|t| {
            OffsetDateTime::from_unix_timestamp(t)
                .map_err(|_| Error::TimestampOutOfRange("new expiration time"))
        })
    }

    /// Checkpoint the task, replacing the payload with the passed in value.
    pub async fn checkpoint_json<T: Serialize>(
        &mut self,
        new_payload: &T,
    ) -> Result<OffsetDateTime> {
        let blob = serde_json::to_vec(new_payload).map_err(Error::PayloadError)?;
        self.checkpoint_blob(blob).await
    }

    /// Tell the queue that the task is still running.
    pub async fn heartbeat(&mut self) -> Result<OffsetDateTime> {
        let job_id = self.job_id;
        let worker_id = self.worker_id;
        let now = OffsetDateTime::now_utc().unix_timestamp();
        let new_expire_time = now + self.heartbeat_increment;
        let actual_new_expire_time = self
            .queue
            .write_db(move |db| {
                let mut stmt = db.prepare_cached(
                    r##"UPDATE running_jobs
                SET last_heartbeat=$now,
                    expires_at = MAX(expires_at, $new_expire_time)
                WHERE job_id=$job_id AND worker_id=$worker_id
                RETURNING expires_at"##,
                )?;

                let actual_new_expire_time: Option<i64> = stmt
                    .query_row(
                        named_params! {
                            "$new_expire_time": new_expire_time,
                            "$now": now,
                            "$job_id": job_id,
                            "$worker_id": worker_id,
                        },
                        |row| row.get::<_, i64>(0),
                    )
                    .optional()?;

                Ok(actual_new_expire_time)
            })
            .await?;

        actual_new_expire_time.ok_or(Error::Expired).and_then(|t| {
            OffsetDateTime::from_unix_timestamp(t)
                .map_err(|_| Error::TimestampOutOfRange("new expiration time"))
        })
    }

    /// Return if the task is past the expiration time or not.
    pub async fn expired(&self) -> bool {
        self.expires.load(std::sync::atomic::Ordering::Relaxed)
            <= OffsetDateTime::now_utc().unix_timestamp()
    }

    pub fn json<'a, T: Deserialize<'a>>(&'a self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(self.payload.as_slice())
    }
}
