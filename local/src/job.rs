use std::sync::atomic::AtomicI64;

use rusqlite::{named_params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::job_status::RunInfo;
use crate::shared_state::SharedState;
use crate::{Error, Result};

pub struct Job {
    pub id: Uuid,
    job_id: i64,
    worker_id: i64,
    heartbeat_increment: i64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
    pub job_type: String,
    pub priority: i64,
    pub payload: Vec<u8>,
    pub expires: AtomicI64,

    pub start_time: OffsetDateTime,
    backoff_multiplier: f32,
    backoff_randomization: f32,
    backoff_initial_interval: i32,
    current_retry: i32,

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
                    r##"UPDATE running
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

        let new_time = actual_new_expire_time.ok_or(Error::Expired).and_then(|t| {
            OffsetDateTime::from_unix_timestamp(t)
                .map_err(|_| Error::TimestampOutOfRange("new expiration time"))
        })?;

        self.update_expiration(new_time);

        Ok(new_time)
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
                    r##"UPDATE running
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

        let new_time = actual_new_expire_time.ok_or(Error::Expired).and_then(|t| {
            OffsetDateTime::from_unix_timestamp(t)
                .map_err(|_| Error::TimestampOutOfRange("new expiration time"))
        })?;

        self.update_expiration(new_time);

        Ok(new_time)
    }

    fn update_expiration(&mut self, new_expiration: OffsetDateTime) {
        self.expires.store(
            new_expiration.unix_timestamp(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn is_done(&self) -> bool {
        self.done.is_none()
    }

    /// Return if the task is past the expiration time or not.
    pub fn is_expired(&self) -> bool {
        self.expires.load(std::sync::atomic::Ordering::Relaxed)
            <= OffsetDateTime::now_utc().unix_timestamp()
    }

    pub fn json_payload<'a, T: Deserialize<'a>>(&'a self) -> Result<T, serde_json::Error> {
        serde_json::from_slice(self.payload.as_slice())
    }

    pub async fn complete<T: Serialize + Send>(&mut self, info: T) -> Result<(), Error> {
        let chan = self
            .done
            .take()
            .expect("Called complete after job finished");

        let info = RunInfo {
            success: true,
            start: self.start_time,
            end: OffsetDateTime::now_utc(),
            info,
        };

        let this_run_info = serde_json::to_string(&info).map_err(Error::InvalidJobRunInfo)?;

        let job_id = self.job_id;
        let worker_id = self.worker_id;
        self.queue
            .write_db(move |db| {
                let tx = db.transaction()?;
                {
                    // Move job from active_jobs to done_jobs, and add the run info
                    let mut stmt = tx.prepare_cached(r##"INSERT INTO done_jobs
                      (job_id, external_id, job_type, priority, status, done_time, from_recurring_job, orig_run_at_time, payload,
                       max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval, added_at, default_timeout,
                       heartbeat_increment, run_info)
                       SELECT job_id, external_id, job_type, priority, status, done_time, from_recurring_job, orig_run_at_time, payload,
                              max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval, added_at, default_timeout,
                              heartbeat_increment,
                              json_array_append(run_info, $this_run_info) AS run_info
                        FROM running
                        JOIN active_jobs USING(job_id)
                        WHERE job_id=$job_id AND worker_id=$worker_id"##)?;

                    let altered = stmt.execute(named_params! {
                            "$job_id": job_id,
                            "$worker_id": worker_id,
                            "$this_run_info": this_run_info
                        })?;

                    if altered == 0 {
                        // The job expired before we could record the success. Return a special error
                        // for this so that we can log it. There isn't much to do about this though
                        // since the job may already be running again. Generally, though, this
                        // shouldn't happen if the heartbeat mechanism is used.
                        return Err(Error::ExpiredWhileRecordingSuccess);
                    }

                    // Clean up the old entries.
                    let mut stmt = tx.prepare_cached(r##"DELETE FROM active_jobs WHERE job_id=$1"##)?;
                    stmt.execute(&[&job_id])?;

                    let mut stmt = tx.prepare_cached(r##"DELETE FROM running WHERE job_id=$1 AND worker_id=$2"##)?;
                    stmt.execute(&[&job_id, &worker_id])?;
                }

                tx.commit()?;
                Ok(())
            })
            .await?;

        Ok(())
    }

    /// Mark the job as failed.
    pub async fn fail<T: Serialize + Send>(&mut self, info: T) -> Result<(), Error> {
        let chan = self.done.take().expect("Called fail after job finished");
        // Remove task from running jobs, update job info, calculate new retry time, and stick the
        // job back into pending.
        // If there is a checkpointed payload, use that. Otherwise use the original payload from the
        // job.

        // Calculate the next run time, given the backoff.
        let now = OffsetDateTime::now_utc();
        let next_try_count = self.current_retry + 1;
        let run_delta = (self.backoff_initial_interval as f32)
            * (self.backoff_multiplier).powi(next_try_count)
            * (1.0 + rand::random::<f32>() * self.backoff_randomization);
        let next_run_time = now.unix_timestamp() + (run_delta as i64);
        let job_id = self.job_id;
        let worker_id = self.worker_id;

        let info = RunInfo {
            success: false,
            start: self.start_time,
            end: now,
            info,
        };

        let this_run_info = serde_json::to_string(&info).map_err(Error::InvalidJobRunInfo)?;

        self.queue.write_db(move |db| {
            let tx = db.transaction()?;

            {
                let mut stmt = tx.prepare_cached(r##"DELETE FROM running
                    WHERE job_id=$1 AND worker_id=$2
                    RETURNING priority, checkpointed_payload"##)?;
                let (priority, checkpoint_payload) = stmt.query_row(&[&job_id, &worker_id], |row| {
                    let priority : i64 = row.get(0)?;
                    let payload : Option<Vec<u8>> = row.get(1)?;
                    Ok((priority, payload))
                })
                    .optional()?
                    .ok_or(Error::Expired)?;

                let mut stmt = tx.prepare_cached(
                    r##"INSERT INTO pending (job_id, priority, job_type, run_at, current_try, checkpointed_payload)
                    VALUES ($job_id, $priority, job_type, $next_run_time, $try_count, $payload)
                    )"##)?;
                stmt.execute(named_params!{
                    "$job_id": job_id,
                    "$priority": priority,
                    "$next_run_time": next_run_time,
                    "$try_count": next_try_count,
                    "$payload": checkpoint_payload,
                })?;

                let mut stmt = tx.prepare_cached(
                    r##"UPDATE active_jobs
                    SET run_info = json_array_append(run_info, $this_run_info)
                    WHERE job_id=$job_id"##,
                )?;
                stmt.execute(named_params! {
                    "$job_id": job_id,
                    "$this_run_info": this_run_info
                })?;
            }

            tx.commit()?;
            Ok(())
        }).await?;

        Ok(())
    }
}
