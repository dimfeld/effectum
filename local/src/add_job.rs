use rusqlite::named_params;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Error, NewJob, Queue, Result};

pub(crate) const ADD_JOB_QUERY: &str = r##"
    INSERT INTO active_jobs
    (external_id, job_type, priority, from_recurring_job, orig_run_at_time,
        payload, max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment)
    VALUES
    ($external_id, $job_type, $priority, $from_recurring_job, $orig_run_at_time,
        $payload, $max_retries, $backoff_multiplier, $backoff_randomization, $backoff_initial_interval,
        $added_at, $default_timeout, $heartbeat_increment)
"##;
pub(crate) const ADD_PENDING_JOB_QUERY: &str = r##"
    INSERT INTO pending
    (job_id, priority, job_type, run_at, current_try, checkpointed_payload)
    VALUES
    ($job_id, $priority, $job_type, $run_at, $current_try, $checkpointed_payload)
"##;

impl Queue {
    pub async fn add_job(
        &self,
        recurring_job_id: Option<i64>,
        job_config: NewJob,
    ) -> Result<(i64, Uuid)> {
        let external_id: Uuid = ulid::Ulid::new().into();

        let state = self.state.clone();
        let task_id = tokio::task::spawn_blocking(move || {
            let mut db = state.db.lock().unwrap();
            let tx = db.transaction()?;

            let priority = job_config.priority.unwrap_or(0);
            let run_time = job_config.run_at.unwrap_or_else(OffsetDateTime::now_utc).unix_timestamp();

            let task_id = {
                let mut add_job_stmt = tx.prepare_cached(ADD_JOB_QUERY)?;
                add_job_stmt.execute(named_params! {
                    "$external_id": &external_id,
                    "$job_type": job_config.job_type,
                    "$priority": priority,
                    "$from_recurring_job": recurring_job_id,
                    "$orig_run_at_time": run_time,
                    "$payload": job_config.payload.as_slice(),
                    "$max_retries": job_config.retries.max_retries,
                    "$backoff_multiplier": job_config.retries.backoff_multiplier,
                    "$backoff_randomization": job_config.retries.backoff_randomization,
                    "$backoff_initial_interval": job_config.retries.backoff_initial_interval.whole_seconds(),
                    "$default_timeout" :job_config.timeout.whole_seconds(),
                    "$heartbeat_increment": job_config.heartbeat_increment.whole_seconds(),
                    "$added_at": OffsetDateTime::now_utc().unix_timestamp(),
                })?;

                let task_id = tx.last_insert_rowid();

                let mut add_pending_job_stmt = tx.prepare_cached(ADD_PENDING_JOB_QUERY)?;
                add_pending_job_stmt.execute(named_params! {
                    "$job_id": task_id,
                    "$priority": priority,
                    "$job_type": job_config.job_type,
                    "$run_at": run_time,
                    "$current_try": 0,
                    "$checkpointed_payload": None::<&[u8]>,
                })?;

                task_id
            };

            tx.commit()?;

            Ok::<_, Error>(task_id)
        })
        .await??;

        self.state.notify_updated.notify_one();

        Ok((task_id, external_id))
    }
}

#[cfg(test)]
mod tests {
    use time::{Duration, OffsetDateTime};

    use crate::{test_util::create_test_queue, NewJob};

    #[tokio::test]
    async fn add_job() {
        let queue = create_test_queue();

        let job = NewJob {
            job_type: "a_job".to_string(),
            priority: Some(1),
            run_at: None,
            payload: Vec::new(),
            retries: crate::Retries::default(),
            timeout: Duration::minutes(5),
            heartbeat_increment: Duration::seconds(30),
        };

        let (_, external_id) = queue.add_job(None, job).await.unwrap();
        let after_start_time = OffsetDateTime::now_utc();
        let status = queue.get_job_status(external_id).await.unwrap();

        assert_eq!(status.status, "active");
        assert_eq!(status.id, external_id);
        assert!(status.orig_run_at_time < after_start_time);
    }
}
