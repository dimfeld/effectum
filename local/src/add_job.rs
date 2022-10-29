use rusqlite::named_params;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{Error, NewJob, Queue, Result};

impl Queue {
    pub async fn add_job(&self, job_config: NewJob) -> Result<(i64, Uuid)> {
        let external_id: Uuid = ulid::Ulid::new().into();

        let job_type = job_config.job_type.clone();
        let task_id = self.state.write_db(move |db| {
            let tx = db.transaction()?;

            let priority = job_config.priority.unwrap_or(0);
            let run_time = job_config.run_at.unwrap_or_else(OffsetDateTime::now_utc).unix_timestamp();

            let task_id = {
                let mut add_job_stmt = tx.prepare_cached(r##"
                    INSERT INTO active_jobs
                    (external_id, job_type, priority, from_recurring_job, orig_run_at, run_at, payload,
                        current_try, max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
                        added_at, default_timeout, heartbeat_increment, run_info)
                    VALUES
                    ($external_id, $job_type, $priority, $from_recurring_job, $run_at, $run_at, $payload,
                        0, $max_retries, $backoff_multiplier, $backoff_randomization, $backoff_initial_interval,
                        $added_at, $default_timeout, $heartbeat_increment, '[]')
                "##)?;
                add_job_stmt.execute(named_params! {
                    "$external_id": &external_id,
                    "$job_type": job_config.job_type,
                    "$priority": priority,
                    "$from_recurring_job": job_config.recurring_job_id,
                    "$run_at": run_time,
                    "$payload": job_config.payload.as_slice(),
                    "$max_retries": job_config.retries.max_retries,
                    "$backoff_multiplier": job_config.retries.backoff_multiplier,
                    "$backoff_randomization": job_config.retries.backoff_randomization,
                    "$backoff_initial_interval": job_config.retries.backoff_initial_interval.whole_seconds(),
                    "$default_timeout" :job_config.timeout.whole_seconds(),
                    "$heartbeat_increment": job_config.heartbeat_increment.whole_seconds(),
                    "$added_at": OffsetDateTime::now_utc().unix_timestamp(),
                })?;

                tx.last_insert_rowid()
            };

            tx.commit()?;

            Ok::<_, Error>(task_id)
        })
        .await?;

        let workers = self.state.workers.read().await;
        workers.new_job_available(&job_type);

        Ok((task_id, external_id))
    }
}

#[cfg(test)]
mod tests {
    use time::OffsetDateTime;

    use crate::{test_util::create_test_queue, NewJob};

    #[tokio::test]
    async fn add_job() {
        let queue = create_test_queue();

        let job = NewJob {
            job_type: "a_job".to_string(),
            priority: Some(1),
            ..Default::default()
        };

        let (_, external_id) = queue.add_job(job).await.unwrap();
        let after_start_time = OffsetDateTime::now_utc();
        let status = queue.get_job_status(external_id).await.unwrap();

        assert_eq!(status.status, "pending");
        assert_eq!(status.id, external_id);
        assert_eq!(status.priority, 1);
        assert!(status.orig_run_at < after_start_time);
    }

    #[tokio::test]
    async fn add_job_at_time() {
        let queue = create_test_queue();

        let job_time = (OffsetDateTime::now_utc() + time::Duration::minutes(10))
            .replace_nanosecond(0)
            .unwrap();

        let job = NewJob {
            job_type: "a_job".to_string(),
            run_at: Some(job_time),
            ..Default::default()
        };

        let (_, external_id) = queue.add_job(job).await.unwrap();
        let status = queue.get_job_status(external_id).await.unwrap();

        assert_eq!(status.orig_run_at, job_time);
        assert_eq!(status.status, "pending");
        assert_eq!(status.id, external_id);
        assert_eq!(status.priority, 0);
    }
}
