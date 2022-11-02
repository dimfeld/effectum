use ahash::{HashSet, HashMap};
use rusqlite::{named_params, Statement, Transaction};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{worker::log_error, Error, NewJob, Queue, Result, SmartString};

const INSERT_QUERY: &str = r##"
    INSERT INTO active_jobs
    (external_id, job_type, priority, weight, from_recurring_job, orig_run_at, run_at, payload,
        current_try, max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info)
    VALUES
    ($external_id, $job_type, $priority, $weight, $from_recurring_job, $run_at, $run_at, $payload,
        0, $max_retries, $backoff_multiplier, $backoff_randomization, $backoff_initial_interval,
        $added_at, $default_timeout, $heartbeat_increment, '[]')
"##;

impl Queue {
    fn execute_add_job_stmt(tx: &Transaction, stmt: &mut Statement, now: OffsetDateTime, run_time: OffsetDateTime, job_config: &NewJob, from_recurring_job: Option<i64>) -> Result<(i64, Uuid)> {
        let external_id: Uuid = ulid::Ulid::new().into();
        stmt.execute(named_params! {
            "$external_id": &external_id,
            "$job_type": job_config.job_type,
            "$priority": job_config.priority,
            "$weight": job_config.weight,
            "$from_recurring_job": from_recurring_job,
            "$run_at": run_time.unix_timestamp(),
            "$payload": job_config.payload.as_slice(),
            "$max_retries": job_config.retries.max_retries,
            "$backoff_multiplier": job_config.retries.backoff_multiplier,
            "$backoff_randomization": job_config.retries.backoff_randomization,
            "$backoff_initial_interval": job_config.retries.backoff_initial_interval.whole_seconds(),
            "$default_timeout" :job_config.timeout.whole_seconds(),
            "$heartbeat_increment": job_config.heartbeat_increment.whole_seconds(),
            "$added_at": now.unix_timestamp(),
        })?;

        Ok((tx.last_insert_rowid(), external_id))
    }

    pub async fn add_job(&self, job_config: NewJob) -> Result<(i64, Uuid)> {
        let job_type = job_config.job_type.clone();
        let now = self.state.time.now();
        let run_time = job_config.run_at.unwrap_or(now);
        let ids = self.state.write_db(move |db| {
            let tx = db.transaction()?;

            let ids = {
                let mut add_job_stmt = tx.prepare_cached(INSERT_QUERY)?;

                Self::execute_add_job_stmt(&tx, &mut add_job_stmt, now, run_time, &job_config, None)?
            };

            tx.commit()?;

            Ok::<_, Error>(ids)
        })
        .await?;

        if run_time <= now {
            let workers = self.state.workers.read().await;
            workers.new_job_available(&job_type);
        } else {
            let mut job_type = SmartString::from(job_type);
            job_type.shrink_to_fit();
            log_error(
                self.state
                    .pending_jobs_tx
                    .send((job_type, run_time.unix_timestamp()))
                    .await,
            );
        }

        Ok(ids)
    }

    pub async fn add_jobs(&self, jobs: Vec<NewJob>) -> Result<Vec<(i64, Uuid)>> {
        let now = self.state.time.now();

        let (ids, ready_job_types, pending_job_types) = self.state.write_db(move |db| {
            let mut ready_job_types : HashSet<String> = HashSet::default();
            let mut pending_job_types : HashMap<String, i64> = HashMap::default();
            let tx = db.transaction()?;

            let mut ids = Vec::with_capacity(jobs.len());
            {
                let mut stmt = tx.prepare_cached(INSERT_QUERY)?;

                for job_config in jobs {
                    let run_time = job_config.run_at.unwrap_or(now);
                    let job_ids = Self::execute_add_job_stmt(&tx, &mut stmt, now, run_time, &job_config, None)?;

                    ids.push(job_ids);

                    if run_time <= now {
                        ready_job_types.insert(job_config.job_type.clone());
                    } else {
                        let ts = run_time.unix_timestamp();
                        pending_job_types
                            .entry(job_config.job_type)
                            .and_modify(|e| *e = std::cmp::min(*e, ts))
                            .or_insert(ts);
                    }
                }
            }

            tx.commit()?;

            Ok::<_, Error>((ids, ready_job_types, pending_job_types))
        }).await?;

        for (job_type, job_time) in pending_job_types {
            let mut job_type = SmartString::from(job_type);
            job_type.shrink_to_fit();
            log_error(
                self.state
                    .pending_jobs_tx
                    .send((job_type, job_time))
                    .await,
            );
        }

        if !ready_job_types.is_empty() {
            let workers = self.state.workers.read().await;
            for job_type in ready_job_types {
                workers.new_job_available(&job_type);
            }
        }

        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use crate::{test_util::create_test_queue, NewJob};

    #[tokio::test]
    async fn add_job() {
        let queue = create_test_queue().await;

        let job = NewJob {
            job_type: "a_job".to_string(),
            priority: 1,
            ..Default::default()
        };

        let (_, external_id) = queue.add_job(job).await.unwrap();
        let after_start_time = queue.state.time.now();
        let status = queue.get_job_status(external_id).await.unwrap();

        assert_eq!(status.status, "pending");
        assert_eq!(status.id, external_id);
        assert_eq!(status.priority, 1);
        assert!(status.orig_run_at < after_start_time);
    }

    #[tokio::test]
    async fn add_job_at_time() {
        let queue = create_test_queue().await;

        let job_time = (queue.state.time.now() + time::Duration::minutes(10))
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
