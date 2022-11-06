use ahash::{HashMap, HashSet};
use tracing::Span;
use uuid::Uuid;

use crate::{
    db_writer::{
        add_job::{AddJobArgs, AddMultipleJobsArgs, AddMultipleJobsResult},
        DbOperation, DbOperationType,
    },
    worker::log_error,
    Error, NewJob, Queue, Result, SmartString,
};

impl Queue {
    /// Submit a job to the queue
    pub async fn add_job(&self, job_config: NewJob) -> Result<(i64, Uuid)> {
        let job_type = job_config.job_type.clone();
        let now = self.state.time.now();
        let run_time = job_config.run_at.unwrap_or(now);

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.state
            .db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: DbOperationType::AddJob(AddJobArgs {
                    job: job_config,
                    now,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let ids = result_rx.await.map_err(|_| Error::QueueClosed)??;

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

    /// Submit multiple jobs to the queue
    pub async fn add_jobs(&self, jobs: Vec<NewJob>) -> Result<Vec<(i64, Uuid)>> {
        let mut ready_job_types: HashSet<String> = HashSet::default();
        let mut pending_job_types: HashMap<String, i64> = HashMap::default();

        let now = self.state.time.now();
        let now_ts = now.unix_timestamp();
        for job_config in &jobs {
            let run_time = job_config
                .run_at
                .map(|t| t.unix_timestamp())
                .unwrap_or(now_ts);
            if run_time <= now_ts {
                ready_job_types.insert(job_config.job_type.clone());
            } else {
                pending_job_types
                    .entry(job_config.job_type.clone())
                    .and_modify(|e| *e = std::cmp::min(*e, run_time))
                    .or_insert(run_time);
            }
        }

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.state
            .db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: DbOperationType::AddMultipleJobs(AddMultipleJobsArgs {
                    jobs,
                    now,
                    result_tx,
                }),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let AddMultipleJobsResult { ids } = result_rx.await.map_err(|_| Error::QueueClosed)??;

        for (job_type, job_time) in pending_job_types {
            let mut job_type = SmartString::from(job_type);
            job_type.shrink_to_fit();
            log_error(self.state.pending_jobs_tx.send((job_type, job_time)).await);
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
    use crate::{test_util::create_test_queue, JobState, NewJob};

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

        assert_eq!(status.state, JobState::Pending);
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
        assert_eq!(status.state, JobState::Pending);
        assert_eq!(status.id, external_id);
        assert_eq!(status.priority, 0);
    }
}
