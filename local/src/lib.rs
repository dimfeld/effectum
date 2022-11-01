pub mod add_job;
mod error;
pub mod job_status;
mod migrations;
mod shared_state;
mod worker_list;

pub mod job;
pub mod job_registry;
mod pending_jobs;
mod sqlite_functions;
#[cfg(test)]
mod test_util;
pub mod worker;

use std::{path::Path, sync::Arc};

use deadpool_sqlite::{Hook, HookError, HookErrorCause};
pub use error::{Error, Result};
use pending_jobs::monitor_pending_jobs;
use rusqlite::Connection;
use shared_state::{SharedState, SharedStateData};
use sqlite_functions::register_functions;
use time::Duration;
use tokio::task::JoinHandle;
use worker_list::Workers;

pub(crate) type SmartString = smartstring::SmartString<smartstring::LazyCompact>;

#[derive(Debug, Clone)]
pub struct Retries {
    pub max_retries: u32,
    pub backoff_multiplier: f32,
    pub backoff_randomization: f32,
    pub backoff_initial_interval: Duration,
}

impl Default for Retries {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_multiplier: 2f32,
            backoff_randomization: 0.2,
            backoff_initial_interval: Duration::seconds(20),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NewJob {
    job_type: String,
    priority: Option<i64>,
    /// When to run the job. `None` means to run it right away.
    run_at: Option<time::OffsetDateTime>,
    payload: Vec<u8>,
    retries: Retries,
    timeout: time::Duration,
    heartbeat_increment: time::Duration,
    recurring_job_id: Option<i64>,
}

impl Default for NewJob {
    fn default() -> Self {
        Self {
            job_type: Default::default(),
            priority: Default::default(),
            run_at: Default::default(),
            payload: Default::default(),
            retries: Default::default(),
            timeout: Duration::minutes(5),
            heartbeat_increment: Duration::seconds(120),
            recurring_job_id: Default::default(),
        }
    }
}

struct Tasks {
    close: tokio::sync::watch::Sender<()>,
    worker_count_rx: tokio::sync::watch::Receiver<usize>,
    pending_jobs_monitor: JoinHandle<()>,
}

pub struct Queue {
    state: SharedState,
    tasks: Option<Tasks>,
}

impl Queue {
    /// Open or create a new Queue database at the given path.
    ///
    /// Note that if you use an existing database file, this queue will set the journal style to
    /// WAL mode.
    pub async fn new(file: &Path) -> Result<Queue> {
        let mut conn = Connection::open(file).map_err(Error::OpenDatabase)?;
        conn.pragma_update(None, "journal", "wal")
            .map_err(Error::OpenDatabase)?;

        register_functions(&mut conn)?;
        crate::migrations::migrate(&mut conn)?;

        let (close_tx, close_rx) = tokio::sync::watch::channel(());

        let read_conn_pool = deadpool_sqlite::Config::new(file)
            .builder(deadpool_sqlite::Runtime::Tokio1)?
            .recycle_timeout(Some(std::time::Duration::from_secs(5 * 60)))
            .post_create(Hook::async_fn(move |conn, _| {
                Box::pin(async move {
                    conn.interact(register_functions)
                        .await
                        .map_err(|e| HookError::Abort(HookErrorCause::Message(e.to_string())))?
                        .map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))?;
                    Ok(())
                })
            }))
            .build()?;

        let (worker_count_tx, worker_count_rx) = tokio::sync::watch::channel(0);
        let (pending_jobs_tx, pending_jobs_rx) = tokio::sync::mpsc::channel(10);

        let shared_state = SharedState(Arc::new(SharedStateData {
            db: std::sync::Mutex::new(conn),
            read_conn_pool,
            workers: tokio::sync::RwLock::new(Workers::new(worker_count_tx)),
            close: close_rx,
            time: crate::shared_state::Time::new(),
            pending_jobs_tx,
        }));

        let pending_jobs_monitor =
            monitor_pending_jobs(shared_state.clone(), pending_jobs_rx).await?;

        // TODO Optionally clean up running jobs here, treating them all as failures and scheduling
        // for retry. For later server mode, we probably want to do something more intelligent so
        // that we can continue to receive "job finished" notifications. This will probably involve
        // persisting the worker information to the database so we can properly recover it.

        // TODO task to monitor expired jobs
        // TODO task to schedule recurring jobs
        // TODO Optional task to delete old jobs from `done_jobs`

        let q = Queue {
            state: shared_state,
            tasks: Some(Tasks {
                close: close_tx,
                worker_count_rx,
                pending_jobs_monitor,
            }),
        };

        Ok(q)
    }

    async fn wait_for_workers_to_stop(
        tasks: &mut Tasks,
        timeout: std::time::Duration,
    ) -> Result<()> {
        if *tasks.worker_count_rx.borrow_and_update() == 0 {
            return Ok(());
        }

        let timeout = tokio::time::sleep(timeout);
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                _ = &mut timeout => return Err(Error::Timeout),
                res = tasks.worker_count_rx.changed() => {
                    if res.is_err() || *tasks.worker_count_rx.borrow() == 0 {
                        return Ok(());
                    }
                }
            }
        }
    }

    /// Stop the queue, and wait for existing workers to finish.
    pub async fn close(&mut self, timeout: std::time::Duration) -> Result<()> {
        if let Some(mut tasks) = self.tasks.take() {
            tasks.close.send(()).ok();
            Self::wait_for_workers_to_stop(&mut tasks, timeout).await?;
        }
        Ok(())
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        if let Some(tasks) = self.tasks.take() {
            tasks.close.send(()).ok();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use tracing::{event, Level};

    use crate::{
        job_registry::{JobDef, JobRegistry},
        test_util::{create_test_queue, wait_for, wait_for_job, TestContext, TestEnvironment},
        worker::Worker,
        NewJob,
    };

    #[tokio::test]
    async fn create_queue() {
        create_test_queue().await;
    }

    #[tokio::test]
    async fn run_job() {
        let test = TestEnvironment::new().await;

        let _worker = test.worker().build().await.expect("failed to build worker");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "counter".to_string(),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        wait_for_job("job to run", &test.queue, job_id).await;
    }

    #[tokio::test]
    async fn worker_gets_pending_jobs_when_starting() {
        let test = TestEnvironment::new().await;

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "counter".to_string(),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        let _worker = test.worker().build().await.expect("failed to build worker");

        wait_for_job("job to run", &test.queue, job_id).await;
    }

    #[tokio::test(start_paused = true)]
    async fn run_future_job() {
        let test = TestEnvironment::new().await;

        let _worker = test.worker().build().await.expect("failed to build worker");

        let run_at1 = test.time.now().replace_nanosecond(0).unwrap() + Duration::from_secs(10);
        let run_at2 = run_at1 + Duration::from_secs(10);

        // Schedule job 2 first, to ensure that it's actually sorting by run_at
        let (_, job_id2) = test
            .queue
            .add_job(NewJob {
                job_type: "push_payload".to_string(),
                payload: serde_json::to_vec("job 2").unwrap(),
                run_at: Some(run_at2),
                ..Default::default()
            })
            .await
            .expect("failed to add job 2");
        event!(Level::INFO, run_at=%run_at2, id=%job_id2, "scheduled job 2");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "push_payload".to_string(),
                payload: serde_json::to_vec("job 1").unwrap(),
                run_at: Some(run_at1),
                ..Default::default()
            })
            .await
            .expect("failed to add job 1");
        event!(Level::INFO, run_at=%run_at1, id=%job_id, "scheduled job 1");

        tokio::time::sleep_until(test.time.instant_for_timestamp(run_at1.unix_timestamp())).await;
        let status1 = wait_for_job("job 1 to run", &test.queue, job_id).await;
        event!(Level::INFO, ?status1);
        let started_at1 = status1.started_at.expect("started_at is set on job 1");
        event!(Level::INFO, orig_run_at=%status1.orig_run_at, run_at=%run_at1, "job 1");
        assert!(status1.orig_run_at >= run_at1);
        assert!(started_at1 >= run_at1);

        tokio::time::sleep_until(test.time.instant_for_timestamp(run_at2.unix_timestamp())).await;
        let status2 = wait_for_job("job 2 to run", &test.queue, job_id2).await;
        event!(Level::INFO, ?status2);
        let started_at2 = status2.started_at.expect("started_at is set on job 2");
        event!(Level::INFO, orig_run_at=%status2.orig_run_at, run_at=%run_at2, "job 2");
        assert!(status2.orig_run_at >= run_at2);
        assert!(started_at2 >= run_at2);

        assert_eq!(test.context.get_values().await, &["job 1", "job 2"]);
    }

    mod retry {
        use crate::{test_util::wait_for_job_status, Retries};

        use super::*;

        #[tokio::test(start_paused = true)]
        async fn success_after_retry() {
            let test = TestEnvironment::new().await;

            let _worker = test.worker().build().await.expect("failed to build worker");

            let (_, job_id) = test
                .queue
                .add_job(NewJob {
                    job_type: "retry".to_string(),
                    payload: serde_json::to_vec(&2).unwrap(),
                    retries: Retries {
                        max_retries: 2,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
                .expect("failed to add job");

            let status = wait_for_job("job to run", &test.queue, job_id).await;
            assert_eq!(status.run_info.len(), 3);
            assert!(!status.run_info[0].success);
            assert!(!status.run_info[1].success);
            assert!(status.run_info[2].success);

            assert_eq!(status.run_info[0].info.to_string(), "\"fail on try 0\"");
            assert_eq!(status.run_info[1].info.to_string(), "\"fail on try 1\"");
            assert_eq!(status.run_info[2].info.to_string(), "\"success on try 2\"");

            // Assert retry time is at least the multiplier time, but less than the additional time
            // that might be added by the randomization.
            let first_retry_time = status.run_info[1].start - status.run_info[0].start;
            event!(Level::INFO, %first_retry_time);
            assert!(first_retry_time >= Duration::from_secs(20));

            let second_retry_time = status.run_info[2].start - status.run_info[1].start;
            event!(Level::INFO, %second_retry_time);
            assert!(second_retry_time >= Duration::from_secs(40));
        }

        #[tokio::test(start_paused = true)]
        async fn exceed_max_retries() {
            let test = TestEnvironment::new().await;

            let _worker = test.worker().build().await.expect("failed to build worker");

            let (_, job_id) = test
                .queue
                .add_job(NewJob {
                    job_type: "retry".to_string(),
                    payload: serde_json::to_vec(&3).unwrap(),
                    retries: Retries {
                        max_retries: 2,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
                .expect("failed to add job");

            let status = wait_for_job_status("job to run", &test.queue, job_id, "failed").await;
            assert_eq!(status.run_info.len(), 3);
            assert!(!status.run_info[0].success);
            assert!(!status.run_info[1].success);
            assert!(!status.run_info[2].success);

            assert_eq!(status.run_info[0].info.to_string(), "\"fail on try 0\"");
            assert_eq!(status.run_info[1].info.to_string(), "\"fail on try 1\"");
            assert_eq!(status.run_info[2].info.to_string(), "\"fail on try 2\"");
        }

        #[tokio::test]
        #[ignore]
        async fn backoff_times() {
            todo!();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn explicit_complete() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn explicit_fail() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn remove_jobs() {
        unimplemented!();
    }

    #[tokio::test]
    #[ignore]
    async fn clear_jobs() {
        unimplemented!();
    }

    #[tokio::test]
    #[ignore]
    async fn recurring_jobs() {
        unimplemented!();
    }

    #[tokio::test]
    #[ignore]
    async fn job_type_subset() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn job_timeout() {
        // TODO Need to track by a specific job_run_id, not just the worker id, since
        // the next run of the job could assign it to the same worker again.
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn manual_heartbeat() {
        todo!();
    }

    #[tokio::test]
    #[ignore]
    async fn auto_heartbeat() {
        todo!();
    }

    #[tokio::test]
    async fn job_priority() {
        let test = TestEnvironment::new().await;

        let now = test.time.now();

        // Add two job. The low priority job has an earlier run_at time so it would normally run first,
        // but the high priority job should actually run first due to running in priority order.
        let (_, low_prio) = test
            .queue
            .add_job(NewJob {
                job_type: "push_payload".to_string(),
                payload: serde_json::to_vec("low").unwrap(),
                priority: Some(1),
                run_at: Some(now - Duration::from_secs(10)),
                ..Default::default()
            })
            .await
            .expect("adding low priority job");

        let (_, high_prio) = test
            .queue
            .add_job(NewJob {
                job_type: "push_payload".to_string(),
                payload: serde_json::to_vec("high").unwrap(),
                priority: Some(2),
                run_at: Some(now - Duration::from_secs(5)),
                ..Default::default()
            })
            .await
            .expect("adding high priority job");

        let _worker = test.worker().build().await.expect("failed to build worker");

        let status_high = wait_for_job("high priority job to run", &test.queue, high_prio).await;
        let status_low = wait_for_job("low priority job to run", &test.queue, low_prio).await;

        let high_started_at = status_high.started_at.expect("high priority job started");
        let low_started_at = status_low.started_at.expect("low priority job started");
        event!(Level::INFO, high_started_at=%high_started_at, low_started_at=%low_started_at, "job start times");
        assert!(high_started_at <= low_started_at);

        assert_eq!(test.context.get_values().await, vec!["high", "low"]);
    }

    #[tokio::test]
    #[ignore]
    async fn checkpoint() {
        // Set a checkpoint
        // Fail the first run
        // Ensure that the second run starts from the checkpoint.
        todo!();
    }

    mod concurrency {
        #[tokio::test]
        #[ignore]
        async fn limit_to_max_concurrency() {
            todo!();
        }

        #[tokio::test]
        #[ignore]
        async fn fetches_batch() {
            todo!();
        }

        #[tokio::test]
        #[ignore]
        async fn weighted_jobs() {
            todo!();
        }

        #[tokio::test]
        #[ignore]
        async fn fetches_again_at_min_concurrency() {
            todo!();
        }
    }

    #[tokio::test]
    #[ignore]
    async fn shutdown() {
        todo!();
    }
}
