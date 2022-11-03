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
use pending_jobs::monitor_pending_jobs;
use rusqlite::Connection;
use shared_state::{SharedState, SharedStateData};
use sqlite_functions::register_functions;
use time::Duration;
use tokio::task::JoinHandle;
use worker_list::Workers;

pub use error::{Error, Result};
pub use job::{Job, JobData};
pub use job_registry::{JobDef, JobDefBuilder, JobRegistry};
pub use worker::{Worker, WorkerBuilder};

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
    pub job_type: String,
    pub priority: i32,
    pub weight: u32,
    /// When to run the job. `None` means to run it right away.
    pub run_at: Option<time::OffsetDateTime>,
    pub payload: Vec<u8>,
    pub retries: Retries,
    pub timeout: time::Duration,
    pub heartbeat_increment: time::Duration,
}

impl Default for NewJob {
    fn default() -> Self {
        Self {
            job_type: Default::default(),
            priority: 0,
            weight: 1,
            run_at: Default::default(),
            payload: Default::default(),
            retries: Default::default(),
            timeout: Duration::minutes(5),
            heartbeat_increment: Duration::seconds(120),
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
    tasks: std::sync::Mutex<Option<Tasks>>,
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
        conn.pragma_update(None, "synchronous", "normal")
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

        // TODO sweeper task for expired jobs that might not have been caught by the normal mechanism
        // TODO task to schedule recurring jobs
        // TODO Optional task to delete old jobs from `done_jobs`

        let q = Queue {
            state: shared_state,
            tasks: std::sync::Mutex::new(Some(Tasks {
                close: close_tx,
                worker_count_rx,
                pending_jobs_monitor,
            })),
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
    pub async fn close(&self, timeout: std::time::Duration) -> Result<()> {
        let tasks = {
            let mut tasks_holder = self.tasks.lock().unwrap();
            tasks_holder.take()
        };

        if let Some(mut tasks) = tasks {
            tasks.close.send(()).ok();
            Self::wait_for_workers_to_stop(&mut tasks, timeout).await?;
        }

        Ok(())
    }
}

impl Drop for Queue {
    fn drop(&mut self) {
        let mut tasks = self.tasks.lock().unwrap();
        if let Some(tasks) = tasks.take() {
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
        test_util::{
            create_test_queue, wait_for, wait_for_job, wait_for_job_status, TestContext,
            TestEnvironment,
        },
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
    async fn run_multiple_jobs() {
        let test = TestEnvironment::new().await;

        let _worker = test.worker().build().await.expect("failed to build worker");

        let ids = test
            .queue
            .add_jobs(vec![
                NewJob {
                    job_type: "counter".to_string(),
                    ..Default::default()
                },
                NewJob {
                    job_type: "counter".to_string(),
                    ..Default::default()
                },
                NewJob {
                    job_type: "counter".to_string(),
                    ..Default::default()
                },
            ])
            .await
            .expect("failed to add job");

        for (_, job_id) in ids {
            wait_for_job("job to run", &test.queue, job_id).await;
        }
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

    #[tokio::test(start_paused = true)]
    async fn add_multiple_future_jobs() {
        let test = TestEnvironment::new().await;

        let _worker = test.worker().build().await.expect("failed to build worker");

        let run_at1 = test.time.now().replace_nanosecond(0).unwrap() + Duration::from_secs(10);
        let run_at2 = run_at1 + Duration::from_secs(10);

        // Schedule job 2 first, to ensure that it's actually sorting by run_at
        let ids = test
            .queue
            .add_jobs(vec![
                NewJob {
                    job_type: "push_payload".to_string(),
                    payload: serde_json::to_vec("job 2").unwrap(),
                    run_at: Some(run_at2),
                    ..Default::default()
                },
                NewJob {
                    job_type: "push_payload".to_string(),
                    payload: serde_json::to_vec("job 1").unwrap(),
                    run_at: Some(run_at1),
                    ..Default::default()
                },
            ])
            .await
            .expect("Failed to add jobs");

        let job_id2 = ids[0].1;
        let job_id = ids[1].1;

        event!(Level::INFO, run_at=%run_at1, id=%job_id, "scheduled job 1");
        event!(Level::INFO, run_at=%run_at2, id=%job_id2, "scheduled job 2");

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

    #[tokio::test(start_paused = true)]
    async fn explicit_finish() {
        let mut test = TestEnvironment::new().await;

        let explicit_complete_job = JobDef::builder(
            "explicit_complete",
            |job, _context: Arc<TestContext>| async move {
                if job.current_try == 0 {
                    job.fail("explicit fail").await?;
                } else {
                    job.complete("explicit succeed").await?;
                }

                Ok::<_, crate::Error>("This should not do anything")
            },
        )
        .build();

        test.registry.add(&explicit_complete_job);

        let _worker = test.worker().build().await.expect("failed to build worker");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "explicit_complete".to_string(),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        let status = wait_for_job("job to run", &test.queue, job_id).await;

        assert_eq!(status.run_info.len(), 2);
        assert!(!status.run_info[0].success);
        assert!(status.run_info[1].success);

        assert_eq!(status.run_info[0].info.to_string(), "\"explicit fail\"");
        assert_eq!(status.run_info[1].info.to_string(), "\"explicit succeed\"");
    }

    #[tokio::test]
    async fn job_type_subset() {
        let test = TestEnvironment::new().await;

        let mut run_jobs = Vec::new();
        let mut no_run_jobs = Vec::new();

        run_jobs.extend(
            test.queue
                .add_jobs(vec![
                    NewJob {
                        job_type: "counter".to_string(),
                        ..Default::default()
                    },
                    NewJob {
                        job_type: "push_payload".to_string(),
                        payload: serde_json::to_vec(&"test").unwrap(),
                        ..Default::default()
                    },
                ])
                .await
                .expect("failed to add jobs"),
        );

        no_run_jobs.push(
            test.queue
                .add_job(NewJob {
                    job_type: "sleep".to_string(),
                    payload: serde_json::to_vec(&"test").unwrap(),
                    ..Default::default()
                })
                .await
                .expect("failed to add job")
                .1,
        );

        let _worker = test
            .worker()
            .job_types(&["counter", "push_payload"])
            .build()
            .await
            .expect("failed to build worker");

        run_jobs.extend(
            test.queue
                .add_jobs(vec![
                    NewJob {
                        job_type: "counter".to_string(),
                        ..Default::default()
                    },
                    NewJob {
                        job_type: "push_payload".to_string(),
                        payload: serde_json::to_vec(&"test").unwrap(),
                        ..Default::default()
                    },
                ])
                .await
                .expect("Failed to add jobs"),
        );

        no_run_jobs.push(
            test.queue
                .add_job(NewJob {
                    job_type: "sleep".to_string(),
                    payload: serde_json::to_vec(&"test").unwrap(),
                    ..Default::default()
                })
                .await
                .expect("failed to add job")
                .1,
        );

        for (_, job_id) in run_jobs {
            event!(Level::INFO, %job_id, "checking job that should run");
            let status = wait_for_job("job to run", &test.queue, job_id).await;
            assert!(status.run_info[0].success);
        }

        for job_id in no_run_jobs {
            event!(Level::INFO, %job_id, "checking job that should not run");
            let status = wait_for_job_status("job to run", &test.queue, job_id, "pending").await;
            assert_eq!(status.run_info.len(), 0);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn job_timeout() {
        // TODO Need to track by a specific job_run_id, not just the worker id, since
        // the next run of the job could assign it to the same worker again.
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("failed to build worker");
        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "sleep".to_string(),
                payload: serde_json::to_vec(&10000).unwrap(),
                timeout: time::Duration::milliseconds(5000),
                retries: crate::Retries {
                    max_retries: 2,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        let status = wait_for_job_status("job to fail", &test.queue, job_id, "failed").await;

        assert_eq!(status.run_info.len(), 3);
        assert!(!status.run_info[0].success);
        assert!(!status.run_info[1].success);
        assert!(!status.run_info[2].success);
        assert_eq!(status.run_info[0].info.to_string(), "\"Job expired\"");
        assert_eq!(status.run_info[1].info.to_string(), "\"Job expired\"");
        assert_eq!(status.run_info[2].info.to_string(), "\"Job expired\"");
    }

    // TODO Run this in virtual time once https://github.com/tokio-rs/tokio/pull/5115 is merged.
    #[tokio::test]
    async fn manual_heartbeat() {
        let mut test = TestEnvironment::new().await;
        let job_def = JobDef::builder(
            "manual_heartbeat",
            |job, _context: Arc<TestContext>| async move {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                job.heartbeat().await?;
                tokio::time::sleep(std::time::Duration::from_millis(750)).await;
                Ok::<_, crate::Error>(())
            },
        )
        .build();

        test.registry.add(&job_def);
        let _worker = test.worker().build().await.expect("failed to build worker");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "manual_heartbeat".to_string(),
                retries: crate::Retries {
                    max_retries: 0,
                    ..Default::default()
                },
                timeout: time::Duration::milliseconds(1000),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        wait_for_job("job to succeed", &test.queue, job_id).await;
    }

    // TODO Run this in virtual time once https://github.com/tokio-rs/tokio/pull/5115 is merged.
    #[tokio::test]
    async fn auto_heartbeat() {
        let mut test = TestEnvironment::new().await;
        let job_def = JobDef::builder(
            "auto_heartbeat",
            |_job, _context: Arc<TestContext>| async move {
                tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
                Ok::<_, crate::Error>(())
            },
        )
        .autoheartbeat(true)
        .build();

        test.registry.add(&job_def);
        let _worker = test.worker().build().await.expect("failed to build worker");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "auto_heartbeat".to_string(),
                retries: crate::Retries {
                    max_retries: 0,
                    ..Default::default()
                },
                timeout: time::Duration::milliseconds(2000),
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        wait_for_job("job to succeed", &test.queue, job_id).await;
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
                priority: 1,
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
                priority: 2,
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
    async fn checkpoint() {
        let mut test = TestEnvironment::new().await;
        let job_def = JobDef::builder(
            "checkpoint_job",
            |job, _context: Arc<TestContext>| async move {
                let payload = job.json_payload::<String>().unwrap();
                event!(Level::INFO, %job, %payload, "running checkpoint job");
                match job.current_try {
                    0 => {
                        assert_eq!(payload, "initial", "checkpoint when loaded");
                        job.checkpoint_json("first").await.unwrap();
                        Err("fail 1")
                    }
                    1 => {
                        assert_eq!(payload, "first", "checkpoint after first run");
                        job.checkpoint_json("second").await.unwrap();
                        Err("fail 2")
                    }
                    2 => {
                        assert_eq!(payload, "second", "checkpoint after second run");
                        job.checkpoint_json("third").await.unwrap();
                        Ok("success")
                    }
                    _ => panic!("unexpected try number"),
                }
            },
        )
        .build();

        test.registry.add(&job_def);
        let _worker = test.worker().build().await.expect("failed to build worker");

        let (_, job_id) = test
            .queue
            .add_job(NewJob {
                job_type: "checkpoint_job".to_string(),
                payload: serde_json::to_vec("initial").unwrap(),
                retries: crate::Retries {
                    max_retries: 3,
                    backoff_multiplier: 1.0,
                    backoff_initial_interval: time::Duration::milliseconds(1),
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .expect("failed to add job");

        let status = wait_for_job("job to succeed", &test.queue, job_id).await;

        assert_eq!(status.status, "success");
        assert_eq!(status.run_info.len(), 3);
        assert!(!status.run_info[0].success);
        assert!(!status.run_info[1].success);
        assert!(status.run_info[2].success);
        assert_eq!(status.run_info[0].info.to_string(), "\"fail 1\"");
        assert_eq!(status.run_info[1].info.to_string(), "\"fail 2\"");
        assert_eq!(status.run_info[2].info.to_string(), "\"success\"");
    }

    mod concurrency {
        use super::*;

        #[tokio::test(start_paused = true)]
        async fn limit_to_max_concurrency() {
            let test = TestEnvironment::new().await;

            let mut jobs = Vec::new();
            for _ in 0..10 {
                let job_id = test
                    .queue
                    .add_job(NewJob {
                        job_type: "max_count".to_string(),
                        ..Default::default()
                    })
                    .await
                    .expect("Adding job")
                    .1;
                jobs.push(job_id);
            }

            let _worker = test
                .worker()
                .max_concurrency(5)
                .build()
                .await
                .expect("failed to build worker");

            for job_id in jobs {
                wait_for_job("job to succeed", &test.queue, job_id).await;
            }

            assert_eq!(test.context.max_count().await, 5);
        }

        #[tokio::test]
        #[ignore]
        async fn fetches_batch() {
            todo!();
        }

        #[tokio::test(start_paused = true)]
        #[ignore = "Reenable once https://github.com/tokio-rs/tokio/pull/5115 is merged."]
        async fn weighted_jobs() {
            let test = TestEnvironment::new().await;

            let mut jobs = Vec::new();
            for _ in 0..10 {
                let job_id = test
                    .queue
                    .add_job(NewJob {
                        job_type: "max_count".to_string(),
                        weight: 3,
                        ..Default::default()
                    })
                    .await
                    .expect("Adding job")
                    .1;
                jobs.push(job_id);
            }

            let worker = test
                .worker()
                .max_concurrency(10)
                .build()
                .await
                .expect("failed to build worker");

            for job_id in jobs {
                wait_for_job("job to succeed", &test.queue, job_id).await;
            }

            // With a weight of 3, there should be at most three jobs (10 / 3) running at once.
            assert_eq!(test.context.max_count().await, 3);
            let counts = worker.counts();

            assert_eq!(counts.started, 10);
            assert_eq!(counts.finished, 10);
        }

        #[tokio::test(start_paused = true)]
        #[ignore = "Reenable once https://github.com/tokio-rs/tokio/pull/5115 is merged."]
        async fn fetches_again_at_min_concurrency() {
            let test = TestEnvironment::new().await;

            let mut jobs = Vec::new();
            for i in 0..20 {
                let time = (i + 1) * 1000;
                let job_id = test
                    .queue
                    .add_job(NewJob {
                        job_type: "sleep".to_string(),
                        payload: serde_json::to_vec(&time).unwrap(),
                        timeout: time::Duration::minutes(20),
                        ..Default::default()
                    })
                    .await
                    .expect("Adding job")
                    .1;
                jobs.push(job_id);
            }

            let _worker = test
                .worker()
                .min_concurrency(5)
                .max_concurrency(10)
                .build()
                .await
                .expect("failed to build worker");

            let mut statuses = Vec::new();
            for job_id in jobs {
                statuses.push(wait_for_job("job to succeed", &test.queue, job_id).await);
            }

            // First 10 jobs should all start at same time.
            let batch1_time = statuses[0].started_at.unwrap();
            for i in 1..10 {
                assert_eq!(batch1_time, statuses[i].started_at.unwrap());
            }

            // Next 5 jobs should start together
            let batch2_time = statuses[10].started_at.unwrap();
            assert!(batch2_time > batch1_time);
            for i in 11..15 {
                assert_eq!(batch2_time, statuses[i].started_at.unwrap());
            }

            // Final 5 jobs should start together
            let batch3_time = statuses[15].started_at.unwrap();
            assert!(batch3_time > batch2_time);
            for i in 16..20 {
                assert_eq!(batch3_time, statuses[i].started_at.unwrap());
            }
        }
    }

    #[tokio::test]
    #[ignore]
    async fn shutdown() {
        todo!();
    }

    mod unimplemented {
        #[tokio::test]
        #[ignore = "not implemented yet"]
        async fn remove_jobs() {
            unimplemented!();
        }

        #[tokio::test]
        #[ignore = "not implemented yet"]
        async fn clear_jobs() {
            unimplemented!();
        }

        #[tokio::test]
        #[ignore = "not implemented yet"]
        async fn recurring_jobs() {
            unimplemented!();
        }
    }
}
