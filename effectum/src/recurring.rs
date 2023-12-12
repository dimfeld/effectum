use std::{rc::Rc, str::FromStr, time::Duration};

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{event, instrument, Level, Span};
use uuid::Uuid;

use crate::{
    db_writer::{
        recurring::{AddRecurringJobArgs, DeleteRecurringJobArgs},
        DbOperation, UpsertMode,
    },
    shared_state::SharedState,
    Error, Job, JobBuilder, JobStatus, Queue,
};

pub(crate) fn create_job_from_recurring_template(
    db: &rusqlite::Connection,
    now: OffsetDateTime,
    from_time: OffsetDateTime,
    ids: Vec<rusqlite::types::Value>,
) -> Result<Vec<Job>, Error> {
    let query = r##"SELECT job_id,
                job_type, priority, weight, payload, max_retries,
                backoff_multiplier, backoff_randomization, backoff_initial_interval,
                default_timeout, heartbeat_increment, schedule
            FROM jobs
            JOIN recurring ON job_id = base_job_id
            WHERE status = 'recurring_base' AND job_id IN rarray(?)
            "##;

    let mut stmt = db.prepare_cached(query)?;

    let rows = stmt
        .query_and_then(params![Rc::new(ids)], |row| {
            let job_id: i64 = row.get(0)?;
            let job_type = row
                .get_ref(1)?
                .as_str()
                .map(|s| s.to_string())
                .map_err(|e| Error::ColumnType(e.into(), "job_type"))?;
            let priority = row.get(2).map_err(|e| Error::ColumnType(e, "priority"))?;
            let weight = row.get(3).map_err(|e| Error::ColumnType(e, "weight"))?;
            let payload = row.get(4).map_err(|e| Error::ColumnType(e, "payload"))?;
            let max_retries = row
                .get(5)
                .map_err(|e| Error::ColumnType(e, "max_retries"))?;
            let backoff_multiplier = row
                .get(6)
                .map_err(|e| Error::ColumnType(e, "backoff_multiplier"))?;
            let backoff_randomization = row
                .get(7)
                .map_err(|e| Error::ColumnType(e, "backoff_randomization"))?;
            let backoff_initial_interval = row
                .get(8)
                .map_err(|e| Error::ColumnType(e, "backoff_initial_interval"))?;
            let default_timeout = row
                .get(9)
                .map_err(|e| Error::ColumnType(e, "default_timeout"))?;
            let heartbeat_increment = row
                .get(10)
                .map_err(|e| Error::ColumnType(e, "heartbeat_increment"))?;
            let schedule = row
                .get_ref(11)?
                .as_str()
                .map_err(|e| Error::ColumnType(e.into(), "schedule"))
                .and_then(|s| {
                    serde_json::from_str::<RecurringJobSchedule>(s)
                        .map_err(|_| Error::InvalidSchedule)
                })?;

            let next_job_time = schedule.find_next_job_time(now, from_time)?;
            let job = JobBuilder::new(job_type)
                .priority(priority)
                .weight(weight)
                .payload(payload)
                .max_retries(max_retries)
                .backoff_multiplier(backoff_multiplier)
                .backoff_randomization(backoff_randomization)
                .backoff_initial_interval(Duration::from_secs(backoff_initial_interval))
                .timeout(Duration::from_secs(default_timeout))
                .heartbeat_increment(Duration::from_secs(heartbeat_increment))
                .from_recurring(job_id)
                .run_at(next_job_time)
                .build();

            Ok::<_, Error>(job)
        })?
        .collect::<Result<Vec<Job>, Error>>()?;

    Ok::<_, Error>(rows)
}

/// Schedule jobs from recurring templates.
#[instrument(level = "debug", skip(queue))]
pub(crate) async fn schedule_recurring_jobs(
    queue: &SharedState,
    from_time: OffsetDateTime,
    ids: &[i64],
) -> Result<(), Error> {
    let conn = queue.read_conn_pool.get().await?;
    let ids = ids
        .iter()
        .map(|id| rusqlite::types::Value::from(*id))
        .collect::<Vec<_>>();
    let now = queue.time.now();
    let jobs = conn
        .interact(move |db| create_job_from_recurring_template(db, now, from_time, ids))
        .await??;

    queue.add_jobs(jobs).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename = "snake_case")]
/// The schedule definition for a recurring job.
pub enum RecurringJobSchedule {
    /// A schedule based on a cron-style schedule string.
    Cron {
        /// The cron string
        spec: String,
    },
    /// Repeat the job on this interval.
    RepeatEvery {
        /// The interval
        interval: Duration,
    },
}

#[derive(Debug)]
/// Information about a recurring job.
pub struct RecurringJobInfo {
    /// The template that new instances of the recurring job are based on.
    pub base_job: JobStatus,
    /// The schedule for the recurring job.
    pub schedule: RecurringJobSchedule,
    /// The status of the last (or current) run.
    pub last_run: Option<JobStatus>,
    /// The job ID of the next job to run and its next time, if it's not currently running.
    pub next_run: Option<(Uuid, OffsetDateTime)>,
}

impl RecurringJobSchedule {
    /// Create a RecurringJobSchedule from a cron-style schedule string.
    pub fn from_cron_string(spec: String) -> Result<Self, Error> {
        // Make sure it parses ok.
        cron::Schedule::from_str(&spec).map_err(|_| Error::InvalidSchedule)?;
        Ok(Self::Cron { spec })
    }

    pub(crate) fn find_next_job_time(
        &self,
        now: OffsetDateTime,
        after: OffsetDateTime,
    ) -> Result<OffsetDateTime, Error> {
        match self {
            RecurringJobSchedule::Cron { spec } => {
                let sched = cron::Schedule::from_str(spec).map_err(|_| Error::InvalidSchedule)?;
                let now = chrono::DateTime::<chrono::Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp_opt(now.unix_timestamp(), 0)
                        .ok_or(Error::TimestampOutOfRange("now"))?,
                    chrono::Utc,
                );
                let after = chrono::DateTime::<chrono::Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp_opt(after.unix_timestamp(), 0)
                        .ok_or(Error::TimestampOutOfRange("after"))?,
                    chrono::Utc,
                );

                let mut schedule_iter = sched.after(&after);
                let mut next = schedule_iter.next().ok_or(Error::InvalidSchedule)?;

                while next < now {
                    // Walk the time up to the next one in the future. This ensures
                    // that the timer catches up without running a bunch of jobs, when it's way
                    // behind due to the server being shut down.
                    next = schedule_iter.next().ok_or(Error::InvalidSchedule)?;
                }

                // The `cron` package uses chrono but everything else here uses `time`, so convert.
                let next = OffsetDateTime::from_unix_timestamp(next.timestamp())
                    .map_err(|_| Error::InvalidSchedule)?;
                Ok(next)
            }
            RecurringJobSchedule::RepeatEvery { interval } => {
                let mut result = after + *interval;
                while result < now {
                    result += *interval;
                }
                Ok(result)
            }
        }
    }
}

impl Queue {
    /// Add a new recurring job to the queue, returning an error if a job with this external ID
    /// already exists.
    pub async fn add_recurring_job(
        &self,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
        run_immediately: bool,
    ) -> Result<(), Error> {
        self.do_recurring_job_update(UpsertMode::Add, id, schedule, job, run_immediately)
            .await
    }

    /// Update a recurring job. Returns an error if the job does not exist.
    pub async fn update_recurring_job(
        &self,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
    ) -> Result<(), Error> {
        self.do_recurring_job_update(UpsertMode::Update, id, schedule, job, false)
            .await
    }

    /// Add a new recurring job, or update an existing one.
    pub async fn upsert_recurring_job(
        &self,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
        run_immediately_on_insert: bool,
    ) -> Result<(), Error> {
        self.do_recurring_job_update(
            UpsertMode::Upsert,
            id,
            schedule,
            job,
            run_immediately_on_insert,
        )
        .await
    }

    async fn do_recurring_job_update(
        &self,
        upsert_mode: UpsertMode,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
        run_immediately_on_insert: bool,
    ) -> Result<(), Error> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let now = self.state.time.now();
        let job_type = job.job_type.to_string();
        self.state
            .db_write_tx
            .send(DbOperation {
                worker_id: 0,
                operation: crate::db_writer::DbOperationType::AddRecurringJob(
                    AddRecurringJobArgs {
                        external_id: id,
                        now,
                        schedule,
                        upsert_mode,
                        job,
                        result_tx,
                        run_immediately_on_insert,
                    },
                ),
                span: Span::current(),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let add_result = result_rx.await.map_err(|_| Error::QueueClosed)??;
        if let Some(run_at) = add_result.new_run_at {
            event!(Level::DEBUG, ?run_at, "Setting up job notify");
            self.state.notify_for_job_type(now, run_at, &job_type).await;
        }

        Ok(())
    }

    /// Remove a recurring job and unschedule any scheduled jobs for it.
    pub async fn delete_recurring_job(&self, id: String) -> Result<(), Error> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        self.state
            .db_write_tx
            .send(DbOperation {
                worker_id: 0,
                span: Span::current(),
                operation: crate::db_writer::DbOperationType::DeleteRecurringJob(
                    DeleteRecurringJobArgs { id, result_tx },
                ),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        result_rx.await.map_err(|_| Error::QueueClosed)??;
        Ok(())
    }

    /// Return information about a recurring job and its latest execution
    pub async fn get_recurring_job_info(&self, id: String) -> Result<RecurringJobInfo, Error> {
        let conn = self.state.read_conn_pool.get().await?;
        let recurring_info = conn
            .interact(move |db| {
                let mut base_info_stmt = db.prepare_cached(
                    r##"SELECT base_job_id, schedule
                FROM recurring
                WHERE external_id = ?"##,
                )?;
                let (base_job_id, schedule) = base_info_stmt
                    .query_row(params![id], |row| {
                        let base_job_id = row.get(0)?;
                        let schedule = row.get::<_, String>(1)?;
                        Ok((base_job_id, schedule))
                    })
                    .optional()?
                    .ok_or(Error::NotFound)?;

                let schedule: RecurringJobSchedule =
                    serde_json::from_str(&schedule).map_err(|_| Error::InvalidSchedule)?;

                let base_job_info =
                    Self::run_job_status_query(db, crate::job_status::JobIdQuery::Id(base_job_id))?;

                let mut find_last_run_stmt = db.prepare_cached(
                    r##"SELECT job_id
                    FROM jobs
                    WHERE from_base_job = ? AND started_at IS NOT NULL
                    ORDER BY started_at DESC
                    LIMIT 1"##,
                )?;

                let last_run_id = find_last_run_stmt
                    .query_row([base_job_id], |row| row.get::<_, i64>(0))
                    .optional()?;

                let last_run = if let Some(last_run_id) = last_run_id {
                    Some(Self::run_job_status_query(
                        db,
                        crate::job_status::JobIdQuery::Id(last_run_id),
                    )?)
                } else {
                    None
                };

                let mut next_run_stmt = db.prepare_cached(
                    r##"SELECT external_id, orig_run_at
                    FROM jobs
                    WHERE from_base_job = ? AND started_at IS NULL
                    LIMIT 1"##,
                )?;

                let next_run = next_run_stmt
                    .query_row([base_job_id], |row| {
                        Ok((row.get::<_, Uuid>(0)?, row.get::<_, i64>(1)?))
                    })
                    .optional()?
                    .map(|(id, time)| {
                        let time = OffsetDateTime::from_unix_timestamp(time)
                            .map_err(|_| Error::TimestampOutOfRange("orig_run_at"))?;
                        Ok::<_, Error>((id, time))
                    })
                    .transpose()?;

                Ok::<_, Error>(RecurringJobInfo {
                    base_job: base_job_info,
                    schedule,
                    last_run,
                    next_run,
                })
            })
            .await??;

        Ok(recurring_info)
    }

    /// Return the IDs of all recurring jobs with the given prefix
    pub async fn list_recurring_jobs_with_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Error> {
        let conn = self.state.read_conn_pool.get().await?;
        let prefix = format!("{prefix}%");
        let ids = conn
            .interact(move |db| {
                let mut stmt = db.prepare_cached(
                    r##"SELECT external_id
                        FROM recurring
                        WHERE external_id LIKE ?"##,
                )?;
                let ids = stmt
                    .query_map([prefix], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, Error>(ids)
            })
            .await??;

        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tracing::{event, Level};

    use crate::{
        recurring::RecurringJobSchedule,
        test_util::{wait_for, wait_for_job, wait_for_job_status, TestEnvironment},
        Error, JobBuilder, JobState,
    };

    #[tokio::test(start_paused = true)]
    async fn simple_recurring() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let start = test.time.now().replace_nanosecond(0).unwrap();
        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job, false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        assert!(job_status.last_run.is_none());
        let (first_job_id, first_run_at) = job_status.next_run.expect("next_run_at");

        let first_run_at = first_run_at.replace_nanosecond(0).unwrap();
        assert_eq!(
            first_run_at,
            start + Duration::from_secs(10),
            "first_run_at"
        );
        assert_eq!(job_status.schedule, schedule);

        tokio::time::sleep_until(
            test.time
                .instant_for_timestamp(first_run_at.unix_timestamp()),
        )
        .await;

        let result = wait_for_job("first run", &test.queue, first_job_id).await;
        assert!(result.started_at.expect("started_at") >= start + Duration::from_secs(10));
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        let last_run = job_status.last_run.as_ref().expect("last_run");
        assert_eq!(last_run.id, first_job_id);
        assert_eq!(last_run.orig_run_at, first_run_at);

        event!(Level::INFO, ?job_status, "status after first job finished");
        let (second_job_id, second_run_time) = job_status.next_run.expect("next_run");
        assert_ne!(
            first_job_id, second_job_id,
            "second job must have different id from first job"
        );
        assert_eq!(second_run_time, first_run_at + Duration::from_secs(10));

        tokio::time::sleep_until(
            test.time
                .instant_for_timestamp(second_run_time.unix_timestamp()),
        )
        .await;

        let result = wait_for_job("second run", &test.queue, second_job_id).await;
        assert!(result.started_at.expect("started_at") >= second_run_time);

        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn run_immediately_on_add() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 5 }))
            .expect("json_payload")
            .build();

        let start = test.time.now().replace_nanosecond(0).unwrap();
        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(2),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job, true)
            .await
            .expect("add_recurring_job");

        let job_status = wait_for("first run", || async {
            let val = test
                .context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed);
            if val < 1 {
                return Err("first task did not finish".to_string());
            }

            let job_status = test
                .queue
                .get_recurring_job_info("job_id".to_string())
                .await
                .expect("Retrieving job status");

            if !job_status
                .last_run
                .as_ref()
                .map(|j| j.state == JobState::Succeeded)
                .unwrap_or(false)
            {
                return Err("task status not updated yet".to_string());
            }

            Ok(job_status)
        })
        .await;

        event!(Level::INFO, ?job_status, "status after first job");
        // Depending on how fast things go, the job may or may not have started once we get here,
        // so we have to check both next_run and last_run.
        let (first_job_id, first_run_at) = job_status
            .last_run
            .map(|last_run| (last_run.id, last_run.orig_run_at))
            .expect("retrieving first run info");
        let first_run_at = first_run_at.replace_nanosecond(0).unwrap();
        assert_eq!(
            first_run_at, start,
            "first invocation should have started right away"
        );
        assert_eq!(job_status.schedule, schedule);

        let result = test
            .queue
            .get_job_status(first_job_id)
            .await
            .expect("checking first job status");
        assert!(result.started_at.expect("started_at") <= start + Duration::from_secs(1));
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        let last_run = job_status.last_run.as_ref().expect("last_run");
        assert_eq!(last_run.id, first_job_id);
        assert_eq!(last_run.orig_run_at, first_run_at);

        event!(Level::INFO, ?job_status, "status after first job finished");
        let (second_job_id, second_run_time) = job_status.next_run.expect("next_run");
        assert_ne!(
            first_job_id, second_job_id,
            "second job must have different id from first job"
        );
        assert_eq!(second_run_time, start + Duration::from_secs(2));

        let result = wait_for_job("second run", &test.queue, second_job_id).await;
        assert!(result.started_at.expect("started_at") >= second_run_time);
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_job_gets_rescheduled() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("retry")
            .json_payload(&serde_json::json!(20))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(400),
        };
        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job, true)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        assert!(job_status.last_run.is_none());
        let (first_job_id, first_run_at) = job_status.next_run.expect("next_run_at");
        assert_eq!(job_status.schedule, schedule);

        wait_for_job_status("first run", &test.queue, first_job_id, JobState::Failed).await;

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        event!(Level::INFO, ?job_status, "status after first job failed");

        let last_run = job_status.last_run.as_ref().expect("last_run");
        assert_eq!(last_run.id, first_job_id);
        assert_eq!(last_run.orig_run_at, first_run_at);

        let (second_job_id, second_run_time) = job_status.next_run.expect("next_run");
        assert_ne!(
            first_job_id, second_job_id,
            "second job must have different id from first job"
        );
        assert_eq!(second_run_time, first_run_at + Duration::from_secs(400));
    }

    #[tokio::test]
    async fn add_with_bad_schedule() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 5 }))
            .expect("json_payload")
            .build();
        let schedule = RecurringJobSchedule::Cron {
            spec: "2 1".to_string(),
        };

        let err = test
            .queue
            .add_recurring_job("job_id".to_string(), schedule, job, false)
            .await
            .expect_err("add_recurring_job");
        println!("{err:?}");
    }

    #[tokio::test]
    async fn update_to_bad_schedule() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 5 }))
            .expect("json_payload")
            .build();
        let schedule = RecurringJobSchedule::Cron {
            spec: "* * * * * 5".to_string(),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule, job.clone(), false)
            .await
            .expect("add_recurring_job");

        let bad_schedule = RecurringJobSchedule::Cron {
            spec: "* * * 5".to_string(),
        };
        let err = test
            .queue
            .update_recurring_job("job_id".to_string(), bad_schedule, job.clone())
            .await
            .expect_err("add_recurring_job");

        assert!(matches!(err, Error::InvalidSchedule));
    }

    #[tokio::test(start_paused = true)]
    /// Ensure that when the next task is scheduled with an "every X duration" schedule, its next time
    /// is based on the previously scheduled time, not when the task was actually started or
    /// finished.
    async fn next_time_based_on_last_scheduled_time() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        // Sleep long enough to make it very obvious if the scheduling is off.
        let job = JobBuilder::new("sleep")
            .json_payload(&serde_json::json!(7000))
            .expect("json_payload")
            .build();

        let start = test.time.now().replace_nanosecond(0).unwrap();
        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job, false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        assert!(job_status.last_run.is_none());
        let (first_job_id, first_run_at) = job_status.next_run.expect("next_run_at");

        let first_run_at = first_run_at.replace_nanosecond(0).unwrap();
        assert_eq!(
            first_run_at,
            start + Duration::from_secs(10),
            "first_run_at"
        );
        assert_eq!(job_status.schedule, schedule);

        tokio::time::sleep_until(
            test.time
                .instant_for_timestamp(first_run_at.unix_timestamp()),
        )
        .await;

        let result = wait_for_job("first run", &test.queue, first_job_id).await;
        assert!(result.started_at.expect("started_at") >= start + Duration::from_secs(10));

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        assert_eq!(
            job_status.next_run.expect("2nd next_run").1,
            first_run_at + Duration::from_secs(10)
        );
    }

    #[tokio::test]
    async fn add_already_existing_error() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");

        let err = test
            .queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job, false)
            .await
            .expect_err("second add_recurring_job");
        assert!(matches!(err, Error::RecurringJobAlreadyExists(_)));
    }

    #[tokio::test]
    async fn update_nonexistent_error() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        let err = test
            .queue
            .update_recurring_job("job_id".to_string(), schedule, job)
            .await
            .expect_err("update_recurring_job");

        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test]
    async fn update_same_schedule() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        assert!(job_status.next_run.is_some());

        tokio::time::pause();
        tokio::time::sleep(Duration::from_secs(2)).await;
        tokio::time::resume();

        test.queue
            .update_recurring_job("job_id".to_string(), schedule, job)
            .await
            .expect("update_recurring_job");
        let new_job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        // The scheduled job and its time to run should be the same.
        assert_eq!(job_status.next_run, new_job_status.next_run);
    }

    #[tokio::test]
    async fn update_payload() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("set-counter")
            .json_payload(&serde_json::json!(1))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        assert!(job_status.next_run.is_some());

        let new_job = JobBuilder::new("set-counter")
            .json_payload(&serde_json::json!(2))
            .expect("json_payload")
            .build();
        test.queue
            .update_recurring_job("job_id".to_string(), schedule, new_job)
            .await
            .expect("update_recurring_job");
        let new_job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let new_next_run = new_job_status.next_run.expect("next_run");

        // The scheduled job and its time to run should be the same.
        let payload: serde_json::Value =
            serde_json::from_slice(&new_job_status.base_job.payload).expect("parsing payload");
        assert_eq!(payload, serde_json::json!(2));

        tokio::time::pause();
        wait_for_job("waiting for job to run", &test.queue, new_next_run.0).await;
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "task should have run with new payload"
        );
    }

    #[tokio::test]
    async fn update_to_earlier() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule, job.clone(), false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let next_run = job_status.next_run.expect("next_run");

        event!(Level::DEBUG, "sleeping");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let update_time = test.time.now().replace_nanosecond(0).unwrap();

        let new_schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(5),
        };
        test.queue
            .update_recurring_job("job_id".to_string(), new_schedule.clone(), job)
            .await
            .expect("update_recurring_job");
        let new_job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        event!(Level::INFO, ?new_job_status);
        let new_next_run = new_job_status.next_run.expect("next_run after update");

        assert_eq!(new_next_run.0, next_run.0);
        assert_eq!(new_next_run.1, update_time + Duration::from_secs(5));
        assert_eq!(new_job_status.schedule, new_schedule);

        tokio::time::pause();
        let job_result = wait_for_job("waiting for job to run", &test.queue, new_next_run.0).await;
        assert_eq!(
            job_result.started_at.expect("started_at"),
            update_time + Duration::from_secs(5)
        );
    }

    #[tokio::test]
    async fn update_to_later() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule, job.clone(), false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let next_run = job_status.next_run.expect("next_run");

        event!(Level::DEBUG, "sleeping");
        tokio::time::sleep(Duration::from_secs(1)).await;
        let update_time = test.time.now().replace_nanosecond(0).unwrap();

        let new_schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(20),
        };
        test.queue
            .update_recurring_job("job_id".to_string(), new_schedule.clone(), job)
            .await
            .expect("update_recurring_job");
        let new_job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let new_next_run = new_job_status.next_run.expect("next_run after update");

        assert_eq!(new_next_run.0, next_run.0);
        assert_eq!(new_next_run.1, update_time + Duration::from_secs(20));
        assert_eq!(new_job_status.schedule, new_schedule);

        tokio::time::pause();
        let job_result = wait_for_job("waiting for job to run", &test.queue, new_next_run.0).await;
        assert_eq!(
            job_result.started_at.expect("started_at"),
            update_time + Duration::from_secs(20)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn upsert_new_job() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let start = test.time.now().replace_nanosecond(0).unwrap();
        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .upsert_recurring_job("job_id".to_string(), schedule.clone(), job, false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        assert!(job_status.last_run.is_none());
        let (first_job_id, first_run_at) = job_status.next_run.expect("next_run_at");

        let first_run_at = first_run_at.replace_nanosecond(0).unwrap();
        assert_eq!(
            first_run_at,
            start + Duration::from_secs(10),
            "first_run_at"
        );
        assert_eq!(job_status.schedule, schedule);

        tokio::time::sleep_until(
            test.time
                .instant_for_timestamp(first_run_at.unix_timestamp()),
        )
        .await;

        let result = wait_for_job("first run", &test.queue, first_job_id).await;
        assert!(result.started_at.expect("started_at") >= start + Duration::from_secs(10));
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");

        let last_run = job_status.last_run.as_ref().expect("last_run");
        assert_eq!(last_run.id, first_job_id);
        assert_eq!(last_run.orig_run_at, first_run_at);

        event!(Level::INFO, ?job_status, "status after first job finished");
        let (second_job_id, second_run_time) = job_status.next_run.expect("next_run");
        assert_ne!(
            first_job_id, second_job_id,
            "second job must have different id from first job"
        );
        assert_eq!(second_run_time, first_run_at + Duration::from_secs(10));

        tokio::time::sleep_until(
            test.time
                .instant_for_timestamp(second_run_time.unix_timestamp()),
        )
        .await;

        let result = wait_for_job("second run", &test.queue, second_job_id).await;
        assert!(result.started_at.expect("started_at") >= second_run_time);

        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[tokio::test]
    async fn upsert_existing_job() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!(1))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };

        test.queue
            .upsert_recurring_job("job_id".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        assert!(job_status.next_run.is_some());

        let new_job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!(2))
            .expect("json_payload")
            .build();
        test.queue
            .upsert_recurring_job("job_id".to_string(), schedule, new_job, false)
            .await
            .expect("update_recurring_job");
        let new_job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let new_next_run = new_job_status.next_run.expect("next_run");

        // The scheduled job and its time to run should be the same.
        let payload: serde_json::Value =
            serde_json::from_slice(&new_job_status.base_job.payload).expect("parsing payload");
        assert_eq!(payload, serde_json::json!(2));

        tokio::time::pause();
        wait_for_job("waiting for job to run", &test.queue, new_next_run.0).await;
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            2,
            "task should have run with new payload"
        );
    }

    #[tokio::test]
    async fn delete() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!({ "value": 1 }))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };

        test.queue
            .add_recurring_job("job_id".to_string(), schedule, job.clone(), false)
            .await
            .expect("add_recurring_job");

        test.queue
            .delete_recurring_job("job_id".to_string())
            .await
            .expect("delete_recurring_job");

        // Wait long enough that the task should have run if it was still there.
        tokio::time::pause();
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "task should not have run"
        );

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect_err("job status fetch should return error");
        assert!(matches!(job_status, Error::NotFound));
    }

    #[tokio::test]
    async fn delete_nonexistent() {
        let test = TestEnvironment::new().await;
        let err = test
            .queue
            .delete_recurring_job("job_id".to_string())
            .await
            .unwrap_err();
        println!("err: {err:?}");
        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test]
    async fn job_status_on_nonexistent_job() {
        let test = TestEnvironment::new().await;
        let err = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .unwrap_err();
        println!("err: {err:?}");
        assert!(matches!(err, Error::NotFound));
    }

    #[tokio::test(start_paused = true)]
    async fn restart() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!(1))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .add_recurring_job("job_id".to_string(), schedule.clone(), job, false)
            .await
            .expect("add_recurring_job");

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let (first_job_id, first_run_at) = job_status.next_run.expect("next_run_at");
        wait_for_job("first run", &test.queue, first_job_id).await;

        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let next_job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let second_run_at = next_job_status.next_run.expect("next_run_at").1;
        let dir = test.queue.close_and_persist().await;
        event!(Level::INFO, "Closed Queue");

        // Wait a while with the queue closed.
        let after_two_runs = second_run_at + Duration::from_secs(21);
        tokio::time::sleep_until(
            test.time
                .instant_for_timestamp(after_two_runs.unix_timestamp()),
        )
        .await;

        // Restart the queue again.
        event!(Level::INFO, "Restarting queue");
        let test = TestEnvironment::from_path(dir).await;
        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            0,
            "counter starts at 0"
        );
        let _worker = test.worker().build().await.expect("Failed to build worker");

        let job_status = test
            .queue
            .get_recurring_job_info("job_id".to_string())
            .await
            .expect("Retrieving job status");
        let (next_job_id, next_run_at) = job_status.next_run.expect("next_run_at");
        event!(Level::INFO, %next_run_at);
        wait_for_job("first run after restart", &test.queue, next_job_id).await;

        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "task should run again after restart"
        );

        // Wait a little more just to make sure that we aren't double-running the task.
        tokio::time::sleep(Duration::from_secs(1)).await;

        assert_eq!(
            test.context
                .counter
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "task should not run again immediately"
        );
    }

    #[tokio::test]
    async fn list_prefix() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!(1))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .add_recurring_job("job_id_3".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        test.queue
            .add_recurring_job("job_id_1".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        test.queue
            .add_recurring_job(
                "other_job".to_string(),
                schedule.clone(),
                job.clone(),
                false,
            )
            .await
            .expect("add_recurring_job");
        test.queue
            .add_recurring_job("job_id_2".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");

        let mut job_ids = test
            .queue
            .list_recurring_jobs_with_prefix("job_id_")
            .await
            .expect("Listing jobs");
        job_ids.sort();
        assert_eq!(job_ids, vec!["job_id_1", "job_id_2", "job_id_3"]);
    }

    #[tokio::test]
    async fn list_prefix_with_no_matches() {
        let test = TestEnvironment::new().await;
        let _worker = test.worker().build().await.expect("Failed to build worker");
        let job = JobBuilder::new("counter")
            .json_payload(&serde_json::json!(1))
            .expect("json_payload")
            .build();

        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(10),
        };
        test.queue
            .add_recurring_job("job_id_3".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        test.queue
            .add_recurring_job("job_id_1".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");
        test.queue
            .add_recurring_job(
                "other_job".to_string(),
                schedule.clone(),
                job.clone(),
                false,
            )
            .await
            .expect("add_recurring_job");
        test.queue
            .add_recurring_job("job_id_2".to_string(), schedule.clone(), job.clone(), false)
            .await
            .expect("add_recurring_job");

        let job_ids = test
            .queue
            .list_recurring_jobs_with_prefix("not_found_jobs")
            .await
            .expect("Listing jobs");
        assert!(job_ids.is_empty());
    }
}
