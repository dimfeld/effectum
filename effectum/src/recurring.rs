use std::{rc::Rc, str::FromStr, time::Duration};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::Span;

use crate::{
    db_writer::{
        recurring::{AddRecurringJobArgs, DeleteRecurringJobArgs},
        DbOperation, UpsertMode,
    },
    shared_state::SharedState,
    Error, Job, JobBuilder, Queue,
};

pub(crate) async fn schedule_needed_recurring_jobs_at_startup(
    queue: &SharedState,
) -> Result<(), Error> {
    let conn = queue.read_conn_pool.get().await?;

    let needed_jobs = conn
        .interact(move |db| {
            let query = r##"SELECT base_job_id
                FROM recurring
                JOIN jobs on recurring.base_job_id = jobs.job_id
                LEFT JOIN active_jobs ON recurring.base_job_id = active_jobs.from_recurring
                WHERE active_jobs.job_id IS NULL"##;
            let mut stmt = db.prepare(query)?;
            let rows = stmt
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<i64>, _>>()?;

            Ok::<_, Error>(rows)
        })
        .await??;
    drop(conn);

    schedule_recurring_jobs(queue, &needed_jobs).await?;

    Ok(())
}

/// Schedule jobs from recurring templates.
pub(crate) async fn schedule_recurring_jobs(queue: &SharedState, ids: &[i64]) -> Result<(), Error> {
    let conn = queue.read_conn_pool.get().await?;
    let ids = ids
        .iter()
        .map(|id| rusqlite::types::Value::from(*id))
        .collect::<Vec<_>>();
    let now = queue.time.now();
    let jobs = conn
        .interact(move |db| {
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

                    let next_job_time = schedule.find_next_job_time(now)?;
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
        })
        .await??;

    queue.add_jobs(jobs).await?;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename = "snake_case")]
pub enum RecurringJobSchedule {
    Cron { spec: String },
}

impl RecurringJobSchedule {
    pub(crate) fn find_next_job_time(
        &self,
        after: OffsetDateTime,
    ) -> Result<OffsetDateTime, Error> {
        match self {
            RecurringJobSchedule::Cron { spec } => {
                let sched = cron::Schedule::from_str(spec).map_err(|_| Error::InvalidSchedule)?;
                let after = chrono::DateTime::<chrono::Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp_opt(after.unix_timestamp(), 0)
                        .ok_or(Error::TimestampOutOfRange("after"))?,
                    chrono::Utc,
                );
                let next = sched.after(&after).next().ok_or(Error::InvalidSchedule)?;
                // The `cron` package uses chrono but everything else here uses `time`, so convert.
                let next = OffsetDateTime::from_unix_timestamp(next.timestamp())
                    .map_err(|_| Error::InvalidSchedule)?;
                Ok(next)
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
    ) -> Result<(), Error> {
        self.do_recurring_job_update(UpsertMode::Add, id, schedule, job)
            .await
    }

    /// Update a recurring job. Returns an error if the job does not exist.
    pub async fn update_recurring_job(
        &self,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
    ) -> Result<(), Error> {
        self.do_recurring_job_update(UpsertMode::Update, id, schedule, job)
            .await
    }

    /// Add a new recurring job, or update an existing one.
    pub async fn upsert_recurring_job(
        &self,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
    ) -> Result<(), Error> {
        self.do_recurring_job_update(UpsertMode::Upsert, id, schedule, job)
            .await
    }

    async fn do_recurring_job_update(
        &self,
        upsert_mode: UpsertMode,
        id: String,
        schedule: RecurringJobSchedule,
        job: Job,
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
                    },
                ),
                span: Span::current(),
            })
            .await
            .map_err(|_| Error::QueueClosed)?;
        let add_result = result_rx.await.map_err(|_| Error::QueueClosed)??;
        if let Some(run_at) = add_result.new_run_at {
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
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore]
    async fn simple_recurring() {}

    #[tokio::test]
    #[ignore]
    async fn bad_schedule() {}

    #[tokio::test]
    #[ignore]
    async fn update_to_bad_schedule() {}

    #[tokio::test]
    #[ignore]
    async fn update_nonexisting_error() {}

    #[tokio::test]
    #[ignore]
    async fn add_already_existing_error() {}

    #[tokio::test]
    #[ignore]
    async fn update_recurring_same_schedule() {}

    #[tokio::test]
    #[ignore]
    async fn update_recurring_to_earlier() {}

    #[tokio::test]
    #[ignore]
    async fn update_recurring_to_later() {}

    #[tokio::test]
    #[ignore]
    async fn upsert_recurring() {}

    #[tokio::test]
    #[ignore]
    async fn delete_recurring() {}
}
