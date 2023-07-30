use std::{rc::Rc, str::FromStr, time::Duration};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::{shared_state::SharedState, Error, Job, JobBuilder, Queue};

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
    let jobs = conn
        .interact(move |db| {
            let query = r##"SELECT job_id,
                job_type, priority, weight, payload, max_retries,
                backoff_multiplier, backoff_randomization, backoff_initial_interval,
                default_timeout, heartbeat_increment, schedule
            FROM jobs
            JOIN recurring ON job_id = base_job_id
            WHERE status = 'recurring_base' AND job_id IN rarray($1)
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

                    let next_job_time = find_next_job_time(&schedule)?;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "snake_case")]
pub enum RecurringJobSchedule {
    Cron { spec: String },
}

pub(crate) fn find_next_job_time(schedule: &RecurringJobSchedule) -> Result<OffsetDateTime, Error> {
    match schedule {
        RecurringJobSchedule::Cron { spec } => {
            let sched = cron::Schedule::from_str(spec).map_err(|_| Error::InvalidSchedule)?;
            let next = sched
                .upcoming(chrono::Utc)
                .next()
                .ok_or(Error::InvalidSchedule)?;
            // The `cron` package uses chrono but everything else here uses `time`, so convert.
            let next = OffsetDateTime::from_unix_timestamp(next.timestamp())
                .map_err(|_| Error::InvalidSchedule)?;
            Ok(next)
        }
    }
}

impl Queue {
    /// Add a new recurring job to the queue, returning an error if a job with this external ID
    /// already exists.
    pub async fn add_recurring_job(
        &self,
        id: &str,
        schedule: RecurringJobSchedule,
        job: Job,
    ) -> Result<Uuid, Error> {
        // Add the job and the recurring job info.
        // Schedule the recurring job.
        todo!();
    }

    /// Update a recurring job. Returns an error if the job does not exist.
    pub async fn update_recurring_job(
        &self,
        id: &str,
        schedule: RecurringJobSchedule,
        job: Job,
    ) -> Result<(), Error> {
        // Update the base recurring job
        // Update any pending scheduled jobs for this job.
        todo!()
    }

    /// Add a new recurring job, or update an existing one.
    pub async fn upsert_recurring_job(
        &self,
        id: &str,
        schedule: RecurringJobSchedule,
        job: Job,
    ) -> Result<(), Error> {
        // Upsert the base recurring job
        // If newly added, schedule the job. Otherwise update any pending scheduled jobs for this job.
        todo!()
    }

    /// Remove a recurring job and unschedule any scheduled jobs for it.
    pub async fn delete_recurring_job(&self, id: &str) -> Result<(), Error> {
        // Delete the base recurring job
        // Cancel any pending scheduled jobs for this job.
        todo!()
    }
}
