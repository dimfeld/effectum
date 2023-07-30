use std::{rc::Rc, time::Duration};

use rusqlite::params;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{shared_state::SharedState, Error, Job, JobBuilder};

pub(crate) async fn schedule_needed_recurring_jobs_at_start(
    queue: &SharedState,
) -> Result<(), Error> {
    let conn = queue.read_conn_pool.get().await?;

    let needed_jobs = conn
        .interact(move |db| {
            let query = r##"SELECT base_job_id
                FROM recurring
                JOIN jobs on jobs.job_id = recurring.base_job_id
                LEFT JOIN active_jobs ON recurring.id = active_jobs.from_recurring
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
                    let priority = row
                        .get(2)
                        .map_err(|e| Error::ColumnType(e.into(), "priority"))?;
                    let weight = row
                        .get(3)
                        .map_err(|e| Error::ColumnType(e.into(), "weight"))?;
                    let payload = row
                        .get(4)
                        .map_err(|e| Error::ColumnType(e.into(), "payload"))?;
                    let max_retries = row
                        .get(5)
                        .map_err(|e| Error::ColumnType(e.into(), "max_retries"))?;
                    let backoff_multiplier = row
                        .get(6)
                        .map_err(|e| Error::ColumnType(e.into(), "backoff_multiplier"))?;
                    let backoff_randomization = row
                        .get(7)
                        .map_err(|e| Error::ColumnType(e.into(), "backoff_randomization"))?;
                    let backoff_initial_interval = row
                        .get(8)
                        .map_err(|e| Error::ColumnType(e.into(), "backoff_initial_interval"))?;
                    let default_timeout = row
                        .get(9)
                        .map_err(|e| Error::ColumnType(e.into(), "default_timeout"))?;
                    let heartbeat_increment = row
                        .get(10)
                        .map_err(|e| Error::ColumnType(e.into(), "heartbeat_increment"))?;
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
                .into_iter()
                .collect::<Result<Vec<Job>, Error>>()?;

            Ok::<_, Error>(rows)
        })
        .await??;

    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename = "snake_case")]
pub enum RecurringJobSchedule {
    Cron { spec: String },
}

pub(crate) fn find_next_job_time(schedule: &RecurringJobSchedule) -> Result<OffsetDateTime, Error> {
    todo!()
}
