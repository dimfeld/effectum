use rusqlite::{named_params, Connection, Statement};
use time::OffsetDateTime;
use tokio::sync::oneshot;
use uuid::Uuid;

use super::DbOperationResult;
use crate::{Job, JobState, Result};

pub(crate) struct AddJobArgs {
    pub job: Job,
    pub now: OffsetDateTime,
    pub result_tx: oneshot::Sender<Result<Uuid>>,
}

pub(crate) struct AddMultipleJobsResult {
    pub ids: Vec<Uuid>,
}

pub(crate) struct AddMultipleJobsArgs {
    pub jobs: Vec<Job>,
    pub now: OffsetDateTime,
    pub result_tx: oneshot::Sender<Result<AddMultipleJobsResult>>,
}

pub(super) const INSERT_JOBS_QUERY: &str = r##"
    INSERT INTO jobs
    (external_id, job_type, name, status, priority, weight, from_base_job, orig_run_at, payload,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info)
    VALUES
    ($external_id, $job_type, $name, $status, $priority, $weight, $from_base_job, $run_at, $payload,
        $max_retries, $backoff_multiplier, $backoff_randomization, $backoff_initial_interval,
        $added_at, $default_timeout, $heartbeat_increment, '[]')
"##;

pub(super) const INSERT_ACTIVE_JOBS_QUERY: &str = r##"
    INSERT INTO active_jobs
    (job_id,  priority, run_at)
    VALUES
    ($job_id, $priority, $run_at)
"##;

pub(super) fn execute_add_job_stmt(
    tx: &Connection,
    jobs_stmt: &mut Statement,
    job_config: &Job,
    now: OffsetDateTime,
    status: Option<JobState>,
) -> Result<(i64, Uuid)> {
    let run_time = job_config.run_at.unwrap_or(now).unix_timestamp();

    jobs_stmt.execute(named_params! {
        "$external_id": &job_config.id,
        "$job_type": job_config.job_type,
        "$name": job_config.name,
        "$priority": job_config.priority,
        "$weight": job_config.weight,
        "$from_base_job": job_config.from_recurring,
        "$status": status.unwrap_or(JobState::Pending).as_str(),
        "$run_at": run_time,
        "$payload": job_config.payload.as_slice(),
        "$max_retries": job_config.retries.max_retries,
        "$backoff_multiplier": job_config.retries.backoff_multiplier,
        "$backoff_randomization": job_config.retries.backoff_randomization,
        "$backoff_initial_interval": job_config.retries.backoff_initial_interval.as_secs(),
        "$default_timeout" :job_config.timeout.as_secs(),
        "$heartbeat_increment": job_config.heartbeat_increment.as_secs(),
        "$added_at": now.unix_timestamp(),
    })?;

    let job_id = tx.last_insert_rowid();

    Ok((job_id, job_config.id))
}

pub(super) fn execute_add_active_job_stmt(
    active_jobs_stmt: &mut Statement,
    job_id: i64,
    job_config: &Job,
    now: OffsetDateTime,
) -> Result<()> {
    let run_time = job_config.run_at.unwrap_or(now).unix_timestamp();
    active_jobs_stmt.execute(named_params! {
        "$job_id": job_id,
        "$priority": job_config.priority,
        "$run_at": run_time,
    })?;

    Ok(())
}

fn do_add_job(tx: &Connection, job_config: &Job, now: OffsetDateTime) -> Result<Uuid> {
    let mut jobs_stmt = tx.prepare_cached(INSERT_JOBS_QUERY)?;
    let mut active_jobs_stmt = tx.prepare_cached(INSERT_ACTIVE_JOBS_QUERY)?;

    let (job_id, external_id) = execute_add_job_stmt(tx, &mut jobs_stmt, job_config, now, None)?;

    execute_add_active_job_stmt(&mut active_jobs_stmt, job_id, job_config, now)?;

    Ok(external_id)
}

pub(super) fn add_job(tx: &Connection, args: AddJobArgs) -> DbOperationResult {
    let AddJobArgs {
        job,
        now,
        result_tx,
    } = args;

    let result = do_add_job(tx, &job, now);
    DbOperationResult::AddJob(super::OperationResult { result, result_tx })
}

fn do_add_jobs(
    tx: &Connection,
    jobs: Vec<Job>,
    now: OffsetDateTime,
) -> Result<AddMultipleJobsResult> {
    let mut ids = Vec::with_capacity(jobs.len());

    let mut jobs_stmt = tx.prepare_cached(INSERT_JOBS_QUERY)?;
    let mut active_jobs_stmt = tx.prepare_cached(INSERT_ACTIVE_JOBS_QUERY)?;

    for job_config in jobs {
        let (internal, external) =
            execute_add_job_stmt(tx, &mut jobs_stmt, &job_config, now, None)?;
        execute_add_active_job_stmt(&mut active_jobs_stmt, internal, &job_config, now)?;

        ids.push(external);
    }

    Ok(AddMultipleJobsResult { ids })
}

pub(super) fn add_jobs(tx: &Connection, args: AddMultipleJobsArgs) -> DbOperationResult {
    let AddMultipleJobsArgs {
        jobs,
        now,
        result_tx,
    } = args;

    let result = do_add_jobs(tx, jobs, now);
    DbOperationResult::AddMultipleJobs(super::OperationResult { result, result_tx })
}
