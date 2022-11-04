use ahash::{HashMap, HashSet};
use rusqlite::named_params;
use rusqlite::{Statement, Transaction};
use time::OffsetDateTime;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::shared_state::SharedState;
use crate::NewJob;
use crate::Result;

pub(crate) struct AddJobArgs {
    pub job: NewJob,
    pub now: OffsetDateTime,
    pub result_tx: oneshot::Sender<Result<(i64, Uuid)>>,
}

pub(crate) struct AddMultipleJobsResult {
    pub ids: Vec<(i64, Uuid)>,
}

pub(crate) struct AddMultipleJobsArgs {
    pub jobs: Vec<NewJob>,
    pub now: OffsetDateTime,
    pub result_tx: oneshot::Sender<Result<AddMultipleJobsResult>>,
}

const INSERT_JOBS_QUERY: &str = r##"
    INSERT INTO jobs
    (external_id, job_type, status, priority, weight, from_recurring_job, orig_run_at, payload,
        max_retries, backoff_multiplier, backoff_randomization, backoff_initial_interval,
        added_at, default_timeout, heartbeat_increment, run_info)
    VALUES
    ($external_id, $job_type, 'active', $priority, $weight, $from_recurring_job, $run_at, $payload,
        $max_retries, $backoff_multiplier, $backoff_randomization, $backoff_initial_interval,
        $added_at, $default_timeout, $heartbeat_increment, '[]')
"##;

const INSERT_ACTIVE_JOBS_QUERY: &str = r##"
    INSERT INTO active_jobs
    (job_id,  priority, run_at)
    VALUES
    ($job_id, $priority, $run_at)
"##;

fn execute_add_job_stmt(
    tx: &Transaction,
    jobs_stmt: &mut Statement,
    active_jobs_stmt: &mut Statement,
    job_config: &NewJob,
    now: OffsetDateTime,
    from_recurring_job: Option<i64>,
) -> Result<(i64, Uuid)> {
    let external_id: Uuid = ulid::Ulid::new().into();
    let run_time = job_config.run_at.unwrap_or(now).unix_timestamp();

    jobs_stmt.execute(named_params! {
        "$external_id": &external_id,
        "$job_type": job_config.job_type,
        "$priority": job_config.priority,
        "$weight": job_config.weight,
        "$from_recurring_job": from_recurring_job,
        "$run_at": run_time,
        "$payload": job_config.payload.as_slice(),
        "$max_retries": job_config.retries.max_retries,
        "$backoff_multiplier": job_config.retries.backoff_multiplier,
        "$backoff_randomization": job_config.retries.backoff_randomization,
        "$backoff_initial_interval": job_config.retries.backoff_initial_interval.whole_seconds(),
        "$default_timeout" :job_config.timeout.whole_seconds(),
        "$heartbeat_increment": job_config.heartbeat_increment.whole_seconds(),
        "$added_at": now.unix_timestamp(),
    })?;

    let job_id = tx.last_insert_rowid();

    active_jobs_stmt.execute(named_params! {
        "$job_id": job_id,
        "$priority": job_config.priority,
        "$run_at": run_time,
    })?;

    Ok((job_id, external_id))
}

fn do_add_job(
    tx: &mut Transaction,
    job_config: &NewJob,
    now: OffsetDateTime,
) -> Result<(i64, Uuid)> {
    let mut jobs_stmt = tx.prepare_cached(INSERT_JOBS_QUERY)?;
    let mut active_jobs_stmt = tx.prepare_cached(INSERT_ACTIVE_JOBS_QUERY)?;

    execute_add_job_stmt(
        tx,
        &mut jobs_stmt,
        &mut active_jobs_stmt,
        job_config,
        now,
        None,
    )
}

pub(crate) fn add_job(tx: &mut Transaction, args: AddJobArgs) {
    let AddJobArgs {
        job,
        now,
        result_tx,
    } = args;

    let result = do_add_job(tx, &job, now);
    result_tx.send(result).ok();
}

fn do_add_jobs(
    tx: &mut Transaction,
    jobs: Vec<NewJob>,
    now: OffsetDateTime,
) -> Result<AddMultipleJobsResult> {
    let mut ids = Vec::with_capacity(jobs.len());

    let mut jobs_stmt = tx.prepare_cached(INSERT_JOBS_QUERY)?;
    let mut active_jobs_stmt = tx.prepare_cached(INSERT_ACTIVE_JOBS_QUERY)?;

    for job_config in jobs {
        let job_ids = execute_add_job_stmt(
            tx,
            &mut jobs_stmt,
            &mut active_jobs_stmt,
            &job_config,
            now,
            None,
        )?;

        ids.push(job_ids);
    }

    Ok(AddMultipleJobsResult { ids })
}

pub(crate) fn add_jobs(tx: &mut Transaction, args: AddMultipleJobsArgs) {
    let AddMultipleJobsArgs {
        jobs,
        now,
        result_tx,
    } = args;

    let result = do_add_jobs(tx, jobs, now);
    result_tx.send(result).ok();
}
