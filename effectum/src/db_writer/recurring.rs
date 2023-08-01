use std::rc::Rc;

use rusqlite::{params, Connection, OptionalExtension};
use time::OffsetDateTime;
use tokio::sync::oneshot;

use super::{
    add_job::{execute_add_active_job_stmt, INSERT_ACTIVE_JOBS_QUERY},
    DbOperationResult, UpsertMode,
};
use crate::{
    db_writer::add_job::{execute_add_job_stmt, INSERT_JOBS_QUERY},
    recurring::RecurringJobSchedule,
    Error, Job, Result,
};

pub(crate) struct DeleteRecurringJobArgs {
    pub id: String,
    pub result_tx: oneshot::Sender<Result<()>>,
}

pub(crate) struct AddRecurringJobArgs {
    pub external_id: String,
    pub now: OffsetDateTime,
    pub schedule: RecurringJobSchedule,
    pub upsert_mode: UpsertMode,
    pub job: Job,
    pub result_tx: oneshot::Sender<Result<AddRecurringJobResult>>,
}

pub(crate) struct AddRecurringJobResult {
    pub recurring_job_id: i64,
    pub base_job_id: i64,
    pub new_run_at: Option<OffsetDateTime>,
}

pub(super) fn add_recurring_job(tx: &Connection, args: AddRecurringJobArgs) -> DbOperationResult {
    let AddRecurringJobArgs {
        external_id,
        now,
        schedule,
        upsert_mode,
        job,
        result_tx,
    } = args;
    let result = do_add_recurring_job(tx, external_id, now, schedule, upsert_mode, job);
    DbOperationResult::AddRecurringJob(super::OperationResult { result, result_tx })
}

fn do_add_recurring_job(
    tx: &Connection,
    external_id: String,
    now: OffsetDateTime,
    schedule: RecurringJobSchedule,
    upsert_mode: UpsertMode,
    job: Job,
) -> Result<AddRecurringJobResult> {
    // First get some basic info about the job, if it already exists.
    let mut existing_job_stmt = tx.prepare_cached(
        "SELECT recurring_job_id, base_job_id, schedule FROM recurring WHERE external_id = ?",
    )?;
    let recurring_job: Option<(i64, i64, RecurringJobSchedule)> = existing_job_stmt
        .query_and_then([&external_id], |row| {
            Ok::<_, Error>((
                row.get(0)
                    .map_err(|e| Error::ColumnType(e, "recurring_job_id"))?,
                row.get(1)
                    .map_err(|e| Error::ColumnType(e, "base_job_id"))?,
                row.get_ref(2)?
                    .as_str()
                    .map_err(|e| Error::ColumnType(e.into(), "schedule"))
                    .and_then(|s| {
                        serde_json::from_str::<RecurringJobSchedule>(s)
                            .map_err(|_| Error::InvalidSchedule)
                    })?,
            ))
        })?
        .next()
        .transpose()?;

    match (upsert_mode, recurring_job) {
        (
            UpsertMode::Upsert | UpsertMode::Update,
            Some((recurring_job_id, base_job_id, old_schedule)),
        ) => update_existing_recurring_job(
            tx,
            recurring_job_id,
            base_job_id,
            now,
            old_schedule,
            schedule,
            job,
        ),
        (UpsertMode::Upsert | UpsertMode::Add, None) => {
            add_new_recurring_job(tx, external_id, now, schedule, job)
        }
        (UpsertMode::Add, Some(_)) => Err(Error::RecurringJobAlreadyExists(external_id)),
        (UpsertMode::Update, None) => Err(Error::NotFound),
    }
}

fn add_new_recurring_job(
    tx: &Connection,
    external_id: String,
    now: OffsetDateTime,
    schedule: RecurringJobSchedule,
    mut job: Job,
) -> Result<AddRecurringJobResult> {
    // Insert the base job
    let mut insert_job_stmt = tx.prepare_cached(INSERT_JOBS_QUERY)?;
    let (base_job_id, _) =
        execute_add_job_stmt(tx, &mut insert_job_stmt, &job, now, Some("recurring_base"))?;

    // Then add the recurring template
    let schedule_str = serde_json::to_string(&schedule).map_err(|_| Error::InvalidSchedule)?;
    let mut add_recurring_stmt = tx.prepare_cached(
        r##"INSERT INTO recurring
            (external_id, base_job_id, schedule)
            VALUES
            (?1, ?2, ?3)"##,
    )?;
    add_recurring_stmt.execute(params![external_id, base_job_id, schedule_str])?;

    let recurring_id = tx.last_insert_rowid();
    let run_at = schedule.find_next_job_time(now)?;

    // Finally, add the version of the job that will actually run the first time.
    job.from_recurring = Some(recurring_id);
    job.run_at = Some(run_at);

    let (job_id, _) = execute_add_job_stmt(tx, &mut insert_job_stmt, &job, now, None)?;
    let mut active_insert_stmt = tx.prepare_cached(INSERT_ACTIVE_JOBS_QUERY)?;
    execute_add_active_job_stmt(&mut active_insert_stmt, job_id, &job, now)?;

    Ok(AddRecurringJobResult {
        recurring_job_id: recurring_id,
        base_job_id,
        new_run_at: Some(run_at),
    })
}

fn update_existing_recurring_job(
    tx: &Connection,
    recurring_job_id: i64,
    base_job_id: i64,
    now: OffsetDateTime,
    old_schedule: RecurringJobSchedule,
    new_schedule: RecurringJobSchedule,
    job: Job,
) -> Result<AddRecurringJobResult> {
    // Update the recurring template

    let next_time = if new_schedule != old_schedule {
        let schedule = serde_json::to_string(&new_schedule).map_err(|_| Error::InvalidSchedule)?;
        let mut recurring_job_stmt = tx.prepare_cached(
            r##"UPDATE recurring SET
            schedule = ?1 WHERE recurring_job_id = ?2"##,
        )?;
        recurring_job_stmt.execute(params![schedule, recurring_job_id])?;
        Some(new_schedule.find_next_job_time(now)?)
    } else {
        // No new time since the schedule did not change. We have to be careful to not reset the
        // next job time if the schedule did not change, since we could inadvertently skip a job if
        // we are doing this update at the moment that the job is just about to run.
        None
    };

    // Update the base job
    let mut base_update_stmt = tx.prepare_cached(
        r##"UPDATE jobs
        SET
            job_type = ?2,
            priority = ?3,
            weight = ?4,
            payload = ?5,
            max_retries = ?6,
            backoff_multiplier = ?7,
            backoff_randomization = ?8,
            backoff_initial_interval = ?9,
            default_timeout = ?10,
            heartbeat_increment = ?11
        WHERE job_id=?1"##,
    )?;
    base_update_stmt.execute(params![
        base_job_id,
        job.job_type,
        job.priority,
        job.weight,
        job.payload,
        job.retries.max_retries,
        job.retries.backoff_multiplier,
        job.retries.backoff_randomization,
        job.retries.backoff_initial_interval.as_secs(),
        job.timeout.as_secs(),
        job.heartbeat_increment.as_secs(),
    ])?;

    // Update any pending jobs
    let mut pending_job_update_stmt = tx.prepare_cached(
        r##"UPDATE jobs
        SET
            orig_run_at = COALESCE(?, orig_run_at),
            job_type = ?,
            priority = ?,
            weight = ?,
            payload = ?,
            max_retries = ?,
            backoff_multiplier = ?,
            backoff_randomization = ?,
            backoff_initial_interval = ?,
            default_timeout = ?,
            heartbeat_increment = ?
        WHERE from_recurring = ? AND status = 'pending'
        RETURNING job_id"##,
    )?;
    let updated_jobs = pending_job_update_stmt
        .query_map(
            params![
                next_time,
                job.job_type,
                job.priority,
                job.weight,
                job.payload,
                job.retries.max_retries,
                job.retries.backoff_multiplier,
                job.retries.backoff_randomization,
                job.retries.backoff_initial_interval.as_secs(),
                job.timeout.as_secs(),
                job.heartbeat_increment.as_secs(),
                recurring_job_id,
            ],
            |row| row.get::<_, rusqlite::types::Value>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;

    // Update the active job entry for any pending jobs
    if !updated_jobs.is_empty() {
        let mut active_job_update_stmt = tx.prepare_cached(
            r##"UPDATE active_jobs
            SET
                priority = ?,
                run_at = COALESCE(?, run_at)
            WHERE job_id in rarray(?) AND active_worker_id IS NULL"##,
        )?;

        active_job_update_stmt.execute(params![job.priority, next_time, Rc::new(updated_jobs)])?;
    }

    Ok(AddRecurringJobResult {
        recurring_job_id,
        base_job_id,
        new_run_at: next_time,
    })
}

pub(super) fn delete_recurring_job(
    tx: &Connection,
    args: DeleteRecurringJobArgs,
) -> DbOperationResult {
    let DeleteRecurringJobArgs { id, result_tx } = args;
    let result = do_delete_recurring_job(tx, id);
    DbOperationResult::DeleteRecurringJob(super::OperationResult { result, result_tx })
}

fn do_delete_recurring_job(tx: &Connection, id: String) -> Result<()> {
    let mut delete_recurring_job_stmt =
        tx.prepare_cached("DELETE FROM recurring WHERE external_id = ? RETURNING base_job_id")?;

    let base_job_id = delete_recurring_job_stmt
        .query_row([id], |row| row.get::<_, i64>(0))
        .optional()?
        .ok_or(Error::NotFound)?;

    // Remove all the pending jobs that referenced this one.
    let mut remove_jobs_stmt = tx.prepare_cached(
        "DELETE FROM jobs WHERE from_recurring = ? AND status = 'pending' RETURNING job_id",
    )?;
    let job_ids = remove_jobs_stmt
        .query_map([base_job_id], |row| row.get::<_, rusqlite::types::Value>(0))?
        .collect::<Result<Vec<_>, _>>()?;

    if !job_ids.is_empty() {
        // Remove the corresponding pending jobs from the active_jobs table.
        let mut remove_active_jobs_stmt = tx.prepare_cached(
            "DELETE FROM active_jobs WHERE active_worker_id IS NULL AND job_id IN rarray(?)",
        )?;
        remove_active_jobs_stmt.execute([Rc::new(job_ids)])?;
    }

    // Finally remove the job template
    let mut remove_base_jobs_stmt = tx.prepare_cached("DELETE FROM jobs WHERE job_id = ?")?;
    remove_base_jobs_stmt.execute([base_job_id])?;
    Ok(())
}
