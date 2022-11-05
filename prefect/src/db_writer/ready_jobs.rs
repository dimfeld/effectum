use std::{
    rc::Rc,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};

use rusqlite::{named_params, types::Value, Connection, Transaction};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{event, Level};
use uuid::Uuid;

use crate::{shared_state::SharedState, worker::RunningJobs, Job, JobData, Result};

pub(crate) struct ReadyJob {
    pub job: Job,
    pub done_rx: tokio::sync::watch::Receiver<bool>,
}

pub(crate) struct GetReadyJobsArgs {
    pub job_types: Vec<Value>,
    pub max_jobs: u32,
    pub max_concurrency: u32,
    pub running_jobs: Arc<RunningJobs>,
    pub now: OffsetDateTime,
    pub result_tx: tokio::sync::oneshot::Sender<Result<Vec<ReadyJob>>>,
}

fn do_get_ready_jobs(
    tx: &Connection,
    queue: &SharedState,
    worker_id: u64,
    job_types: Vec<Value>,
    max_jobs: u32,
    max_concurrency: u32,
    running_jobs: Arc<RunningJobs>,
    now: OffsetDateTime,
) -> Result<Vec<ReadyJob>> {
    let mut stmt = tx.prepare_cached(
        r##"SELECT job_id, external_id, active_jobs.priority, weight,
                job_type, current_try,
                COALESCE(checkpointed_payload, payload) as payload,
                default_timeout,
                heartbeat_increment,
                backoff_multiplier,
                backoff_randomization,
                backoff_initial_interval,
                max_retries
            FROM active_jobs
            JOIN jobs USING(job_id)
            WHERE active_worker_id IS NULL
                AND run_at <= $now
                AND job_type in rarray($job_types)
                AND weight <= $max_concurrency
            ORDER BY active_jobs.priority DESC, run_at
            LIMIT $limit"##,
    )?;

    #[derive(Debug)]
    struct JobResult {
        job_id: i64,
        external_id: Uuid,
        priority: i32,
        weight: u16,
        job_type: String,
        current_try: i32,
        payload: Option<Vec<u8>>,
        default_timeout: i32,
        heartbeat_increment: i32,
        backoff_multiplier: f64,
        backoff_randomization: f64,
        backoff_initial_interval: i32,
        max_retries: i32,
    }

    let now_timestamp = now.unix_timestamp();
    let jobs = stmt.query_map(
        named_params! {
            "$job_types": Rc::new(job_types),
            "$now": now_timestamp,
            "$max_concurrency": max_concurrency,
            "$limit": max_jobs,
        },
        |row| {
            let job_id: i64 = row.get(0)?;
            let external_id: Uuid = row.get(1)?;
            let priority: i32 = row.get(2)?;
            let weight: u16 = row.get(3)?;
            let job_type: String = row.get(4)?;
            let current_try: i32 = row.get(5)?;
            let payload: Option<Vec<u8>> = row.get(6)?;
            let default_timeout: i32 = row.get(7)?;
            let heartbeat_increment: i32 = row.get(8)?;
            let backoff_multiplier: f64 = row.get(9)?;
            let backoff_randomization: f64 = row.get(10)?;
            let backoff_initial_interval: i32 = row.get(11)?;
            let max_retries: i32 = row.get(12)?;

            Ok(JobResult {
                job_id,
                priority,
                weight,
                job_type,
                current_try,
                payload,
                default_timeout,
                external_id,
                heartbeat_increment,
                backoff_multiplier,
                backoff_randomization,
                backoff_initial_interval,
                max_retries,
            })
        },
    )?;

    let mut set_running = tx.prepare_cached(
        r##"UPDATE active_jobs
            SET active_worker_id=$worker_id, started_at=$now, expires_at=$expiration
            WHERE job_id=$job_id"##,
    )?;

    let mut ready_jobs = Vec::with_capacity(max_jobs as usize);
    let mut running_count = running_jobs.current_weighted.load(Ordering::Relaxed);
    for job in jobs {
        let job = job?;
        let weight = job.weight as u32;

        event!(Level::DEBUG, running_count, weight, max_concurrency);

        if running_count + weight > max_concurrency {
            break;
        }

        let expiration = now_timestamp + job.default_timeout as i64;

        set_running.execute(named_params! {
            "$job_id": job.job_id,
            "$worker_id": worker_id,
            "$now": now_timestamp,
            "$expiration": expiration
        })?;

        running_count = running_jobs
            .current_weighted
            .fetch_add(weight, Ordering::Relaxed)
            + weight;
        running_jobs.started.fetch_add(1, Ordering::Relaxed);

        let (done_tx, done_rx) = tokio::sync::watch::channel(false);
        let job = Job(Arc::new(JobData {
            id: job.external_id,
            job_id: job.job_id,
            worker_id,
            heartbeat_increment: job.heartbeat_increment,
            job_type: job.job_type,
            payload: job.payload.unwrap_or_default(),
            priority: job.priority,
            weight: job.weight,
            start_time: now,
            current_try: job.current_try,
            backoff_multiplier: job.backoff_multiplier,
            backoff_randomization: job.backoff_randomization,
            backoff_initial_interval: job.backoff_initial_interval,
            max_retries: job.max_retries,
            done: Mutex::new(Some(done_tx)),
            queue: queue.clone(),
            expires: AtomicI64::new(expiration),
        }));

        ready_jobs.push(ReadyJob { job, done_rx });
    }

    Ok(ready_jobs)
}

pub(crate) fn get_ready_jobs(
    tx: &Connection,
    queue: &SharedState,
    worker_id: u64,
    args: GetReadyJobsArgs,
) -> bool {
    let GetReadyJobsArgs {
        job_types,
        max_jobs,
        max_concurrency,
        running_jobs,
        now,
        result_tx,
    } = args;

    let result = do_get_ready_jobs(
        tx,
        queue,
        worker_id,
        job_types,
        max_jobs,
        max_concurrency,
        running_jobs,
        now,
    );

    let worked = result.is_ok();
    result_tx.send(result).ok();
    worked
}
