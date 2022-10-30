use std::rc::Rc;

use ahash::HashMap;
use rusqlite::params;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{event, instrument, Level};

use crate::error::Result;
use crate::shared_state::SharedState;
use crate::{Error, SmartString};

pub(crate) type ScheduledJobType = (SmartString, i64);

#[instrument(skip_all)]
pub(crate) async fn monitor_pending_jobs(
    queue: SharedState,
    pending_job_rx: mpsc::Receiver<ScheduledJobType>,
) -> Result<JoinHandle<()>> {
    // get the initial list of pending jobs so we can start for them
    let now = queue.time.now().unix_timestamp();
    let initial_pending = queue
        .write_db(move |db| {
            let mut stmt = db.prepare(
                r##"
            SELECT job_type, MIN(run_at) as run_at
            FROM active_jobs
            WHERE run_at > $1 AND active_worker_id IS NULL
            GROUP BY job_type
            "##,
            )?;

            let rows = stmt
                .query_map(params![now], |row| {
                    let name = SmartString::from(row.get_ref(0)?.as_str()?);
                    Ok((name, row.get(1)?))
                })?
                .into_iter()
                .collect::<Result<Vec<ScheduledJobType>, _>>()?;
            Ok(rows)
        })
        .await?;

    let next_times = HashMap::from_iter(initial_pending.into_iter());

    Ok(tokio::spawn(pending_jobs_task(
        queue,
        pending_job_rx,
        next_times,
    )))
}

#[instrument(level = "debug", skip(queue), fields(next_times))]
async fn get_next_times(
    queue: &SharedState,
    now: i64,
    job_types: Option<Vec<SmartString>>,
) -> Result<Vec<ScheduledJobType>> {
    let conn = queue.read_conn_pool.get().await?;

    let job_types = job_types
        .unwrap_or_default()
        .into_iter()
        .map(|s| rusqlite::types::Value::from(String::from(s)))
        .collect::<Vec<_>>();

    let next_times = conn
        .interact(move |db| {
            let query = if job_types.is_empty() {
                r##"
            SELECT job_type, MIN(run_at) as run_at
            FROM active_jobs
            WHERE run_at > $1 AND active_worker_id IS NULL
            GROUP BY job_type
            "##
            } else {
                r##"
            SELECT job_type, MIN(run_at) as run_at
            FROM active_jobs
            WHERE run_at > $1 AND active_worker_id IS NULL AND job_type IN rarray($2)
            GROUP BY job_type
            "##
            };

            let mut stmt = db.prepare_cached(query)?;

            let rows = stmt
                .query_map(params![now, Rc::new(job_types)], |row| {
                    let name = SmartString::from(row.get_ref(0)?.as_str()?);
                    Ok((name, row.get(1)?))
                })?
                .into_iter()
                .collect::<Result<Vec<ScheduledJobType>, _>>()?;
            Ok::<_, Error>(rows)
        })
        .await??;

    tracing::Span::current().record("next_times", tracing::field::debug(&next_times));
    event!(Level::DEBUG, ?next_times);
    Ok(next_times)
}

async fn pending_jobs_task(
    queue: SharedState,
    mut pending_job_rx: mpsc::Receiver<ScheduledJobType>,
    mut next_times: HashMap<SmartString, i64>,
) {
    let mut global_close_rx = queue.close.clone();
    loop {
        let next_time = next_times.values().copied().min().unwrap_or(0);

        event!(Level::TRACE, %next_time, "Waiting for pending job");

        tokio::select! {
            _ = tokio::time::sleep_until(queue.time.instant_for_timestamp(next_time)), if next_time > 0 =>{
                let now = queue.time.now().unix_timestamp();
                let workers = queue.workers.read().await;

                let job_types = next_times
                    .iter()
                    .filter(|(_, &run_at)| run_at <= now)
                    .map(|(job_type, _)| job_type.clone())
                    .collect::<Vec<_>>();

                for job_type in &job_types {
                    event!(Level::DEBUG, %job_type, "Notifying pending jobs");
                    next_times.remove(job_type);
                    workers.new_job_available(job_type.as_str());
                }

                let new_next_times = get_next_times(&queue, now, Some(job_types)).await;
                match new_next_times {
                    Ok(new_next_times) => {
                        next_times.extend(new_next_times);
                    }
                    Err(e) => event!(Level::ERROR, err = %e, "Failed to fetch new times")
                }
            }
            Some((job_type, run_at)) = pending_job_rx.recv() => {
                event!(Level::DEBUG, %job_type, %run_at, "Got pending job");
                next_times.entry(job_type)
                    .and_modify(|ts| *ts = std::cmp::min(run_at, *ts))
                    .or_insert(run_at);
            }
            _ = global_close_rx.changed() => {
                break;
            }
        }
    }
}
