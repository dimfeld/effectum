//! This is a simple example of using the queue to schedule a number of jobs that use a JSON
//! payload and access the context.

use std::{
    path::Path,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
    time::Duration,
};

use prefect::{Job, JobRunner, Queue, RunningJob};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{event, Level};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[derive(Debug)]
struct JobContext {
    counter: AtomicI64,
}

#[derive(Serialize, Deserialize)]
struct AddCounterPayload {
    id: usize,
    add: i64,
}

#[derive(Debug, Serialize)]
struct AddCounterJobResult {
    added: i64,
    result: i64,
}

async fn add_counter(
    job: RunningJob,
    context: Arc<JobContext>,
) -> Result<AddCounterJobResult, eyre::Report> {
    let AddCounterPayload { id, add } = job.json_payload()?;

    let value = context.counter.fetch_add(add, Ordering::Relaxed) + add;
    event!(Level::INFO, added=%add, new_value=%value, job=%id, "Altered the value");

    // Can return any `Serialize` value and it will be logged in the job info.
    Ok(AddCounterJobResult {
        added: add,
        result: value,
    })
}

const ADD_COUNTER_JOB: &str = "increment_counter";

#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<(), eyre::Report> {
    color_eyre::install()?;

    let tree = tracing_tree::HierarchicalLayer::new(2)
        .with_targets(true)
        .with_bracketed_fields(true);
    let env_filter = EnvFilter::try_from_env("LOG").unwrap_or_else(|_| EnvFilter::new("info"));

    let subscriber = tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(tree);
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let queue = Queue::new(Path::new("prefect-examples.db")).await?;

    let context = Arc::new(JobContext {
        counter: AtomicI64::new(0),
    });

    let _worker = prefect::Worker::builder(&queue, context)
        .jobs(vec![
            JobRunner::builder(ADD_COUNTER_JOB, add_counter).build()
        ])
        .min_concurrency(3)
        .max_concurrency(5)
        .build()
        .await?;

    event!(Level::INFO, "Running 20 jobs over the next 10 seconds");
    for i in 0..20 {
        let delay = (rand::random::<f64>() * 10.0).round() as u64;
        let run_time = OffsetDateTime::now_utc() + Duration::from_secs(delay);
        let add = (rand::random::<f64>() * 100.0 - 50.0) as i64;

        event!(Level::INFO, id=%i, delay=%delay, "Scheduling job");
        Job::builder(ADD_COUNTER_JOB)
            .json_payload(&AddCounterPayload { id: i, add })?
            .run_at(run_time)
            .add_to(&queue)
            .await?;
    }

    // Wait for the jobs to run.
    tokio::time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
