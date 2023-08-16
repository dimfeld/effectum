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

use effectum::{Job, JobRunner, Queue, RecurringJobSchedule, RunningJob};
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use tracing::{event, Level};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[derive(Debug)]
struct JobContext {
    start: OffsetDateTime,
}

async fn print_message_job(job: RunningJob, context: Arc<JobContext>) -> Result<(), eyre::Report> {
    let message: String = job.json_payload()?;
    let seconds = OffsetDateTime::now_utc() - context.start;
    println!(
        "Running job: {message} after {}s",
        seconds.as_seconds_f32().round()
    );
    Ok(())
}

const PRINT_MESSAGE_JOB: &str = "print_message";

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

    let queue = Queue::new(Path::new("effectum-examples.db")).await?;

    let context = Arc::new(JobContext {
        start: OffsetDateTime::now_utc(),
    });

    let _worker = effectum::Worker::builder(&queue, context)
        .jobs(vec![JobRunner::builder(
            PRINT_MESSAGE_JOB,
            print_message_job,
        )
        .build()])
        .min_concurrency(3)
        .max_concurrency(5)
        .build()
        .await?;

    for time in [2, 3, 5] {
        let job_name = format!("job_{time}");
        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(time),
        };

        let job = Job::builder(PRINT_MESSAGE_JOB)
            .json_payload(&serde_json::json!(job_name.clone()))?
            .build();
        queue
            .upsert_recurring_job(job_name, schedule, job, false)
            .await?;
    }

    // Wait for the jobs to run.
    tokio::time::sleep(Duration::from_secs(15)).await;

    Ok(())
}
