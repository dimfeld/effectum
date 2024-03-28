//! This is a simple example of using the queue to schedule a number of jobs that use a JSON
//! payload and access the context.

use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use ahash::HashMap;
use effectum::{Job, JobRunner, Queue, RecurringJobSchedule, RunningJob};
use time::OffsetDateTime;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};

#[derive(Debug)]
struct JobContext {
    start: OffsetDateTime,
    job_times: Mutex<HashMap<String, Vec<i64>>>,
}

async fn print_message_job(job: RunningJob, context: Arc<JobContext>) -> Result<(), eyre::Report> {
    let message: String = job.json_payload()?;
    let seconds = OffsetDateTime::now_utc() - context.start;
    println!(
        "Running job: {message} after {}s",
        seconds.as_seconds_f32().round()
    );

    context
        .job_times
        .lock()
        .unwrap()
        .entry(message)
        .or_default()
        .push(seconds.as_seconds_f32() as i64);

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
        job_times: Mutex::new(HashMap::default()),
    });

    let worker = effectum::Worker::builder(&queue, context.clone())
        .jobs(vec![JobRunner::builder(
            PRINT_MESSAGE_JOB,
            print_message_job,
        )
        .build()])
        .min_concurrency(3)
        .max_concurrency(5)
        .build()
        .await?;

    let times = [2, 3, 5];
    for time in &times {
        let job_name = format!("job_{time}");
        let schedule = RecurringJobSchedule::RepeatEvery {
            interval: Duration::from_secs(*time),
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

    drop(worker);

    for time in &times {
        let key = format!("job_{time}");
        let times = context.job_times.lock().unwrap().remove(&key).unwrap();
        println!("{key} ran at: {times:?}");
    }

    Ok(())
}
