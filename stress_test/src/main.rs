use eyre::{eyre, Result};
use futures::future::try_join_all;
use std::{sync::Arc, time::Duration};
use uuid::Uuid;

use clap::Parser;
use prefect::{JobDef, JobRegistry, NewJob, Queue};
use temp_dir::TempDir;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value_t = 500000)]
    num_jobs: usize,

    #[arg(long)]
    worker_min_concurrency: Option<u16>,

    #[arg(long, default_value_t = 20)]
    worker_max_concurrency: u16,

    #[arg(long, default_value_t = 10)]
    num_workers: usize,

    #[arg(long, default_value_t = 20)]
    new_job_batch_size: usize,

    #[arg(long, default_value_t = 10)]
    num_submit_tasks: usize,
}

async fn empty_task(job: prefect::Job, context: ()) -> Result<(), String> {
    Ok(())
}

async fn submit_task(
    queue: Arc<Queue>,
    batch_size: usize,
    num_batches: usize,
) -> Result<Vec<Vec<(i64, Uuid)>>> {
    let mut job_ids = Vec::with_capacity(num_batches);
    for _ in 0..num_batches {
        let jobs = (0..batch_size)
            .map(|_| NewJob {
                job_type: "empty".to_string(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let ids = queue.add_jobs(jobs).await?;
        job_ids.push(ids);
    }
    Ok(job_ids)
}

async fn run() -> Result<()> {
    let args = Args::parse();

    let min_concurrency = args
        .worker_min_concurrency
        .unwrap_or(args.worker_max_concurrency);

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("prefect.db");
    let queue = Arc::new(Queue::new(&path).await.unwrap());

    let empty_job = JobDef::builder("empty", empty_task).build();
    let registry = JobRegistry::new(&[empty_job]);

    let workers = futures::future::try_join_all(
        (0..args.num_workers)
            .map(|_| {
                prefect::Worker::builder(&registry, &queue, ())
                    .min_concurrency(min_concurrency)
                    .max_concurrency(args.worker_max_concurrency)
                    .build()
            })
            .collect::<Vec<_>>(),
    )
    .await
    .unwrap();

    let jobs_per_worker = args.num_jobs / args.num_submit_tasks;
    let batches_per_worker = jobs_per_worker / args.new_job_batch_size;

    let submit_tasks = (0..args.num_submit_tasks)
        .map(|_| {
            tokio::spawn(submit_task(
                queue.clone(),
                args.new_job_batch_size,
                batches_per_worker,
            ))
        })
        .collect::<Vec<_>>();

    let expected_total =
        (args.new_job_batch_size * batches_per_worker * args.num_submit_tasks) as u64;
    let mut last_log_time = tokio::time::Instant::now();
    let mut last_total = 0;
    let log_interval = 10;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let total = workers
            .iter()
            .map(|w| w.counts().finished as u64)
            .sum::<u64>();
        let since_last = last_log_time.elapsed();
        if since_last > Duration::from_secs(log_interval) || total >= expected_total {
            last_log_time = tokio::time::Instant::now();

            let rate = (total - last_total) * 1000 / (since_last.as_millis() as u64);
            println!("Finished {total} jobs ({}/sec)", rate);
            last_total = total;
        }

        if total >= expected_total {
            break;
        }
    }

    let values = try_join_all(submit_tasks).await?;

    println!("Verifying results...");
    let mut checked = 0;
    for task in values {
        for batch in task.unwrap() {
            for (_, uuid) in batch {
                let job = queue.get_job_status(uuid).await?;
                assert_eq!(job.status, "success");
                checked += 1;
            }
        }
    }

    println!("{checked} jobs ok");

    queue.close(std::time::Duration::from_secs(86400)).await?;

    Ok(())
}

#[cfg(not(feature = "rt-multi-thread"))]
#[tokio::main(flavor = "current_thread")]
pub async fn main() -> Result<()> {
    run().await
}

#[cfg(feature = "rt-multi-thread")]
#[tokio::main(flavor = "multi_thread")]
pub async fn main() -> Result<()> {
    run().await
}
