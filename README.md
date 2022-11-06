# prefect

A Rust job queue library, based on SQLite so it doesn't depend on any other services.

Currently this is just a library embeddable into Rust applications, but future goals include bindings into other languages
and the ability to run as a standalone server, accessible by HTTP and gRPC.

```rust

use prefect::{Job, JobDef, Queue, Worker};

async fn run_a_job(job: Job, context: Arc<JobContext>) -> Result<(), Error> {
    // do something with the job
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
  let queue = Queue::new("prefect.db").await?;
  let a_job = JobDef::builder("remind_me", run_a_job).build();

  let worker = worker.builder()
    .max_concurrency(10)
    .job_list([&a_job])
    .build();

  let job_id = queue.add_job(NewJob {
    job_type: "remind_me".to_string(),
    run_at: Some(OffsetDateTime::now_utc() + Duration::from_secs(3600)),
    ..Default::default()
  }).await?;

  let status = queue.get_job_status(job_id).await?;
  assert_eq!(status.status, "pending");
}
```

[Full Development Notes](https://imfeld.dev/notes/projects_prefect)

# Roadmap

## 0.1

- Multiple job types
- Jobs can be added with higher priority to "skip the line"
- Workers can run multiple jobs concurrently
- Schedule jobs in the future
- Automatically retry failed jobs, with exponential backoff

## Soon

- Support for recurring scheduled jobs

## Later

- Node.js bindings
- Run as a standalone server
- Communicate with the queue via the outbox pattern.
