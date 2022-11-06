# prefect

A Rust job queue library, based on SQLite so it doesn't depend on any other services.

Currently this is just a library embeddable into Rust applications, but future goals include bindings into other languages
and the ability to run as a standalone server, accessible by HTTP and gRPC.

```rust
use prefect::{Error, Job, JobState, JobRunner, RunningJob, Queue, Worker};

#[derive(Debug)]
pub struct JobContext {
   // database pool or other things here
}

#[derive(Serialize, Deserialize)]
struct RemindMePayload {
  email: String,
  message: String,
}

async fn remind_me_job(job: RunningJob, context: Arc<JobContext>) -> Result<(), Error> {
    let payload: RemindMePayload = job.json_payload()?;
    // do something with the job
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Error> {
  // Create a queue
  let queue = Queue::new(&PathBuf::from("prefect.db")).await?;

  // Define a type job for the queue.
  let a_job = JobRunner::builder("remind_me", remind_me_job).build();

  let context = Arc::new(JobContext{
    // database pool or other things here
  });

  // Create a worker to run jobs.
  let worker = Worker::builder(&queue, context)
    .max_concurrency(10)
    .jobs([a_job])
    .build();

  // Submit a job to the queue.
  let job_id = Job::builder("remind_me")
    .run_at(time::OffsetDateTime::now_utc() + std::time::Duration::from_secs(3600))
    .json_payload(&RemindMePayload {
        email: "me@example.com".to_string(),
        message: "Time to go!".to_string()
    })
    .unwrap()
    .add_to(&queue)
    .await?;

  // See what's happening with the job.
  let status = queue.get_job_status(job_id).await?;
  assert_eq!(status.state, JobState::Pending);

  // Do other stuff...

  Ok(())
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
