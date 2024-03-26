use std::{
    borrow::Borrow,
    fmt::{Debug, Display},
    marker::PhantomData,
    panic::AssertUnwindSafe,
    sync::Arc,
};

use ahash::HashMap;
use futures::{Future, FutureExt};
use serde::Serialize;
use tracing::{event, span, Instrument, Level};

use crate::{job::RunningJob, worker::log_error, SmartString};

pub(crate) type JobFn<CONTEXT> =
    Arc<dyn Fn(RunningJob, CONTEXT) -> tokio::task::JoinHandle<()> + Send + Sync + 'static>;

/// A list of jobs that can be run by a worker.
pub struct JobRegistry<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub(crate) jobs: HashMap<SmartString, JobRunner<CONTEXT>>,
}

impl<CONTEXT> JobRegistry<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    /// Create a new job registry from a list of [JobRunners](JobRunner).
    pub fn new<JOBLIST>(jobs: JOBLIST) -> JobRegistry<CONTEXT>
    where
        JOBLIST: IntoIterator,
        JOBLIST::Item: Borrow<JobRunner<CONTEXT>>,
    {
        let jobs = jobs
            .into_iter()
            .map(|d| {
                let d = d.borrow().to_owned();
                (d.name.clone(), d)
            })
            .collect();

        JobRegistry { jobs }
    }

    /// Add a [JobRunner] to an existing registry.
    pub fn add(&mut self, job: &JobRunner<CONTEXT>) {
        self.jobs
            .entry(job.name.clone())
            .and_modify(|_| {
                panic!("Job {} already exists", job.name);
            })
            .or_insert_with(|| job.clone());
    }
}

/// A definition of a job, including the name of the job, the function that runs the job, and
/// other settings.
///
/// The function that runs the job should be an `async` function that takes a [RunningJob] and a context object.
/// All jobs for a particular worker must have the same context type.
///
/// The function can be either a normal function or a closure.
///
/// ```
/// # use effectum::*;
/// # use std::sync::Arc;
/// #[derive(Debug)]
/// pub struct JobContext {
///   // database pool or other things here
/// }
///
/// let job = JobRunner::builder("a_job", |job: RunningJob, context: Arc<JobContext>| async move {
///   // do some work
///   Ok::<_, Error>("optional info about the success")
/// }).build();
///
/// async fn another_job(job: RunningJob, context: Arc<JobContext>) -> Result<String, Error> {
///   // do some work
///   Ok("optional info about the success".to_string())
/// }
///
/// let another_job = JobRunner::builder("another_job", another_job)
///     .autoheartbeat(true)
///     .build();
/// ```
#[derive(Clone)]
pub struct JobRunner<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub(crate) name: SmartString,
    pub(crate) runner: JobFn<CONTEXT>,
    pub(crate) autoheartbeat: bool,
}

/// Options for a [JobRunner]
#[derive(Debug, Clone, Default)]
pub struct JobRunnerOptions {
    /// If false (default), format failures with the [Display] implementation when storing the
    /// information in the database. If true, use the [Debug] implementation.
    pub format_failures_with_debug: bool,
    /// If true, automatically heartbeat the job when it's running.
    pub autoheartbeat: bool,
}

impl<CONTEXT> JobRunner<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    /// Create a new [JobRunner], passing all the possible fields. Generally it's easier to use
    /// [JobRunner::builder].
    pub fn new<F, Fut, T, E>(
        name: impl Into<SmartString>,
        runner: F,
        autoheartbeat: bool,
    ) -> JobRunner<CONTEXT>
    where
        F: Fn(RunningJob, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
        CONTEXT: Send + Sync + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send + Debug + Serialize + 'static,
        E: Send + Debug + Display + 'static,
    {
        let options = JobRunnerOptions {
            format_failures_with_debug: false,
            autoheartbeat,
        };

        Self::with_options(name, options, runner)
    }

    /// Create a new [JobRunner] with options
    pub fn with_options<F, Fut, T, E>(
        name: impl Into<SmartString>,
        def: JobRunnerOptions,
        runner: F,
    ) -> JobRunner<CONTEXT>
    where
        F: Fn(RunningJob, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
        CONTEXT: Send + Sync + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send + Debug + Serialize + 'static,
        E: Send + Debug + Display + 'static,
    {
        let name = name.into();
        let JobRunnerOptions {
            format_failures_with_debug,
            autoheartbeat,
        } = def;
        let f = move |job: RunningJob, context: CONTEXT| {
            let runner = runner.clone();
            tokio::spawn(async move {
                let result = {
                    let span = span!(Level::INFO, "run_job", %job);
                    AssertUnwindSafe(runner(job.clone(), context).instrument(span))
                        .catch_unwind()
                        .await
                };

                let explicitly_finished = job.is_done().await;
                event!(Level::DEBUG, ?job, %explicitly_finished, now=%job.queue.time.now(), "done");
                match result {
                    Err(e) => {
                        let msg = if let Some(s) = e.downcast_ref::<&str>() {
                            s.to_string()
                        } else if let Some(s) = e.downcast_ref::<String>() {
                            s.clone()
                        } else {
                            "Panic".to_string()
                        };

                        if explicitly_finished {
                            event!(Level::ERROR, %msg, "Job panicked after it was completed");
                        } else {
                            log_error(job.fail(msg).await);
                        }
                    }
                    Ok(Ok(info)) => {
                        if !explicitly_finished {
                            log_error(job.complete(info).await);
                        }
                    }
                    Ok(Err(e)) => {
                        if explicitly_finished {
                            if format_failures_with_debug {
                                event!(Level::ERROR, err = ?e, "Job returned error after it was completed");
                            } else {
                                event!(Level::ERROR, err = %e, "Job returned error after it was completed");
                            }
                        } else {
                            let msg = if format_failures_with_debug {
                                format!("{e:?}")
                            } else {
                                e.to_string()
                            };
                            log_error(job.fail(msg).await);
                        }
                    }
                }
            })
        };

        JobRunner {
            name: name.into(),
            runner: Arc::new(f),
            autoheartbeat,
        }
    }

    /// Create a [JobRunnerBuilder] for this job.
    pub fn builder<F, Fut, T, E>(
        name: impl Into<SmartString>,
        runner: F,
    ) -> JobRunnerBuilder<F, Fut, T, E, CONTEXT>
    where
        F: Fn(RunningJob, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
        CONTEXT: Send + Sync + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send + Debug + Serialize + 'static,
        E: Send + Debug + Display + 'static,
    {
        JobRunnerBuilder::new(name, runner)
    }
}

/// A builder object for a [JobRunner].
pub struct JobRunnerBuilder<F, Fut, T, E, CONTEXT>
where
    F: Fn(RunningJob, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
    CONTEXT: Send + Sync + Debug + Clone + 'static,
    Fut: Future<Output = Result<T, E>> + Send,
    T: Send + Debug + Serialize + 'static,
    E: Send + Debug + Display + 'static,
{
    name: SmartString,
    runner_fn: F,
    def: JobRunnerOptions,
    _fut: PhantomData<Fut>,
    _t: PhantomData<T>,
    _e: PhantomData<E>,
    _context: PhantomData<CONTEXT>,
}

impl<F, Fut, T, E, CONTEXT> JobRunnerBuilder<F, Fut, T, E, CONTEXT>
where
    F: Fn(RunningJob, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
    CONTEXT: Send + Sync + Debug + Clone + 'static,
    Fut: Future<Output = Result<T, E>> + Send,
    T: Send + Debug + Serialize + 'static,
    E: Send + Debug + Display + 'static,
{
    /// Create a new [JobRunnerBuilder].
    pub fn new(name: impl Into<SmartString>, runner_fn: F) -> Self {
        Self {
            runner_fn,
            name: name.into(),
            def: JobRunnerOptions {
                format_failures_with_debug: false,
                autoheartbeat: false,
            },
            _fut: PhantomData,
            _t: PhantomData,
            _e: PhantomData,
            _context: PhantomData,
        }
    }

    /// Set whether the job should automatically send heartbeats while it runs.
    pub fn autoheartbeat(mut self, autoheartbeat: bool) -> Self {
        self.def.autoheartbeat = autoheartbeat;
        self
    }

    /// If true, format failures with the [Debug] implementation when storing the
    /// information in the database. If false, use the [Display] implementation. The default is
    /// false.
    pub fn format_failures_with_debug(mut self, format_failures_with_debug: bool) -> Self {
        self.def.format_failures_with_debug = format_failures_with_debug;
        self
    }

    /// Consume the builder, returning a [JobRunner].
    pub fn build(self) -> JobRunner<CONTEXT> {
        JobRunner::with_options(self.name, self.def, self.runner_fn)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{JobRegistry, JobRunner};
    use crate::{
        test_util::{TestContext, TestEnvironment},
        RunningJob,
    };

    async fn test_job(_job: RunningJob, _context: ()) -> Result<(), String> {
        Ok(())
    }

    #[test]
    fn create_job_from_fn() {
        JobRunner::new("test", test_job, false);
    }

    mod registry_joblist {
        use super::*;

        #[tokio::test]
        async fn slice_of_objects() {
            let job = JobRunner::new("test", test_job, false);
            JobRegistry::new(&[job]);
        }

        #[tokio::test]
        async fn array_of_objects() {
            let job = JobRunner::new("test", test_job, false);
            JobRegistry::new([job]);
        }

        #[tokio::test]
        async fn array_of_refs() {
            let job = JobRunner::new("test", test_job, false);
            JobRegistry::new([&job]);
        }

        #[tokio::test]
        async fn vec_of_objects() {
            let job = JobRunner::new("test", test_job, false);
            JobRegistry::new(vec![job]);
        }

        #[tokio::test]
        async fn vec_of_refs() {
            let job = JobRunner::new("test", test_job, false);
            JobRegistry::new(vec![&job]);
        }
    }

    #[tokio::test]
    #[should_panic]
    async fn disallow_adding_same_job_type_twice() {
        let mut test = TestEnvironment::new().await;

        let job = JobRunner::builder("counter", |_, _context: Arc<TestContext>| async {
            Ok::<_, String>(())
        })
        .build();
        test.registry.add(&job);
    }
}
