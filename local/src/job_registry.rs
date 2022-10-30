use std::{borrow::Borrow, fmt::Debug, fmt::Display, panic::AssertUnwindSafe, pin::Pin, sync::Arc};

use ahash::HashMap;
use futures::{Future, FutureExt};
use serde::Serialize;
use tracing::{event, span, Level};

use crate::{job::Job, worker::log_error, SmartString};

pub(crate) type JobFn<CONTEXT> =
    Arc<dyn Fn(Job, CONTEXT) -> tokio::task::JoinHandle<()> + Send + Sync + 'static>;

pub struct JobRegistry<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub(crate) jobs: HashMap<SmartString, JobDef<CONTEXT>>,
}

impl<CONTEXT> JobRegistry<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub fn new<JOBLIST>(jobs: JOBLIST) -> JobRegistry<CONTEXT>
    where
        JOBLIST: IntoIterator,
        JOBLIST::Item: Borrow<JobDef<CONTEXT>>,
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

    pub fn add(&mut self, job: &JobDef<CONTEXT>) {
        self.jobs.insert(job.name.clone(), job.clone());
    }
}

#[derive(Clone)]
pub struct JobDef<CONTEXT>
where
    CONTEXT: Send + Debug + Clone + 'static,
{
    pub name: SmartString,
    pub runner: JobFn<CONTEXT>,
    pub weight: u16,
    pub autoheartbeat: bool,
}

impl<CONTEXT> JobDef<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub fn new<F, Fut, T, E>(
        name: impl Into<SmartString>,
        runner: F,
        weight: u16,
        autoheartbeat: bool,
    ) -> JobDef<CONTEXT>
    where
        F: Fn(Job, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
        CONTEXT: Send + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send + Sync,
        T: Send + Sync + Debug + Serialize + 'static,
        E: Send + Display + 'static,
    {
        let f = move |job: Job, context: CONTEXT| {
            let runner = runner.clone();
            tokio::spawn(async move {
                let result = {
                    let span = span!(Level::INFO, "run_job", %job);
                    let _enter = span.enter();
                    AssertUnwindSafe(runner(job.clone(), context))
                        .catch_unwind()
                        .await
                };

                let explicitly_finished = job.is_done().await;
                event!(Level::DEBUG, ?job, %explicitly_finished, now=%job.queue.time.now(), "done");
                if !explicitly_finished {
                    match result {
                        Err(e) => {
                            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = e.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "Panic".to_string()
                            };

                            log_error(job.fail(msg).await);
                        }
                        Ok(Ok(info)) => {
                            log_error(job.complete(info).await);
                        }
                        Ok(Err(e)) => {
                            let msg = e.to_string();
                            log_error(job.fail(msg).await);
                        }
                    }
                }
            })
        };

        JobDef {
            name: name.into(),
            weight,
            runner: Arc::new(f),
            autoheartbeat,
        }
    }

    pub fn builder<F, Fut, T, E>(name: impl Into<SmartString>, runner: F) -> JobDefBuilder<CONTEXT>
    where
        F: Fn(Job, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
        CONTEXT: Send + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send + Sync,
        T: Send + Sync + Debug + Serialize + 'static,
        E: Send + Display + 'static,
    {
        let def = JobDef::new(name, runner, 1, false);
        JobDefBuilder { def }
    }
}

pub struct JobDefBuilder<CONTEXT>
where
    CONTEXT: Send + Debug + Clone + 'static,
{
    def: JobDef<CONTEXT>,
}

impl<CONTEXT> JobDefBuilder<CONTEXT>
where
    CONTEXT: Send + Debug + Clone + 'static,
{
    pub fn autoheartbeat(&mut self, autoheartbeat: bool) -> &mut Self {
        self.def.autoheartbeat = autoheartbeat;
        self
    }

    pub fn weight(&mut self, weight: u16) -> &mut Self {
        self.def.weight = weight;
        self
    }

    pub fn build(self) -> JobDef<CONTEXT> {
        self.def
    }
}
