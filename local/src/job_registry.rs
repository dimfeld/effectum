use std::{fmt::Debug, fmt::Display, panic::AssertUnwindSafe, sync::Arc};

use ahash::HashMap;
use futures::{Future, FutureExt};
use serde::Serialize;

use crate::{job::Job, SmartString};

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
        JOBLIST::Item: AsRef<JobDef<CONTEXT>>,
    {
        let jobs = jobs
            .into_iter()
            .map(|d| {
                let d = d.as_ref().to_owned();
                (d.name.clone(), d)
            })
            .collect();

        JobRegistry { jobs }
    }
}

#[derive(Clone)]
pub struct JobDef<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub name: SmartString,
    pub runner: JobFn<CONTEXT>,
    pub weight: u16,
    pub autohearbeat: bool,
}

impl<CONTEXT> JobDef<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub fn new<F, Fut, T, E>(
        name: impl Into<SmartString>,
        runner: F,
        weight: u16,
        autohearbeat: bool,
    ) -> JobDef<CONTEXT>
    where
        F: Fn(&mut Job, CONTEXT) -> Fut + Send + Sync + Clone + 'static,
        CONTEXT: Send + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send + Sync,
        T: Send + Sync + Serialize + 'static,
        E: Send + Display + 'static,
    {
        let f = move |mut job: Job, context: CONTEXT| {
            let runner = runner.clone();
            tokio::spawn(async move {
                let result = AssertUnwindSafe(runner(&mut job, context))
                    .catch_unwind()
                    .await;

                if !job.is_done() && !job.is_expired() {
                    match result {
                        Err(e) => {
                            let msg = if let Some(s) = e.downcast_ref::<&str>() {
                                s.to_string()
                            } else if let Some(s) = e.downcast_ref::<String>() {
                                s.clone()
                            } else {
                                "Panic".to_string()
                            };

                            job.fail(msg).await;
                        }
                        Ok(Ok(info)) => {
                            job.complete(info).await;
                        }
                        Ok(Err(e)) => {
                            let msg = e.to_string();
                            job.fail(msg).await;
                        }
                    }
                }
            })
        };

        JobDef {
            name: name.into(),
            weight,
            runner: Arc::new(f),
            autohearbeat,
        }
    }
}
