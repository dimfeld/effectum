use std::{fmt::Debug, fmt::Display, pin::Pin, sync::Arc};

use ahash::HashMap;
use futures::Future;
use serde::Serialize;
use serde_json::json;

use crate::{job::Job, worker::Worker, Queue, SmartString};

pub(crate) type JobFn<CONTEXT> =
    Arc<dyn Fn(Job, CONTEXT) -> tokio::task::JoinHandle<()> + Send + Sync + 'static>;
pub(crate) type JobResult = String;

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
    pub weight: usize,
    pub runner: JobFn<CONTEXT>,
}

impl<CONTEXT> JobDef<CONTEXT>
where
    CONTEXT: Send + Sync + Debug + Clone + 'static,
{
    pub const fn new<F, Fut, T, E>(
        name: impl Into<SmartString>,
        runner: F,
        weight: usize,
    ) -> JobDef<CONTEXT>
    where
        F: Fn(&mut Job, CONTEXT) -> Fut + Send + Sync + Clone,
        CONTEXT: Send + Debug + Clone + 'static,
        Fut: Future<Output = Result<T, E>> + Send + Sync,
        T: Send + Sync + Serialize + 'static,
        E: Send + Display + 'static,
    {
        let f = |job: Job, context: CONTEXT| {
            let runner = runner.clone();
            tokio::spawn(async move {
                let result = runner(&mut job, context).await;

                if !job.is_done() && !job.is_expired() {
                    match result {
                        Ok(info) => {
                            job.complete(info).await;
                        }
                        Err(e) => {
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
        }
    }
}
