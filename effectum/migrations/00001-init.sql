CREATE TABLE active_jobs (
  job_id INTEGER PRIMARY KEY,
  active_worker_id bigint,
  priority int not null default 0,
  run_at bigint not null,
  started_at bigint,
  expires_at bigint
);
CREATE INDEX active_run_at ON active_jobs(priority desc, run_at) WHERE active_worker_id is null;
CREATE INDEX running_expires_at ON active_jobs(expires_at) WHERE active_worker_id is not null;

CREATE TABLE jobs (
  job_id INTEGER PRIMARY KEY,
  external_id blob not null,
  job_type text not null,
  priority int not null default 0,
  weight int not null default 1,
  status text,
  from_recurring_job int,
  orig_run_at bigint not null,
  payload blob,
  checkpointed_payload blob,
  current_try int not null default 0,
  max_retries int not null,
  backoff_multiplier real not null,
  backoff_randomization real not null,
  backoff_initial_interval int not null,
  added_at bigint not null,
  default_timeout int not null,
  heartbeat_increment int not null,
  started_at bigint,
  finished_at bigint,
  run_info text
);

CREATE UNIQUE INDEX active_jobs_external_id ON jobs(external_id);
CREATE INDEX done_jobs_job_type ON jobs(finished_at desc) where finished_at is not null;
CREATE INDEX done_jobs_job_type_and_time ON jobs(job_type, finished_at desc) where finished_at is not null;

CREATE TABLE recurring (
  recurrring_job_id INTEGER PRIMARY KEY,
  external_id text not null,
  base_job_id bigint references jobs(job_id),
  schedule text not null
);

CREATE UNIQUE INDEX recurring_external_id ON recurring(external_id);
