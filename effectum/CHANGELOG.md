# 0.7.0

- Upgrade to rusqlite 0.31.0 and libsqlite3-sys 0.28

# 0.6.0

- Jobs can now have a name, which can be used as an argument to `get_jobs_by_name`. The name does not have to be unique.

# 0.5.1

- Allow configuring whether to use the `Display` or `Debug` formatter to format failure information.

# 0.5.0

- The `Job` type now contains the UUID which can be used to reference the job, instead of this value being generated
    inside the queue and returned by `add_job`. This helps to know the ID of the job when you may not be able to examine
    the return value of `add_job`, such as when submitting jobs using a transactional outbox pattern.

# 0.4.1

- Ensure that add_job futures don't do anything if you forget to await the future.

# 0.4.0

- Upgrade to rusqlite 0.30.0
- Upgrade rusqlite_migration to 1.1.0 to keep up with rusqlite.
- Add `list_recurring_jobs_with_prefix` function. 
- Add trace message when a `Worker` unregisters.
- Add `#[must_use]` directive on `Worker`. This helps avoid dropping a Worker too early, which can disconnect it from
    the queue before you want it to stop.

# 0.3.0

- Upgrade to rusqlite 0.29.0

# 0.2.0

- Support recurring jobs using cron strings or simple "every N" schedules.

# 0.1.5

- Support updating and cancelling jobs that have been submitted, but have not yet run.

# 0.1.4

- effectum now handles jobs that were left unfinished due to an unexpected process restart. It treats these as failures,
    and can either reschedule them immediately or like a normal failure, using the task's backoff configuration.

# 0.1.3

- Better output for Job object in INFO-level tracing messages

# 0.1.2

- Remove unnecessary `Sync` restriction on the future returned by a job runner function. This allows job runners to hold
    non-`Sync` values across await points.
- Alter `json_payload` functions in `RunningJob` and `JobBuilder` to return a `effectum::Error::PayloadError` instead of a `serde_json::Error`.

# 0.1.1

- Delay returning results from database writes until after the transaction has been committed.

# 0.1.0

- Initial Release
