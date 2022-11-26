# 0.1.4

- Prefect now handles jobs that were left unfinished due to an unexpected process restart. It treats these as failures,
    and can either reschedule them immediately or like a normal failure, using the task's backoff configuration.

# 0.1.3

- Better output for Job object in INFO-level tracing messages

# 0.1.2

- Remove unnecessary `Sync` restriction on the future returned by a job runner function. This allows job runners to hold
    non-`Sync` values across await points.
- Alter `json_payload` functions in `RunningJob` and `JobBuilder` to return a `prefect::Error::PayloadError` instead of a `serde_json::Error`.

# 0.1.1

- Delay returning results from database writes until after the transaction has been committed.

# 0.1.0

- Initial Release
