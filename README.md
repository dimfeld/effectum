# prefect

A Rust job queue library, based on SQLite so it doesn't depend on any other services.

Currently this is just a library embeddable into Rust applications, but future goals include bindings into other languages
and the ability to run as a standalone server, accessible by HTTP and gRPC.

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
