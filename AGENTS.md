# Cleary - Agent Guide

## Project Overview

Cleary is a lightweight JVM task scheduler library written in Kotlin. It supports cron
expressions, fixed-rate scheduling, one-shot tasks, retry with exponential backoff, and
concurrency control, explicit run outcomes, and runtime task inspection - all through
a clean DSL with no annotation processing or reflection.

- **Group ID / Artifact**: `io.github.cymoo:cleary`
- **Current version**: `0.2.0`
- **Minimum Java**: 11
- **Build tool**: Maven

## Repository Layout

```
cleary/
├── AGENTS.md
├── README.md
├── README-zh.md
├── pom.xml
├── examples/
│   └── task-dashboard/              # runnable Colleen dashboard example
└── src/
    ├── main/kotlin/io/github/cymoo/cleary/
    │   ├── TaskScheduler.kt          # scheduler lifecycle, queueing, execution
    │   ├── TaskSchedulerConfig.kt    # config, events, TaskRunResult, TaskInfo
    │   ├── TaskBuilder.kt            # task DSL and RetryPolicy
    │   ├── Schedule.kt               # schedules, triggers, duration extensions
    │   └── TaskContext.kt            # per-execution context API
    └── test/kotlin/io/github/cymoo/cleary/
        └── TaskSchedulerTest.kt      # JUnit 5 coverage for public behavior
```

Production code is split by concern but remains in the single package
`io.github.cymoo.cleary`:

| File | Key types / responsibilities |
|---|---|
| `TaskScheduler.kt` | public API: `task`, `start`, `shutdown`, `await`, `run`, `runBlocking`, `enable`, `disable`, `remove`, `exists`, `listTaskNames`, `getTaskInfo`; internal queue dispatch and execution accounting |
| `TaskSchedulerConfig.kt` | configuration DSL, lifecycle/result enums, event payloads, `TaskRunResult`, `TaskInfo` |
| `TaskBuilder.kt` | `TaskBuilder` DSL receiver and `RetryPolicy` backoff calculation |
| `Schedule.kt` | `Schedule` sealed class, cron/fixed-rate/once/initial-delay triggers, duration extension properties |
| `TaskContext.kt` | `TaskContext` interface and isolated per-execution implementation |
| `examples/task-dashboard/` | runnable web UI demonstrating grouping, manual runs, enable/disable/remove/reset, counters, and history |

## Build & Test Commands

```bash
# Compile
mvn compile

# Run tests
mvn test

# Package (JAR)
mvn package

# Install locally for examples
mvn -DskipTests install

# Compile the dashboard example
mvn -f examples/task-dashboard/pom.xml clean compile

# Run the dashboard example
mvn -f examples/task-dashboard/pom.xml compile exec:java

# Publish to Maven Central (requires GPG key + Central credentials)
mvn deploy -P release
```

## Dependencies

| Dependency | Version | Scope |
|---|---|---|
| `org.jetbrains.kotlin:kotlin-stdlib` | `2.3.0` | compile |
| `com.cronutils:cron-utils` | `9.2.1` | compile — Quartz cron parsing |
| `org.jetbrains.kotlin:kotlin-test-junit5` | `2.3.0` | test |
| `org.junit.jupiter:junit-jupiter` | `5.10.2` | test |

## Code Conventions

- **Small, package-local surface**: keep production code in
  `src/main/kotlin/io/github/cymoo/cleary/` and group new types by the existing
  file responsibilities above.
- **Kotlin style**: official Kotlin code style (`kotlin.code.style=official` in
  `pom.xml`). Use extension properties / functions for DSL ergonomics (e.g.
  `5.seconds`, `1.hour`).
- **Thread safety**: every public method must be thread-safe. Shared mutable state uses
  `ConcurrentHashMap` or `Atomic*` types — never `synchronized` blocks on `this`.
- **No reflection, no annotation processing**: keep the zero-magic design.
- **Comments**: only comment non-obvious logic (e.g. overflow guard in backoff math).
  Public API gets KDoc; internal helpers get inline comments only when needed.
- **Error handling**: propagate `InterruptedException` immediately (re-interrupt +
  return/throw) so `shutdown()` is never blocked by sleeping retry loops.

## Key Design Notes

- The scheduler runs on a **single dedicated thread** that drains a `DelayQueue`.
  All task execution happens on a separate **fixed-size worker pool**.
- Worker submissions use a bounded queue (`queueCapacity`). If it is full, the run is
  reported as `TaskRunResult.Rejected` for manual calls and `onTaskRejected` for all calls.
- **Fixed-rate drift prevention**: the next trigger time is anchored to the *planned*
  scheduled time, not to `Instant.now()`, so accumulated execution latency never shifts
  the schedule.
- **Concurrency guard** (`allowConcurrent = false`, default): an in-flight task whose
  next slot arrives while it is still executing is **skipped**, not executed.
- **Context isolation**: the `TaskContext` passed to each task block is a per-execution
  copy of the global context — tasks cannot share mutable state through it accidentally.
- **Retry threading**: retries sleep on the worker thread that ran the first attempt.
  The next *scheduled* slot is re-enqueued before the first attempt begins, so retries
  never delay future runs.
- **Explicit manual outcomes**: `run()` and `runBlocking()` return `TaskRunResult`
  (`Success`, `Failure`, `Skipped`, or `Rejected`) instead of throwing task-body errors.
- **Hook isolation**: exceptions from lifecycle hooks are reported through
  `onSchedulerError` and must not change the task body's result.
