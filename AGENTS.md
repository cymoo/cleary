# Cleary - Agent Guide

## Project Overview

Cleary is a lightweight JVM task scheduler library written in Kotlin. It supports cron
expressions, fixed-rate and fixed-delay scheduling, one-shot tasks, custom triggers,
retry with exponential backoff, per-attempt timeouts, misfire policies, concurrency
control, explicit run outcomes, and runtime task inspection - all through a clean DSL
with no annotation processing or reflection.

- **Group ID / Artifact**: `io.github.cymoo:cleary`
- **Current version**: `0.3.0`
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
| `TaskScheduler.kt` | public API: `taskScheduler`, `task`, `replace`, `start`, `shutdown`, `await`, `run`, `runBlocking`, `enable`, `disable`, `remove`, `exists`, `listTaskNames`, `listTasks`, `getTaskInfo`; internal queue dispatch (fires, retries, timeouts) and execution accounting |
| `TaskSchedulerConfig.kt` | configuration DSL, `MisfirePolicy`, lifecycle/result enums, event payloads, `TaskRunResult`, `TaskInfo`, `TaskTimeoutException` |
| `TaskBuilder.kt` | `TaskBuilder` DSL receiver (schedules, timeout, tags, per-task hooks) and `RetryPolicy` backoff calculation |
| `Schedule.kt` | `Schedule` sealed class, public `Trigger` interface, cron/fixed-rate/fixed-delay/once/custom trigger implementations |
| `TaskContext.kt` | `TaskContext` interface, reified typed accessors, copy-on-write per-execution implementation |
| `examples/task-dashboard/` | runnable web UI demonstrating grouping, manual runs, enable/disable/remove/reset, counters, and history (still targets the 0.2.x API) |

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

- The scheduler runs on a **single dedicated thread** that drains a `DelayQueue` of
  `Fire` (scheduled runs), `Retry` (pending retry attempts), and `Timeout` (attempt
  watchdogs) items. All task execution happens on a separate **fixed-size worker pool**.
- **Single-stream invariant**: each task has at most one live `Fire` in the queue,
  enforced by a CAS on `TaskEntry.currentFire`. `disable`/`remove`/`replace` kill the
  stream by swapping that reference; stale queue items are dropped by identity checks
  at dispatch time. Never `offer` a `Fire` without going through this CAS.
- **Misfire policy**: under the default `SKIP`, late fires run once and the trigger
  is asked for the next time strictly after `now` (fixed-rate stays on its grid);
  `CATCH_UP` replays every missed slot.
- Worker submissions use a bounded queue (`queueCapacity`). If it is full, the run is
  reported as `TaskRunResult.Rejected` for manual calls and `onTaskRejected` for all calls.
- **Fixed-rate drift prevention**: the next trigger time is anchored to the *planned*
  scheduled time, not to `Instant.now()`, so accumulated execution latency never shifts
  the schedule. Fixed-delay tasks instead re-arm when the execution's future completes.
- **Concurrency guard** (`allowConcurrent = false`, default): an in-flight task whose
  next slot arrives while it is still executing is **skipped**, not executed. The
  fast-path check happens before the worker pool is involved; the worker-side CAS in
  `beginExecution` remains the authoritative guard.
- **Context isolation**: the `TaskContext` passed to each task block is a copy-on-write
  view over the global context — reads fall through until the first write materializes
  a private map, so non-writing executions allocate nothing.
- **Retry threading**: retries wait in the scheduler's `DelayQueue`, never on a worker
  thread. One `PendingExecution` spans all attempts and completes a single
  `CompletableFuture`. `InterruptedException` and `Error` are never retried.
- **No hung futures**: every unfinished `PendingExecution` is tracked in `livePendings`;
  `shutdown()` settles queued retries early and sweeps whatever `shutdownNow` dropped,
  so `run()`/`runBlocking()` callers can never hang across shutdown.
- **Timeouts**: a `Timeout` watchdog item interrupts the attempt's thread; ownership
  of the outcome is decided by a CAS on the attempt's `done` flag so a completed
  attempt is never interrupted retroactively. Watchdog items are removed when the
  attempt completes, each attempt starts by clearing any leaked interrupt flag, and
  all deadline math uses `saturatedAdd` (so `Duration.INFINITE` timeouts are safe).
- **Explicit manual outcomes**: `run()` and `runBlocking()` return `TaskRunResult`
  (`Success`, `Failure`, `Skipped`, or `Rejected`) instead of throwing task-body errors.
- **Hook isolation**: exceptions from lifecycle hooks are reported through
  `onSchedulerError` and must not change the task body's result. Per-task hooks fire
  before their global counterparts. Hooks may run on scheduler/worker/caller threads.
- **Default logging**: when no relevant hook is configured, failures are logged via
  `System.Logger` (`io.github.cymoo.cleary`) — final task failures at `WARNING`,
  scheduler errors at `ERROR`.
- **Durations**: the public API uses `kotlin.time.Duration` with `java.time.Duration`
  overloads on every DSL function; do not reintroduce custom duration extensions.
