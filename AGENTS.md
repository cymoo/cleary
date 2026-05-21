# Cleary — Agent Guide

## Project Overview

Cleary is a lightweight JVM task scheduler library written in Kotlin. It supports cron
expressions, fixed-rate scheduling, one-shot tasks, retry with exponential backoff, and
concurrency control — all through a clean DSL with no annotation processing or reflection.

- **Group ID / Artifact**: `io.github.cymoo:cleary`
- **Current version**: `0.1.0`
- **Minimum Java**: 11
- **Build tool**: Maven

## Repository Layout

```
cleary/
├── pom.xml
└── src/
    ├── main/kotlin/io/github/cymoo/cleary/
    │   └── TaskScheduler.kt          # entire library — single file
    └── test/kotlin/                  # JUnit 5 tests (none committed yet)
```

The entire library lives in one file: `TaskScheduler.kt`. Internal structure (top to
bottom):

| Section | Key types |
|---------|-----------|
| `TaskScheduler` class | public API: `task`, `start`, `shutdown`, `await`, `run`, `runBlocking`, `enable`, `disable`, `remove` |
| `TaskSchedulerConfig` | configuration DSL (`concurrency`, `autoStart`, `registerShutdownHook`, `context`, `onTaskStart`, `onTaskComplete`, `onRetry`) |
| Events | `TaskStartEvent`, `TaskCompleteEvent`, `TaskRetryEvent` |
| `TaskContext` / `TaskContextImpl` | per-execution key-value context |
| `Schedule` sealed class | `Cron`, `FixedRate`, `Once`, `WithInitialDelay` |
| `Trigger` / `CronTrigger` / `FixedRateTrigger` / `OnceTrigger` | internal scheduling math |
| `TaskBuilder` | DSL receiver for `task { }` blocks |
| `RetryPolicy` | retry configuration and delay calculation |
| `TaskEntry` / `ScheduledTask` | internal state per registered task |

## Build & Test Commands

```bash
# Compile
mvn compile

# Run tests
mvn test

# Package (JAR)
mvn package

# Publish to Maven Central (requires GPG key + Central credentials)
mvn deploy -P release
```

## Dependencies

| Dependency | Version | Scope |
|---|---|---|
| `org.jetbrains.kotlin:kotlin-stdlib` | `2.3.0` | compile |
| `com.cronutils:cron-utils` | `9.2.1` | compile — Quartz cron parsing |
| `org.junit.jupiter:junit-jupiter` | `5.10.2` | test |

## Code Conventions

- **Single-file library**: all production code stays in `TaskScheduler.kt`. Do not
  split into multiple files unless the library grows significantly.
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
- **Fixed-rate drift prevention**: the next trigger time is anchored to the *planned*
  scheduled time, not to `Instant.now()`, so accumulated execution latency never shifts
  the schedule.
- **Concurrency guard** (`allowConcurrent = false`, default): an in-flight task whose
  next slot arrives while it is still executing is **skipped**, not queued.
- **Context isolation**: the `TaskContext` passed to each task block is a per-execution
  copy of the global context — tasks cannot share mutable state through it accidentally.
- **Retry threading**: retries sleep on the worker thread that ran the first attempt.
  The next *scheduled* slot is re-enqueued before the first attempt begins, so retries
  never delay future runs.
