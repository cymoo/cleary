# Cleary

[中文文档](README-zh.md)

A lightweight, dependency-minimal task scheduler for the JVM written in Kotlin.
Cleary supports cron expressions, fixed-rate and fixed-delay scheduling, one-shot
tasks, retry with exponential backoff, per-attempt timeouts, and full concurrency
control — all through a clean DSL with no annotation processing or reflection.

---

## Features

- **Cron scheduling** — Quartz-compatible 6-field expressions with per-task time zones
- **Fixed-rate scheduling** — drift-free intervals anchored to the planned trigger time
- **Fixed-delay scheduling** — intervals measured from the previous run's completion
- **One-shot execution** — run a task exactly once at a given `Instant`
- **Custom triggers** — plug in your own `Trigger` for schedules the built-ins can't express
- **Initial delay** — control exactly when the first run happens (including "immediately")
- **Retry with backoff** — constant or exponential, waiting in the scheduler queue, never on a worker thread
- **Timeouts** — interrupt attempts that overrun, with cooperative cancellation via `isCancelled`
- **Misfire policy** — skip missed fire times after system sleep, or catch them all up
- **Concurrency guard** — overlapping executions are skipped (default) or allowed per task
- **Dynamic task management** — register, disable, enable, replace, reschedule, and remove tasks at runtime
- **Built-in web dashboard** — zero-dependency live UI for monitoring and controlling tasks
- **Observable outcomes** — explicit success, failure, skipped, and rejected results
- **Observability hooks** — global and per-task lifecycle callbacks, multicast listeners, plus default logging when no hook is set
- **Tags** — group tasks and filter runtime snapshots with `listTasks(tag)`
- **Shared context** — pass services and values into every task without closures

---

## Installation

**Maven**

```xml
<dependency>
    <groupId>io.github.cymoo</groupId>
    <artifactId>cleary</artifactId>
    <version>0.3.0</version>
</dependency>
```

**Gradle (Kotlin DSL)**

```kotlin
implementation("io.github.cymoo:cleary:0.3.0")
```

Cleary requires **Java 21** or later.

> **Upgrading from 0.2.x?** See [Breaking changes in 0.3.0](#breaking-changes-in-030).

---

## Quick Start

```kotlin
import io.github.cymoo.cleary.*
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

fun main() {
    val tasks = taskScheduler {
        registerShutdownHook = true
    }

    tasks.task("heartbeat") {
        every(5.seconds)
        run {
            println("ping at ${Instant.now()}")
        }
    }

    tasks.task("cleanup") {
        cron("0 0 0 * * ?")   // every day at midnight
        retry(maxAttempts = 3, initialDelay = 1.seconds, backoffMultiplier = 2.0)
        run {
            println("running nightly cleanup")
        }
    }

    tasks.task("flush-cache") {
        run {
            println("manual cache flush")
            "flushed"
        }
    }

    tasks.start()

    val result = tasks.runBlocking("flush-cache")
    println("manual result: $result")

    // Blocks until shutdown() is called. The shutdown hook makes SIGTERM / CTRL+C
    // call shutdown() cleanly, so no Thread.sleep loop is needed.
    tasks.await()
}
```

---

## Durations

Cleary uses **`kotlin.time.Duration`** throughout. The standard library already
provides the readable literals:

```kotlin
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.hours

every(30.seconds)
initialDelay(500.milliseconds)
timeout(2.minutes)
```

Every DSL function also accepts `java.time.Duration`, so Java-style call sites keep
working:

```kotlin
every(Duration.ofSeconds(30))
retry(maxAttempts = 3, initialDelay = Duration.ofMillis(500))
```

---

## Configuration

`taskScheduler { }` (or `TaskScheduler { }`) accepts a configuration block:

| Property               | Default                        | Description                                                   |
|------------------------|--------------------------------|---------------------------------------------------------------|
| `concurrency`          | min(32, max(4, CPU cores × 4)) | Worker thread pool size                                       |
| `queueCapacity`        | `10_000`                       | Worker queue capacity before manual/scheduled runs reject     |
| `threadNamePrefix`     | `"task-scheduler"`             | Prefix for all thread names                                   |
| `autoStart`            | `false`                        | Start the scheduler immediately after construction            |
| `registerShutdownHook` | `false`                        | Register a JVM shutdown hook that calls `shutdown()`          |
| `misfirePolicy`        | `MisfirePolicy.SKIP`           | How missed fire times are handled (see [Misfire policy](#misfire-policy)) |
| `shutdownTimeout`      | `30.seconds`                   | How long `shutdown()` waits for in-flight executions          |
| `context`              | empty map                      | Key-value pairs visible to every task's execution context     |
| `onTaskStart`          | `null`                         | Callback fired before each execution begins                   |
| `onTaskComplete`       | `null`                         | Callback fired after each execution ends (success or failure) |
| `onRetry`              | `null`                         | Callback fired after each failed attempt when retries remain  |
| `onTaskSkipped`        | `null`                         | Callback fired when overlap protection skips an execution     |
| `onTaskRejected`       | `null`                         | Callback fired when the worker queue rejects an execution     |
| `onSchedulerError`     | `null`                         | Callback fired when a hook or scheduler loop throws           |

Hooks may run on scheduler, worker, or caller threads — keep them fast and
non-blocking.

---

## Scheduling

### Fixed-rate

```kotlin
tasks.task("metrics") {
    every(30.seconds)
    run { collectMetrics() }
}
```

The next execution is anchored to the *planned* trigger time, not the wall clock,
so accumulated delays never cause drift. The first run happens one interval after
the scheduler arms the task; use `initialDelay` to change that.

### Fixed-delay

```kotlin
tasks.task("drain-queue") {
    fixedDelay(30.seconds)   // 30 s pause between runs, measured from completion
    run { drain() }
}
```

Unlike `every`, the interval starts counting when the previous run **finishes**
(including its retries), so runs never pile up behind a slow execution.

### Cron

Cleary uses Quartz 6-field cron expressions (`seconds minutes hours day-of-month month day-of-week [year]`).

```kotlin
tasks.task("daily-digest") {
    cron("0 0 8 * * ?")   // every day at 08:00 (system time zone)
    run { sendDigest() }
}

tasks.task("weekday-report") {
    cron("0 0 9 ? * MON-FRI", ZoneId.of("America/New_York"))
    run { generateReport() }
}
```

| Expression          | Meaning                              |
|---------------------|--------------------------------------|
| `0/30 * * * * ?`    | Every 30 seconds                     |
| `0 0/5 * * * ?`     | Every 5 minutes                      |
| `0 0 8 * * ?`       | Every day at 08:00                   |
| `0 0 0 1 * ?`       | First day of every month at midnight |
| `0 0 9 ? * MON-FRI` | Weekdays at 09:00                    |

### Once

```kotlin
tasks.task("scheduled-migration") {
    once(Instant.parse("2025-06-01T02:00:00Z"))
    run { runMigration() }
}
```

A `once` instant in the past fires immediately. After it has fired, re-enabling
the task does not fire it again.

### Custom triggers

When the built-ins can't express a schedule ("last business day of the month"),
implement `Trigger` yourself:

```kotlin
tasks.task("custom-cadence") {
    custom(object : Trigger {
        override fun initialExecutionTime(armTime: Long): Long? = /* first fire */
        override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long? =
            /* next fire strictly after minTime, or null to stop */
    }, description = "my cadence")
    run { work() }
}
```

### Initial delay

`initialDelay` controls when the **first** run happens after each arm (start,
registration, or re-enable). It may be declared before or after the schedule:

```kotlin
tasks.task("warmup-then-poll") {
    every(1.minutes)
    initialDelay(30.seconds)   // first run at ~30 s, then every 1 min
    run { poll() }
}

tasks.task("start-immediately") {
    every(1.hours)
    initialDelay(Duration.ZERO)   // first run right away, then hourly
    run { sync() }
}
```

- With `every` / `fixedDelay`, the first run happens at `now + delay` (replacing
  the default one-interval wait).
- With `cron`, the task fires at the first cron point after `now + delay`.
- With `once(at)`, the task fires once at `at + delay`.

### Misfire policy

If the machine sleeps or the scheduler is saturated, fire times can be missed.
By default Cleary runs the late fire once and **skips ahead** to the next future
fire time (staying on the original grid for fixed-rate schedules). Set
`MisfirePolicy.CATCH_UP` to run every missed slot instead:

```kotlin
val tasks = taskScheduler {
    misfirePolicy = MisfirePolicy.CATCH_UP   // default: MisfirePolicy.SKIP
}
```

---

## Retry

```kotlin
tasks.task("sync") {
    every(5.minutes)
    retry(
        maxAttempts = 4,          // total attempts including the first
        initialDelay = 500.milliseconds,
        backoffMultiplier = 2.0,  // 500 ms → 1 s → 2 s → …
        maxDelay = 30.seconds
    )
    run { syncRemoteData() }
}
```

- **`backoffMultiplier = 1.0`** (default) — constant delay between retries
- **`backoffMultiplier = 2.0`** — exponential backoff
- `maxDelay` caps the computed delay regardless of the multiplier
- Retry waits happen in the scheduler's delay queue — they **never occupy a worker
  thread**, so failing tasks cannot starve the pool
- `InterruptedException` and `Error`s are never retried; the execution fails
  immediately
- `onRetry` fires after each failed attempt except the last; `onTaskComplete` fires
  once with the final outcome (success or exhausted retries)

---

## Timeout

```kotlin
tasks.task("external-call") {
    every(1.minutes)
    timeout(10.seconds)
    run { callSlowService() }
}
```

An attempt that overruns its timeout is interrupted and recorded as a
`TaskRunResult.Failure` carrying `TaskTimeoutException`. Timeouts apply per
attempt and combine with `retry`.

Interruption only works for blocking, interruptible code. CPU-bound tasks should
poll `isCancelled` (which also turns true during shutdown):

```kotlin
run {
    while (!isCancelled && hasMoreWork()) {
        processNextChunk()
    }
}
```

---

## Concurrency Control

By default, only one execution of a task can run at a time. If the task is still
running when its next slot arrives, that slot is **skipped** and reported as
`TaskRunResult.Skipped` for manual runs and `onTaskSkipped` for all runs. The
skip is detected before the worker pool is involved, so a stuck task never fills
the worker queue with doomed submissions.

```kotlin
tasks.task("slow-report") {
    every(1.seconds)
    // concurrent(false) is the default — overlapping executions are skipped
    run {
        Thread.sleep(5_000)   // takes longer than the interval
    }
}
```

Set `concurrent(true)` when the task is stateless and parallel execution is safe:

```kotlin
tasks.task("parallel-ingest") {
    every(200.milliseconds)
    concurrent(true)
    run { processChunk() }
}
```

---

## Task Context

Every task receives a `TaskContext` scoped to its execution. Values written to the
context are not visible to other executions or other tasks; global context values
are layered underneath as read-only defaults (copied lazily, only when the
execution actually writes).

```kotlin
// Inject shared services at construction time
val tasks = taskScheduler {
    autoStart = true
    context["db"] = database
    context["mailer"] = emailClient
}

tasks.task("send-digest") {
    cron("0 0 9 * * ?")
    run {
        val db = require<Database>("db")
        val mailer = require<EmailClient>("mailer")
        val subscribers = db.query("SELECT email FROM users WHERE subscribed = true")
        subscribers.forEach { mailer.send(it, "Digest", buildDigest(db)) }
    }
}
```

Context API:

| Member                         | Description                                                     |
|--------------------------------|-----------------------------------------------------------------|
| `get("key")`                   | Raw value (`Any?`), or `null` when absent                       |
| `getAs<T>("key")`              | Typed value, or `null` when absent **or of another type**       |
| `getOrDefault("key", default)` | Typed value, or the fallback when absent or of another type     |
| `require<T>("key")`            | Typed value; throws with a precise message when absent or wrong |
| `set("key", value)`            | Writes a value (isolated to this execution and its retries)     |
| `remove("key")`                | Removes a value from this execution's view                      |
| `toMap()`                      | Snapshot of all currently visible values                        |
| `taskName`                     | Name of the executing task                                      |
| `isCancelled`                  | True when shutdown was requested or the attempt was interrupted |

The typed accessors are `reified`, so type mismatches are actually detected —
`getAs<String>("count")` on an `Int` value returns `null` instead of blowing up
later with a `ClassCastException`.

---

## Observability

### Global hooks

```kotlin
val tasks = taskScheduler {
    autoStart = true

    onTaskStart = { event ->
        // Inject a trace ID before the task block runs
        event.context["traceId"] = UUID.randomUUID().toString()
        logger.info("START ${event.taskName} scheduled=${event.scheduledTime}")
    }

    onTaskComplete = { event ->
        if (event.isSuccess) {
            logger.info("DONE  ${event.taskName} duration=${event.duration} ms")
        } else {
            logger.error("FAIL  ${event.taskName}", event.error)
            alerting.fire("Task failed: ${event.taskName}")
        }
    }

    onRetry = { event ->
        logger.warn(
            "RETRY ${event.taskName} " +
                    "attempt=${event.failedAttempts}/${event.maxAttempts} " +
                    "nextIn=${event.nextRetryDelayMs} ms"
        )
    }

    onTaskSkipped = { event ->
        logger.info("SKIP  ${event.taskName} type=${event.executionType} reason=${event.reason}")
    }

    onTaskRejected = { event ->
        logger.warn("REJECT ${event.taskName} type=${event.executionType} reason=${event.reason}")
    }

    onSchedulerError = { event ->
        logger.error("SCHEDULER ERROR phase=${event.phase} task=${event.taskName}", event.error)
    }
}
```

Hook failures are isolated: an exception thrown from any hook is reported to
`onSchedulerError` and does not change the task's execution result.

### Per-task hooks

Each task can attach its own hooks; they fire **before** the global counterpart:

```kotlin
tasks.task("payment-sync") {
    every(5.minutes)
    onComplete { event -> paymentMetrics.record(event) }
    onRetry { event -> logger.warn("payment-sync retrying: ${event.error.message}") }
    run { syncPayments() }
}
```

### Listeners

The config hooks are single-slot properties. When several observers need the
same events (metrics, logging, the built-in dashboard), register
`TaskLifecycleListener`s instead — any number can coexist:

```kotlin
tasks.addListener(object : TaskLifecycleListener {
    override fun onTaskComplete(event: TaskCompleteEvent) = metrics.record(event)
    override fun onRetry(event: TaskRetryEvent) = logger.warn("retrying ${event.taskName}")
})
```

Listeners fire after the task-level and global hooks; their failures are
isolated just like hook failures.

### Default logging

When no relevant hook is configured, Cleary logs through the JDK's
`System.Logger` (`io.github.cymoo.cleary`): final task failures at `WARNING`
(unless an `onTaskComplete` hook exists) and internal scheduler errors at
`ERROR` (unless `onSchedulerError` exists). Failures are never silent out of
the box.

### Event fields

**`TaskStartEvent`**

| Field           | Type          | Description                                                          |
|-----------------|---------------|----------------------------------------------------------------------|
| `taskName`      | `String`      | Name of the task                                                     |
| `scheduledTime` | `Long`        | Planned trigger time (epoch ms); equals `actualTime` for manual runs |
| `actualTime`    | `Long`        | Wall-clock time when execution began (epoch ms)                      |
| `context`       | `TaskContext` | Live context; values written here are visible to the task            |

**`TaskCompleteEvent`**

| Field       | Type         | Description                                       |
|-------------|--------------|---------------------------------------------------|
| `taskName`  | `String`     | Name of the task                                  |
| `startTime` | `Long`       | Execution start time (epoch ms)                   |
| `endTime`   | `Long`       | Execution end time (epoch ms)                     |
| `duration`  | `Long`       | `endTime - startTime` (ms)                        |
| `result`    | `Any?`       | Return value of the task block; `null` on failure |
| `error`     | `Throwable?` | Last exception thrown; `null` on success          |
| `isSuccess` | `Boolean`    | `error == null`                                   |

**`TaskRetryEvent`**

| Field              | Type        | Description                                |
|--------------------|-------------|--------------------------------------------|
| `taskName`         | `String`    | Name of the task                           |
| `failedAttempts`   | `Int`       | How many attempts have failed so far (≥ 1) |
| `maxAttempts`      | `Int`       | Total configured attempts                  |
| `error`            | `Throwable` | Exception from the most recent failure     |
| `nextRetryDelayMs` | `Long`      | Wait before the next attempt               |

**`TaskSkippedEvent`**

| Field           | Type                | Description                                             |
|-----------------|---------------------|---------------------------------------------------------|
| `taskName`      | `String`            | Name of the task                                        |
| `scheduledTime` | `Long?`             | Planned trigger time for scheduled runs; `null` manual |
| `skippedAt`     | `Long`              | Wall-clock time when the skip was recorded (epoch ms)  |
| `executionType` | `TaskExecutionType` | `SCHEDULED` or `MANUAL`                                 |
| `reason`        | `TaskSkipReason`    | Currently `ALREADY_RUNNING`                             |

**`TaskRejectedEvent`**

| Field           | Type                 | Description                                             |
|-----------------|----------------------|---------------------------------------------------------|
| `taskName`      | `String`             | Name of the task                                        |
| `scheduledTime` | `Long?`              | Planned trigger time for scheduled runs; `null` manual |
| `rejectedAt`    | `Long`               | Wall-clock time when the rejection occurred (epoch ms) |
| `executionType` | `TaskExecutionType`  | `SCHEDULED` or `MANUAL`                                 |
| `reason`        | `TaskRejectedReason` | Currently `WORKER_QUEUE_FULL`                           |

**`SchedulerErrorEvent`**

| Field      | Type                  | Description                                      |
|------------|-----------------------|--------------------------------------------------|
| `taskName` | `String?`             | Task related to the error, when known            |
| `phase`    | `SchedulerErrorPhase` | Callback or scheduler phase that threw           |
| `error`    | `Throwable`           | The isolated hook or scheduler-loop exception    |

---

## Dynamic Task Management

Tasks can be added, paused, resumed, replaced, and removed at any time after `start()`:

```kotlin
// Register tasks at any time — even after start()
tasks.task("new-poller") {
    every(10.seconds)
    tags("polling", "network")
    run { poll() }
}

// Pause without removing
tasks.disable("new-poller")

// Resume
tasks.enable("new-poller")

// Change the schedule or body in place — stats are preserved
tasks.replace("new-poller") {
    every(30.seconds)
    run { pollV2() }
}

// Change only the schedule — body, settings, and stats are kept
tasks.reschedule("new-poller", Schedule.FixedRate(1.minutes))
tasks.reschedule("new-poller", null)   // make it manual-only

// Inspect
println(tasks.listTaskNames())
println(tasks.listTasks())               // List<TaskInfo> for all tasks
println(tasks.listTasks("polling"))      // only tasks tagged "polling"
println(tasks.getTaskInfo("new-poller"))
println(tasks.exists("new-poller"))

// Permanently remove
tasks.remove("new-poller")
```

- A task can be registered disabled with `enabled(false)` and armed later via
  `enable()`.
- `replace()` keeps the accumulated statistics and (unless `enabled()` is set)
  the current enabled state; in-flight executions of the old definition finish
  normally.
- `TaskInfo` includes static metadata (`scheduleDescription`, `allowConcurrent`,
  `retryPolicy`, `timeout`, `tags`) and runtime fields (`activeExecutions`,
  `running`, next/last timestamps, last duration/error, and
  success/failure/skip/reject counters).

---

## Web Dashboard

Cleary ships a built-in web dashboard implemented as a
[Colleen](https://github.com/cymoo/colleen) application. It shows live
scheduler state (overview counters, a per-task timeline of upcoming fires, and
an activity feed of completions, failures, retries, timeouts, skips, and
rejections) and supports manual runs, pause/resume, removal, and rescheduling
from the browser with live expression preview. Light and dark themes included.

The colleen dependency is **optional** in cleary's POM — add it explicitly to
use the dashboard:

```xml
<dependency>
    <groupId>io.github.cymoo</groupId>
    <artifactId>colleen</artifactId>
    <version>0.5.0</version>
</dependency>
```

**Standalone** — the dashboard runs its own server:

```kotlin
import io.github.cymoo.cleary.dashboard.Dashboard

val dashboard = Dashboard(scheduler).start(port = 8378)
// ... later
dashboard.stop()
```

**Embedded** — mount it into an existing Colleen application as a sub-app:

```kotlin
val app = Colleen()
app.get("/") { "my app" }
app.mount("/tasks", Dashboard(scheduler).app)
app.listen(8000)   // dashboard at http://localhost:8000/tasks/
```

Options:

```kotlin
Dashboard(scheduler) {
    eventHistoryLimit = 300   // activity feed depth
    readOnly = true           // all mutating endpoints answer 403
}
```

The schedule editor accepts `every <duration>` (`every 90s`, `every 1h30m`),
`fixed-delay <duration>`, `once <ISO-8601 instant>`, or a Quartz cron
expression. Edits are applied via `reschedule`, so statistics are preserved.

The server binds to `127.0.0.1` by default and has **no authentication** — to
expose it beyond localhost, front it with an authenticating reverse proxy or
enable `readOnly`.

The page is driven by a small JSON API that can also be used directly
(paths are relative to the mount point):

| Endpoint | Description |
|---|---|
| `GET /api/state?window=SECONDS` | Snapshot: stats, tasks with upcoming fire times, recent events |
| `GET /api/schedule/preview?expr=…` | Validate an expression and project its next fires |
| `POST /api/tasks/{name}/run` · `/pause` · `/resume` · `/remove` | Control actions |
| `POST /api/tasks/{name}/schedule` | Reschedule; body `{"expr": "every 30s"}` |

For a runnable demo, see [`examples/task-dashboard`](examples/task-dashboard).

---

## Manual Execution

Any registered task (including schedule-less ones) can be triggered manually.
Manual runs execute even when the task is disabled, and never affect its schedule.

```kotlin
tasks.task("flush-cache") {
    // No schedule: this task only runs when run() or runBlocking() is called.
    run {
        val reason = getOrDefault("reason", "manual")
        println("cache flushed ($reason)")
        "ok"
    }
}

// Fire-and-forget — returns a CompletableFuture<TaskRunResult>
val future = tasks.run("flush-cache")
println(future.get())

// Block until complete — returns Success, Failure, Skipped, or Rejected
when (val result = tasks.runBlocking("flush-cache")) {
    is TaskRunResult.Success -> println("done: ${result.value}")
    is TaskRunResult.Failure -> println("failed: ${result.error.message}")
    is TaskRunResult.Skipped -> println("skipped: ${result.reason}")
    is TaskRunResult.Rejected -> println("rejected: ${result.reason}")
}

// Pass extra context values for this execution only
tasks.runBlocking("flush-cache", mapOf("reason" to "deploy"))
```

Task-body exceptions are captured as `TaskRunResult.Failure`; scheduler misuse still
fails fast, for example running before `start()` or referencing an unknown task.

---

## Lifecycle

```kotlin
val tasks = taskScheduler {
    registerShutdownHook = true
}

// Register tasks before starting
tasks.task("t") { every(1.seconds); run { /* … */ } }

// Start the scheduler; all registered tasks are armed
tasks.start()

// Check state
println(tasks.isRunning)     // true while scheduler loop is alive
println(tasks.isTerminated)  // true after shutdown() fully completes

// In a main() method, block until another thread or a shutdown hook calls shutdown().
tasks.await()

// From another signal handler, admin endpoint, or test, choose one shutdown mode:
// Graceful shutdown — waits up to shutdownTimeout (default 30 s) for in-flight tasks.
tasks.shutdown()

// Immediate shutdown — interrupts running tasks.
tasks.shutdown(awaitTermination = false)
```

`start()` is idempotent while the scheduler is running, and `shutdown()` is
idempotent. A scheduler is single-use: after shutdown it cannot be restarted,
new tasks cannot be registered, and control operations fail explicitly.
Registering a task after `start()` arms it immediately. Executions waiting on a
retry when shutdown begins are settled promptly as `Failure` with their last
error.

---

## Thread Safety

All public methods are thread-safe. The scheduler runs on a single dedicated thread;
task execution happens on a fixed-size worker pool. The `TaskContext` passed to each
task block is a per-execution view over the global context (copy-on-write), so tasks
cannot accidentally share mutable state through the context map.

---

## Dependencies

| Library                    | Purpose                                            |
|----------------------------|----------------------------------------------------|
| `com.cronutils:cron-utils` | Quartz cron parsing and next-execution calculation |
| JDK `java.util.concurrent` | `DelayQueue`, thread pool, atomics                 |

Test dependencies: `org.junit.jupiter` (JUnit 5).

---

## Breaking changes in 0.3.0

- **Java 21+** is now required (was 11), aligning with the Colleen-based dashboard
  and current LTS.
- **Durations**: the custom `5.seconds` / `1.hour` extension properties were removed.
  Use `kotlin.time.Duration` literals (`import kotlin.time.Duration.Companion.seconds`)
  or the `java.time.Duration` overloads. `RetryPolicy` and `Schedule` now carry
  `kotlin.time.Duration` values.
- **`initialDelay` semantics**: with `every`, the first run now happens at
  `now + delay` (previously `now + interval + delay`). `initialDelay(Duration.ZERO)`
  fires immediately.
- **`TaskContext`**: the erased-generic `get<T>` / `getOrNull<T>` members were
  replaced by `get(key): Any?` plus reified `getAs<T>` / `getOrDefault` /
  `require<T>` extensions that actually check types.
- **`TaskStartEvent.context`** is now a `TaskContext` instead of a `MutableMap`.
- **`Schedule.WithInitialDelay`** was removed; the initial delay is part of the task
  definition, not the schedule tree.
- **Missing run block** now throws `IllegalArgumentException` (was `IllegalStateException`).
- **`run()`** now returns `CompletableFuture<TaskRunResult>` (was `Future`).
- **Misfire behavior**: missed fire times are skipped by default (previously all
  missed slots were replayed). Opt back in with `misfirePolicy = MisfirePolicy.CATCH_UP`.
- **Retry threading**: retries no longer sleep on a worker thread, and
  `InterruptedException` / `Error` are no longer retried.

---

## Examples

For a runnable demo of the built-in web dashboard, see
[`examples/task-dashboard`](examples/task-dashboard): a handful of tasks
exercising fixed-rate, fixed-delay, cron, one-shot, retry, timeout, and
manual-only scheduling, monitored at `http://localhost:8000`.

```kotlin
import io.github.cymoo.cleary.*
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.hours

// =============================================================================
// Example 1 — Quick Start
//
// The simplest possible setup: two scheduled tasks and one manual task.
// =============================================================================

fun quickStart() {
    val tasks = taskScheduler {
        registerShutdownHook = true
    }

    // Fires every 5 seconds
    tasks.task("heartbeat") {
        every(5.seconds)
        run {
            println("[${taskName}] ping at ${Instant.now()}")
        }
    }

    // Fires every hour; the first run happens 10 s after start so the
    // application has time to fully initialize
    tasks.task("hourly-report") {
        every(1.hours)
        initialDelay(10.seconds)
        run {
            println("[${taskName}] generating report…")
        }
    }

    // Manual-only task — no schedule, triggered on demand
    tasks.task("flush-cache") {
        run {
            println("[${taskName}] cache flushed")
        }
    }

    tasks.start()

    // Trigger the manual task explicitly from anywhere in the app
    tasks.runBlocking("flush-cache")

    tasks.await()
}

// =============================================================================
// Example 2 — Cron Scheduling
//
// Using Quartz cron expressions for wall-clock–aligned scheduling.
// =============================================================================

fun cronScheduling() {
    val tasks = taskScheduler { autoStart = true }

    // Every day at midnight
    tasks.task("daily-cleanup") {
        cron("0 0 0 * * ?")
        run { println("Cleaning up stale data…") }
    }

    // Every weekday at 08:00 in the New York time zone
    tasks.task("business-hours-summary") {
        cron("0 0 8 ? * MON-FRI", java.time.ZoneId.of("America/New_York"))
        run { println("Sending morning summary…") }
    }

    // Every 30 seconds (useful for short-interval polling)
    tasks.task("metrics-poll") {
        cron("0/30 * * * * ?")
        run { println("Polling metrics…") }
    }

    tasks.await()
}

// =============================================================================
// Example 3 — Retry with Exponential Backoff
//
// Unreliable tasks that should be retried with increasing delays.
// =============================================================================

fun retryWithBackoff() {
    val tasks = taskScheduler { autoStart = true }

    tasks.task("sync-remote-api") {
        every(5.minutes)

        // Up to 4 total attempts:
        //   attempt 1 fails → wait 500 ms
        //   attempt 2 fails → wait 1 000 ms
        //   attempt 3 fails → wait 2 000 ms  (capped at maxDelay)
        //   attempt 4 — final; if it fails the error is reported
        retry(
            maxAttempts = 4,
            initialDelay = 500.milliseconds,
            backoffMultiplier = 2.0,
            maxDelay = 30.seconds,
        )
        run {
            println("[${taskName}] calling remote API…")
            if (Math.random() < 0.7) error("Transient network error")
            println("[${taskName}] sync succeeded")
        }
    }

    tasks.await()
}

// =============================================================================
// Example 4 — Timeout and Cooperative Cancellation
//
// Interrupt attempts that overrun, and let CPU-bound loops cooperate.
// =============================================================================

fun timeouts() {
    val tasks = taskScheduler { autoStart = true }

    // A blocking call that overruns is interrupted and reported as
    // Failure(TaskTimeoutException); combined with retry it gets another chance.
    tasks.task("external-call") {
        every(1.minutes)
        timeout(10.seconds)
        retry(maxAttempts = 2, initialDelay = 1.seconds)
        run { callSlowService() }
    }

    // CPU-bound loops can't be interrupted — poll isCancelled instead.
    tasks.task("batch-crunch") {
        fixedDelay(5.minutes)
        timeout(2.minutes)
        run {
            while (!isCancelled && hasMoreWork()) {
                processNextChunk()
            }
        }
    }

    tasks.await()
}

// =============================================================================
// Example 5 — Observability
//
// Global hooks plus per-task hooks for logging, tracing, and alerting.
// =============================================================================

fun observability() {
    val tasks = taskScheduler {
        autoStart = true

        onTaskStart = { event ->
            // Inject a per-execution trace ID so the task block can log it
            event.context["traceId"] = java.util.UUID.randomUUID().toString()
            println("[START] ${event.taskName}  trace=${event.context["traceId"]}")
        }

        onTaskComplete = { event ->
            if (event.isSuccess) {
                println("[DONE]  ${event.taskName}  duration=${event.duration} ms")
            } else {
                System.err.println("[FAIL]  ${event.taskName}  error=${event.error?.message}")
                // Here you would send an alert, increment a Prometheus counter, etc.
            }
        }

        onSchedulerError = { event ->
            System.err.println("[HOOK ERROR] phase=${event.phase} task=${event.taskName}: ${event.error.message}")
        }
    }

    tasks.task("work") {
        every(2.seconds)
        retry(maxAttempts = 3, initialDelay = 100.milliseconds)

        // Task-level hooks fire before the global ones
        onRetry { event ->
            println("  [RETRY] attempt=${event.failedAttempts}/${event.maxAttempts}")
        }
        run {
            // Access the trace ID that onTaskStart injected
            val traceId = getAs<String>("traceId") ?: "n/a"
            println("  [WORK] trace=$traceId — doing something")
            if (Math.random() < 0.4) error("Simulated failure")
        }
    }

    Thread.sleep(8_000)
    tasks.shutdown()
}

// =============================================================================
// Example 6 — Shared Application Context
//
// Passing services (databases, caches, etc.) into every task via the global
// context, so tasks do not need to capture them via closure.
// =============================================================================

// Pretend these are real application services
class Database {
    fun query(sql: String): List<String> = listOf("row1", "row2")
    fun execute(sql: String) {
        println("DB: $sql")
    }
}

class EmailClient {
    fun send(to: String, subject: String, body: String) =
        println("Email → $to | $subject")
}

fun sharedContext() {
    val db = Database()
    val email = EmailClient()

    val tasks = taskScheduler {
        autoStart = true
        context["db"] = db
        context["email"] = email
    }

    tasks.task("expire-sessions") {
        every(15.minutes)
        run {
            val database = require<Database>("db")
            database.execute("DELETE FROM sessions WHERE expires_at < NOW()")
            println("Expired sessions removed")
        }
    }

    tasks.task("weekly-digest") {
        cron("0 0 9 ? * MON")   // Every Monday at 09:00
        run {
            val database = require<Database>("db")
            val mailer = require<EmailClient>("email")
            val rows = database.query("SELECT user_email FROM subscribers")
            rows.forEach { addr -> mailer.send(addr, "Weekly digest", "Here's your summary.") }
        }
    }

    tasks.await()
}

// =============================================================================
// Example 7 — Concurrency Control
//
// allowConcurrent = false (default): overlapping runs are skipped.
// concurrent(true): parallel runs are permitted.
// =============================================================================

fun concurrencyControl() {
    val tasks = taskScheduler {
        concurrency = 8
        autoStart = true
    }

    // This task takes longer than its interval. Without the concurrency guard
    // a second instance would overlap the first; instead the slot is skipped.
    tasks.task("slow-report") {
        every(1.seconds)
        // concurrent(false) is the default — no annotation needed
        run {
            println("Report started…")
            Thread.sleep(3_000)   // takes 3 s but fires every 1 s
            println("Report done")
        }
    }

    // This task is stateless and safe to run in parallel
    tasks.task("parallel-ingest") {
        every(200.milliseconds)
        concurrent(true)
        run {
            println("Ingesting chunk on thread ${Thread.currentThread().name}")
            Thread.sleep(500)
        }
    }

    Thread.sleep(5_000)
    tasks.shutdown()
}

// =============================================================================
// Example 8 — One-shot Task
//
// Schedule a task to run exactly once at a specific point in time.
// =============================================================================

fun oneShotTask() {
    val tasks = taskScheduler { autoStart = true }

    tasks.task("scheduled-maintenance") {
        once(Instant.now().plusSeconds(3))
        run {
            println("Running scheduled maintenance at ${Instant.now()}")
        }
    }

    // Optionally run the same task right now as well
    tasks.runBlocking("scheduled-maintenance")   // fires immediately (manual)

    Thread.sleep(5_000)   // wait for the scheduled one-shot to fire too
    tasks.shutdown()
}

// =============================================================================
// Example 9 — Dynamic Task Management
//
// Adding, disabling, enabling, replacing, and removing tasks at runtime.
// =============================================================================

fun dynamicTaskManagement() {
    val tasks = taskScheduler { autoStart = true }

    tasks.task("poller") {
        every(500.milliseconds)
        tags("polling")
        run { println("Polling…") }
    }

    println("Active tasks: ${tasks.listTasks().map { it.name }}")

    // Pause polling temporarily
    tasks.disable("poller")
    Thread.sleep(1_500)

    // Resume
    tasks.enable("poller")
    Thread.sleep(1_000)

    // Slow it down in place — statistics carry over
    tasks.replace("poller") {
        every(2.seconds)
        tags("polling")
        run { println("Polling (slower)…") }
    }
    Thread.sleep(3_000)

    // Permanently remove when no longer needed
    tasks.remove("poller")
    println("Poller removed. Active tasks: ${tasks.listTaskNames()}")

    tasks.shutdown()
}

// =============================================================================
// Example 10 — Long-running Process (await)
//
// In a real application the main thread should not busy-wait or sleep.
// await() blocks until shutdown() is called, and pairs naturally with
// registerShutdownHook so that SIGTERM / CTRL+C triggers a clean exit.
// =============================================================================

fun longRunningProcess() {
    val tasks = taskScheduler {
        autoStart = true
        registerShutdownHook = true
    }

    tasks.task("ping") {
        cron("0/2 * * * * ?")
        run { println("Pong at ${Instant.now()}") }
    }

    tasks.task("health-check") {
        every(30.seconds)
        run { println("Health check OK") }
    }

    // Block here until the JVM receives SIGTERM or CTRL+C.
    // The shutdown hook will call tasks.shutdown(), which releases await().
    tasks.await()
}

// =============================================================================
// Example 11 — Manual Execution with Per-run Context
//
// Extra context values can be supplied at call time and are visible only to
// that single execution — they do not affect the global context or other runs.
// =============================================================================

fun manualExecutionWithContext() {
    val tasks = taskScheduler { autoStart = true }

    tasks.task("generate-report") {
        // No schedule — manual-only task
        run {
            val format = getOrDefault("format", "html")
            val recipient = getOrDefault("recipient", "admin@example.com")
            println("Generating $format report for $recipient")
        }
    }

    // Each call can supply its own context without interfering with others
    tasks.runBlocking("generate-report", mapOf("format" to "pdf", "recipient" to "ceo@example.com"))
    tasks.runBlocking("generate-report", mapOf("format" to "csv"))
    tasks.runBlocking("generate-report")   // falls back to defaults

    tasks.shutdown()
}
```

---

## License

MIT
