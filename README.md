# Cleary

[中文文档](README-zh.md)

A lightweight, dependency-minimal task scheduler for the JVM written in Kotlin.
Cleary supports cron expressions, fixed-rate scheduling, one-shot tasks, retry with
exponential backoff, and full concurrency control — all through a clean DSL with no
annotation processing or reflection.

---

## Features

- **Cron scheduling** — Quartz-compatible 6-field expressions with per-task time zones
- **Fixed-rate scheduling** — drift-free intervals anchored to the planned trigger time
- **One-shot execution** — run a task exactly once at a given `Instant`
- **Initial delay** — defer the first execution of any schedule
- **Retry with backoff** — constant or exponential, with a configurable cap
- **Concurrency guard** — overlapping executions are skipped (default) or allowed per task
- **Dynamic task management** — register, disable, enable, and remove tasks at runtime
- **Observable outcomes** — explicit success, failure, skipped, and rejected results
- **Observability hooks** — task lifecycle, retry, skip, reject, and scheduler-error callbacks
- **Shared context** — pass services and values into every task without closures

---

## Installation

**Maven**

```xml
<dependency>
    <groupId>io.github.cymoo</groupId>
    <artifactId>cleary</artifactId>
    <version>0.2.0</version>
</dependency>
```

**Gradle (Kotlin DSL)**

```kotlin
implementation("io.github.cymoo:cleary:0.2.0")
```

Cleary requires **Java 11** or later.

---

## Quick Start

```kotlin
val tasks = TaskScheduler()

tasks.task("heartbeat") {
    every(5.seconds)
    run {
        println("ping at ${Instant.now()}")
    }
}

tasks.task("cleanup") {
    cron("0 0 0 * * ?")   // every day at midnight
    retry(maxAttempts = 3, initialDelay = 1.second, backoffMultiplier = 2.0)
    run {
        println("running nightly cleanup")
    }
}

tasks.start()

// Block the main thread until the JVM shuts down (SIGTERM / CTRL+C).
// Pair with registerShutdownHook = true for a clean exit — no Thread.sleep needed.
tasks.await()
```

---

## Configuration

`TaskScheduler { }` accepts a configuration block:

| Property               | Default                        | Description                                                   |
|------------------------|--------------------------------|---------------------------------------------------------------|
| `concurrency`          | min(32, max(4, CPU cores × 4)) | Worker thread pool size                                       |
| `queueCapacity`        | `10_000`                       | Worker queue capacity before manual/scheduled runs reject     |
| `threadNamePrefix`     | `"task-scheduler"`             | Prefix for all thread names                                   |
| `autoStart`            | `false`                        | Start the scheduler immediately after construction            |
| `registerShutdownHook` | `false`                        | Register a JVM shutdown hook that calls `shutdown()`          |
| `context`              | empty map                      | Key-value pairs injected into every task's execution context  |
| `onTaskStart`          | `null`                         | Callback fired before each execution begins                   |
| `onTaskComplete`       | `null`                         | Callback fired after each execution ends (success or failure) |
| `onRetry`              | `null`                         | Callback fired after each failed attempt when retries remain  |
| `onTaskSkipped`        | `null`                         | Callback fired when overlap protection skips an execution     |
| `onTaskRejected`       | `null`                         | Callback fired when the worker queue rejects an execution     |
| `onSchedulerError`     | `null`                         | Callback fired when a hook or scheduler loop throws           |

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
so accumulated delays never cause drift.

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

### Initial Delay

`initialDelay` can be combined with any schedule and may be declared before or after it:

```kotlin
tasks.task("warmup-then-poll") {
    every(1.minute)
    initialDelay(30.seconds)   // wait 30 s before the first run
    run { poll() }
}
```

For `once(at)`, the delay is added to the one-shot instant. For example,
`once(t)` plus `initialDelay(30.seconds)` fires once at `t + 30 seconds`.

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
- Retries occupy a worker thread during the sleep; size `concurrency` accordingly
- `onRetry` fires after each failed attempt except the last; `onTaskComplete` fires after
  the final outcome (success or exhausted retries)

---

## Concurrency Control

By default, only one execution of a task can run at a time. If the task is still
running when its next slot arrives, that slot is **skipped** and reported as
`TaskRunResult.Skipped` for manual runs and `onTaskSkipped` for all runs.

```kotlin
tasks.task("slow-report") {
    every(1.second)
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
context are not visible to other executions or other tasks.

```kotlin
// Inject shared services at construction time
val tasks = TaskScheduler {
    autoStart = true
    context["db"] = database
    context["mailer"] = emailClient
}

tasks.task("send-digest") {
    cron("0 0 9 * * ?")
    run {
        val db: Database = get("db")
        val mailer: EmailClient = get("mailer")
        val subscribers = db.query("SELECT email FROM users WHERE subscribed = true")
        subscribers.forEach { mailer.send(it, "Digest", buildDigest(db)) }
    }
}
```

Context API:

| Method                         | Description                                 |
|--------------------------------|---------------------------------------------|
| `get<T>("key")`                | Returns the value; throws if absent         |
| `getOrNull<T>("key")`          | Returns the value or `null`                 |
| `getOrDefault("key", default)` | Returns the value or a fallback             |
| `set("key", value)`            | Writes a value (isolated to this execution) |
| `remove("key")`                | Removes a value                             |

---

## Observability

```kotlin
val tasks = TaskScheduler {
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
        logger.info("SKIP  ${event.taskName} reason=${event.reason}")
    }

    onTaskRejected = { event ->
        logger.warn("REJECT ${event.taskName} reason=${event.reason}")
    }

    onSchedulerError = { event ->
        logger.error("SCHEDULER ERROR phase=${event.phase} task=${event.taskName}", event.error)
    }
}
```

Hook failures are isolated: an exception thrown from `onTaskStart`,
`onTaskComplete`, `onRetry`, `onTaskSkipped`, or `onTaskRejected` is reported to
`onSchedulerError` and does not change the task's execution result.

### Event fields

**`TaskStartEvent`**

| Field           | Type                      | Description                                                          |
|-----------------|---------------------------|----------------------------------------------------------------------|
| `taskName`      | `String`                  | Name of the task                                                     |
| `scheduledTime` | `Long`                    | Planned trigger time (epoch ms); equals `actualTime` for manual runs |
| `actualTime`    | `Long`                    | Wall-clock time when execution began (epoch ms)                      |
| `context`       | `MutableMap<String, Any>` | Live context; values added here are visible to the task              |

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
| `nextRetryDelayMs` | `Long`      | Sleep time before the next attempt         |

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

Tasks can be added, paused, resumed, and removed at any time after `start()`:

```kotlin
// Register tasks at any time — even after start()
tasks.task("new-poller") {
    every(10.seconds)
    run { poll() }
}

// Pause without removing
tasks.disable("new-poller")

// Resume
tasks.enable("new-poller")

// Permanently remove
tasks.remove("new-poller")

// Inspect
println(tasks.listTaskNames())
println(tasks.getTaskInfo("new-poller"))
println(tasks.exists("new-poller"))
```

`TaskInfo` includes static metadata (`scheduleDescription`, `allowConcurrent`,
`retryPolicy`) and runtime fields (`activeExecutions`, `running`, next/last
timestamps, last duration/error, and success/failure/skip/reject counters).

---

## Manual Execution

Any registered task (including schedule-less ones) can be triggered manually:

```kotlin
// Fire-and-forget — returns a Future<TaskRunResult>
val future = tasks.run("flush-cache")

// Block until complete — returns Success, Failure, Skipped, or Rejected
when (val result = tasks.runBlocking("flush-cache")) {
    is TaskRunResult.Success -> println("done: ${result.value}")
    is TaskRunResult.Failure -> println("failed: ${result.error.message}")
    is TaskRunResult.Skipped -> println("skipped: ${result.reason}")
    is TaskRunResult.Rejected -> println("rejected: ${result.reason}")
}

// Pass extra context values for this execution only
tasks.runBlocking("generate-report", mapOf("format" to "pdf"))
```

---

## Lifecycle

```kotlin
val tasks = TaskScheduler()   // autoStart = false

// Register tasks before starting
tasks.task("t") { every(Duration.ofSeconds(1)); run { /* … */ } }

// Start the scheduler; all registered tasks are enqueued
tasks.start()

// Check state
println(tasks.isRunning)     // true while scheduler loop is alive
println(tasks.isTerminated)  // true after shutdown() fully completes

// Graceful shutdown — waits up to 30 s for in-flight tasks
tasks.shutdown()

// Immediate shutdown — interrupts running tasks
tasks.shutdown(awaitTermination = false)

// Block the calling thread until shutdown() is invoked.
// Designed for main() — pair with registerShutdownHook = true so that
// SIGTERM / CTRL+C triggers shutdown automatically.
tasks.await()
```

`start()` is idempotent while the scheduler is running, and `shutdown()` is
idempotent. A scheduler is single-use: after shutdown it cannot be restarted,
new tasks cannot be registered, and control operations fail explicitly.
Registering a task after `start()` enqueues it immediately.

---

## Thread Safety

All public methods are thread-safe. The scheduler runs on a single dedicated thread;
task execution happens on a fixed-size worker pool. The `TaskContext` passed to each
task block is a per-execution copy of the global context, so tasks cannot accidentally
share mutable state through the context map.

---

## Dependencies

| Library                    | Purpose                                            |
|----------------------------|----------------------------------------------------|
| `com.cronutils:cron-utils` | Quartz cron parsing and next-execution calculation |
| JDK `java.util.concurrent` | `DelayQueue`, thread pool, atomics                 |

Test dependencies: `org.junit.jupiter` (JUnit 5).

---

## Examples

For a runnable web UI example, see [`examples/task-dashboard`](examples/task-dashboard).
It uses the Colleen web framework to provide a mission-control style dashboard for
running, enabling, disabling, removing, and resetting demo Cleary tasks.

```kotlin
import io.github.cymoo.cleary.*
import java.time.Duration
import java.time.Instant

// =============================================================================
// Example 1 — Quick Start
//
// The simplest possible setup: two scheduled tasks and one manual task.
// =============================================================================

fun quickStart() {
    val tasks = TaskScheduler()

    // Fires every 5 seconds
    tasks.task("heartbeat") {
        every(5.seconds)
        run {
            println("[${taskName}] ping at ${Instant.now()}")
        }
    }

    // Fires every hour, but waits 10 s before the first run so the
    // application has time to fully initialize
    tasks.task("hourly-report") {
        every(1.hour)
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
    val tasks = TaskScheduler { autoStart = true }

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
    val tasks = TaskScheduler { autoStart = true }

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
// Example 4 — Observability
//
// Using lifecycle callbacks for logging, tracing, and alerting.
// =============================================================================

fun observability() {
    val tasks = TaskScheduler {
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

        onRetry = { event ->
            println(
                "[RETRY] ${event.taskName}  " +
                        "attempt=${event.failedAttempts}/${event.maxAttempts}  " +
                        "nextIn=${event.nextRetryDelayMs} ms  " +
                        "error=${event.error.message}"
            )
        }
    }

    tasks.task("work") {
        every(2.seconds)
        retry(maxAttempts = 3, initialDelay = 100.milliseconds)
        run {
            // Access the trace ID that onTaskStart injected
            val traceId = getOrNull<String>("traceId") ?: "n/a"
            println("  [WORK] trace=$traceId — doing something")
            if (Math.random() < 0.4) error("Simulated failure")
        }
    }

    Thread.sleep(8_000)
    tasks.shutdown()
}

// =============================================================================
// Example 5 — Shared Application Context
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

    val tasks = TaskScheduler {
        autoStart = true
        context["db"] = db
        context["email"] = email
    }

    tasks.task("expire-sessions") {
        every(15.minutes)
        run {
            val database: Database = get("db")
            database.execute("DELETE FROM sessions WHERE expires_at < NOW()")
            println("Expired sessions removed")
        }
    }

    tasks.task("weekly-digest") {
        cron("0 0 9 ? * MON")   // Every Monday at 09:00
        run {
            val database: Database = get("db")
            val mailer: EmailClient = get("email")
            val rows = database.query("SELECT user_email FROM subscribers")
            rows.forEach { addr -> mailer.send(addr, "Weekly digest", "Here's your summary.") }
        }
    }

    tasks.await()
}

// =============================================================================
// Example 6 — Concurrency Control
//
// allowConcurrent = false (default): overlapping runs are skipped.
// concurrent(true): parallel runs are permitted.
// =============================================================================

fun concurrencyControl() {
    val tasks = TaskScheduler {
        concurrency = 8
        autoStart = true
    }

    // This task takes longer than its interval. Without the concurrency guard
    // a second instance would overlap the first; instead the slot is skipped.
    tasks.task("slow-report") {
        every(1.second)
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
// Example 7 — One-shot Task
//
// Schedule a task to run exactly once at a specific point in time.
// =============================================================================

fun oneShotTask() {
    val tasks = TaskScheduler { autoStart = true }

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
// Example 8 — Dynamic Task Management
//
// Adding, disabling, enabling, and removing tasks at runtime.
// =============================================================================

fun dynamicTaskManagement() {
    val tasks = TaskScheduler { autoStart = true }

    tasks.task("poller") {
        every(500.milliseconds)
        run { println("Polling…") }
    }

    println("Active tasks: ${tasks.listTaskNames()}")

    // Pause polling temporarily
    tasks.disable("poller")
    println("Poller disabled. Active tasks: ${tasks.listTaskNames()}")
    Thread.sleep(1_500)

    // Resume
    tasks.enable("poller")
    println("Poller re-enabled")
    Thread.sleep(1_000)

    // Permanently remove when no longer needed
    tasks.remove("poller")
    println("Poller removed. Active tasks: ${tasks.listTaskNames()}")

    // Register a brand-new task on the fly
    tasks.task("replacement-poller") {
        every(Duration.ofSeconds(1))
        run { println("Replacement poller running…") }
    }

    Thread.sleep(3_000)
    tasks.shutdown()
}

// =============================================================================
// Example 9 — Long-running Process (await)
//
// In a real application the main thread should not busy-wait or sleep.
// await() blocks until shutdown() is called, and pairs naturally with
// registerShutdownHook so that SIGTERM / CTRL+C triggers a clean exit.
// =============================================================================

fun longRunningProcess() {
    val tasks = TaskScheduler {
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
// Example 10 — Manual Execution with Per-run Context
//
// Extra context values can be supplied at call time and are visible only to
// that single execution — they do not affect the global context or other runs.
// =============================================================================

fun manualExecutionWithContext() {
    val tasks = TaskScheduler { autoStart = true }

    tasks.task("generate-report") {
        // No schedule — manual-only task
        run {
            val format: String = getOrDefault("format", "html")
            val recipient: String = getOrDefault("recipient", "admin@example.com")
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
