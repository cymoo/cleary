# Cleary

[English Documentation](README.md)

Cleary 是一个使用 Kotlin 编写的轻量级的 JVM 任务调度器。  
它支持 cron 表达式、固定频率调度、一次性任务、重试机制以及完整的并发控制——所有功能均通过简洁的 DSL
提供，无需注解处理或反射。

---

## 特性

- **Cron 调度** —— 兼容 Quartz 的 6 字段表达式，支持为每个任务单独设置时区
- **固定频率调度** —— 基于计划触发时间，无漂移
- **一次性执行** —— 在指定 `Instant` 精确执行一次任务
- **初始延迟** —— 延迟首次执行时间
- **重试机制（退避）** —— 支持固定或指数退避，并可配置最大延迟
- **并发保护** —— 默认跳过重叠执行（可按任务启用并发）
- **动态任务管理** —— 支持运行时注册、禁用、启用和删除任务
- **可观测性钩子** —— 提供 `onTaskStart`、`onTaskComplete`、`onRetry` 回调
- **共享上下文** —— 无需闭包即可向任务注入服务或数据

---

## 安装

**Maven**

```xml
<dependency>
    <groupId>io.github.cymoo</groupId>
    <artifactId>cleary</artifactId>
    <version>0.1.0</version>
</dependency>
```

**Gradle (Kotlin DSL)**

```kotlin
implementation("io.github.cymoo:cleary:0.1.0")
```

Cleary 需要 **Java 11** 或更高版本。

---

## 快速开始

```kotlin
val tasks = TaskScheduler()

tasks.task("heartbeat") {
    every(5.seconds)
    run {
        println("ping at ${Instant.now()}")
    }
}

tasks.task("cleanup") {
    cron("0 0 0 * * ?")   // 每天午夜执行
    retry(maxAttempts = 3, initialDelay = 1.second, backoffMultiplier = 2.0)
    run {
        println("running nightly cleanup")
    }
}

tasks.start()

// 阻塞主线程直到 JVM 关闭（SIGTERM / CTRL+C）。
// 配合 registerShutdownHook = true 可实现优雅退出，无需 Thread.sleep。
tasks.await()
```

---

## 配置

`TaskScheduler { }` 接受配置块：

| 属性                     | 默认值                            | 描述                                     |
|------------------------|--------------------------------|----------------------------------------|
| `concurrency`          | min(32, max(4, CPU cores × 4)) | 工作线程池大小                                |
| `threadNamePrefix`     | `"task-scheduler"`             | 所有线程名称前缀                               |
| `autoStart`            | `false`                        | 构造后立即启动调度器                             |
| `registerShutdownHook` | `false`                        | 注册 JVM shutdown hook 自动调用 `shutdown()` |
| `context`              | 空 map                          | 注入到每个任务执行上下文的键值对                       |
| `onTaskStart`          | `null`                         | 每次执行开始前触发                              |
| `onTaskComplete`       | `null`                         | 每次执行结束后触发（成功或失败）                       |
| `onRetry`              | `null`                         | 每次失败且仍有重试机会时触发                         |

---

## 调度方式

### 固定频率

```kotlin
tasks.task("metrics") {
    every(30.seconds)
    run { collectMetrics() }
}
```

下一次执行基于“计划时间”，而不是实际执行完成时间，因此不会因延迟产生漂移。

---

### Cron

Cleary 使用 Quartz 6 字段 cron 表达式：

```
seconds minutes hours day-of-month month day-of-week [year]
```

```kotlin
tasks.task("daily-digest") {
    cron("0 0 8 * * ?")   // 每天 08:00 执行（系统时区）
    run { sendDigest() }
}

tasks.task("weekday-report") {
    cron("0 0 9 ? * MON-FRI", ZoneId.of("America/New_York"))
    run { generateReport() }
}
```

示例：

| 表达式                 | 含义        |
|---------------------|-----------|
| `0/30 * * * * ?`    | 每 30 秒    |
| `0 0/5 * * * ?`     | 每 5 分钟    |
| `0 0 8 * * ?`       | 每天 08:00  |
| `0 0 0 1 * ?`       | 每月 1 日午夜  |
| `0 0 9 ? * MON-FRI` | 工作日 09:00 |

---

### 一次性任务

```kotlin
tasks.task("scheduled-migration") {
    once(Instant.parse("2025-06-01T02:00:00Z"))
    run { runMigration() }
}
```

---

### 初始延迟

可与任何调度组合使用：

```kotlin
tasks.task("warmup-then-poll") {
    every(1.minute)
    initialDelay(30.seconds)
    run { poll() }
}
```

---

## 重试机制

```kotlin
tasks.task("sync") {
    every(5.minutes)
    retry(
        maxAttempts = 4,
        initialDelay = 500.milliseconds,
        backoffMultiplier = 2.0,
        maxDelay = 30.seconds
    )
    run { syncRemoteData() }
}
```

说明：

* `backoffMultiplier = 1.0` —— 固定间隔重试
* `backoffMultiplier = 2.0` —— 指数退避
* `maxDelay` 限制最大延迟
* 重试期间占用 worker 线程
* `onRetry` 在每次失败后触发（最后一次除外）
* `onTaskComplete` 在最终结果后触发

---

## 并发控制

默认情况下，同一任务不会并发执行。

如果任务尚未完成，下一个执行周期将被跳过：

```kotlin
tasks.task("slow-report") {
    every(1.second)
    run {
        Thread.sleep(5_000)
    }
}
```

允许并发执行：

```kotlin
tasks.task("parallel-ingest") {
    every(200.milliseconds)
    concurrent(true)
    run { processChunk() }
}
```

---

## 任务上下文

每个任务都有独立的 `TaskContext`。

```kotlin
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
    }
}
```

API：

| 方法                             | 描述           |
|--------------------------------|--------------|
| `get<T>("key")`                | 获取值，不存在则抛异常  |
| `getOrNull<T>("key")`          | 获取值或返回 null  |
| `getOrDefault("key", default)` | 获取值或默认值      |
| `set("key", value)`            | 写入值（仅当前执行可见） |
| `remove("key")`                | 删除值          |

---

## 可观测性

```kotlin
val tasks = TaskScheduler {
    autoStart = true

    onTaskStart = { event ->
        event.context["traceId"] = UUID.randomUUID().toString()
        logger.info("START ${event.taskName}")
    }

    onTaskComplete = { event ->
        if (event.isSuccess) {
            logger.info("DONE ${event.taskName}")
        } else {
            logger.error("FAIL ${event.taskName}", event.error)
        }
    }

    onRetry = { event ->
        logger.warn("RETRY ${event.taskName}")
    }
}
```

---

## 动态任务管理

```kotlin
tasks.task("new-poller") {
    every(10.seconds)
    run { poll() }
}

tasks.disable("new-poller")
tasks.enable("new-poller")
tasks.remove("new-poller")

println(tasks.listTaskNames())
println(tasks.exists("new-poller"))
```

---

## 手动执行

```kotlin
val future = tasks.run("flush-cache")

tasks.runBlocking("flush-cache")

tasks.runBlocking("generate-report", mapOf("format" to "pdf"))
```

---

## 生命周期

```kotlin
val tasks = TaskScheduler()

tasks.task("t") {
    every(Duration.ofSeconds(1))
    run { }
}

tasks.start()

println(tasks.isRunning)
println(tasks.isTerminated)

tasks.shutdown()

tasks.shutdown(awaitTermination = false)

tasks.await()
```

说明：

* `start()` 和 `shutdown()` 是幂等的
* start 后注册的任务会立即加入调度

---

## 线程安全

* 所有 public 方法线程安全
* 调度器使用单独线程运行
* 任务执行在固定大小线程池中
* 每次执行都有独立 TaskContext 副本

---

## 依赖

| 库                    | 用途      |
|----------------------|---------|
| cron-utils           | cron 解析 |
| java.util.concurrent | 并发调度    |

测试依赖：

JUnit 5

---

## 常见示例

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
