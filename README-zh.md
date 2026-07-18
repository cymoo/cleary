# Cleary

[English Documentation](README.md)

Cleary 是一个使用 Kotlin 编写的轻量级 JVM 任务调度器。
它支持 cron 表达式、固定频率与固定间隔调度、一次性任务、重试机制、单次执行超时以及完整的并发控制——所有功能均通过简洁的
DSL 提供，无需注解处理或反射。

---

## 特性

- **Cron 调度** —— 兼容 Quartz 的 6 字段表达式，支持为每个任务单独设置时区
- **固定频率调度（fixed-rate）** —— 基于计划触发时间，无漂移
- **固定间隔调度（fixed-delay）** —— 间隔从上一次执行完成时刻起算
- **一次性执行** —— 在指定 `Instant` 精确执行一次任务
- **自定义 Trigger** —— 内置调度无法表达时可插入自己的 `Trigger` 实现
- **初始延迟** —— 精确控制首次执行时间（包括"立即执行"）
- **重试机制（退避）** —— 固定或指数退避；重试等待发生在调度队列中，不占用 worker 线程
- **超时** —— 中断超时的执行，并支持通过 `isCancelled` 协作式取消
- **Misfire 策略** —— 系统休眠后默认跳过错过的触发点，也可选择全部补跑
- **并发保护** —— 默认跳过重叠执行（可按任务启用并发）
- **动态任务管理** —— 支持运行时注册、禁用、启用、替换、改排程和删除任务
- **内置 Web Dashboard** —— 零依赖的实时监控与管理界面
- **显式执行结果** —— 区分成功、失败、跳过和拒绝
- **可观测性钩子** —— 全局与任务级生命周期回调、多播监听器；未配置钩子时有默认日志兜底
- **标签** —— 任务分组，`listTasks(tag)` 过滤运行时快照
- **共享上下文** —— 无需闭包即可向任务注入服务或数据

---

## 安装

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

Cleary 需要 **Java 11** 或更高版本。

> **从 0.2.x 升级？** 请阅读 [0.3.0 破坏性变更](#030-破坏性变更)。

---

## 快速开始

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
        cron("0 0 0 * * ?")   // 每天午夜执行
        retry(maxAttempts = 3, initialDelay = 1.seconds, backoffMultiplier = 2.0)
        run {
            println("running nightly cleanup")
        }
    }

    tasks.start()

    // 阻塞主线程直到 JVM 关闭（SIGTERM / CTRL+C）。
    // 配合 registerShutdownHook = true 可实现优雅退出，无需 Thread.sleep。
    tasks.await()
}
```

---

## 时长（Duration）

Cleary 全面使用 **`kotlin.time.Duration`**，标准库自带可读的字面量写法：

```kotlin
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes

every(30.seconds)
initialDelay(500.milliseconds)
timeout(2.minutes)
```

所有 DSL 函数同时提供 `java.time.Duration` 重载：

```kotlin
every(Duration.ofSeconds(30))
retry(maxAttempts = 3, initialDelay = Duration.ofMillis(500))
```

---

## 配置

`taskScheduler { }`（或 `TaskScheduler { }`）接受配置块：

| 属性                     | 默认值                            | 描述                                     |
|------------------------|--------------------------------|----------------------------------------|
| `concurrency`          | min(32, max(4, CPU cores × 4)) | 工作线程池大小                                |
| `queueCapacity`        | `10_000`                       | worker 队列容量，满后会拒绝执行请求                  |
| `threadNamePrefix`     | `"task-scheduler"`             | 所有线程名称前缀                               |
| `autoStart`            | `false`                        | 构造后立即启动调度器                             |
| `registerShutdownHook` | `false`                        | 注册 JVM shutdown hook 自动调用 `shutdown()` |
| `misfirePolicy`        | `MisfirePolicy.SKIP`           | 错过的触发点如何处理（见 [Misfire 策略](#misfire-策略)） |
| `shutdownTimeout`      | `30.seconds`                   | `shutdown()` 等待在途执行的最长时间               |
| `context`              | 空 map                          | 对所有任务执行上下文可见的键值对                       |
| `onTaskStart`          | `null`                         | 每次执行开始前触发                              |
| `onTaskComplete`       | `null`                         | 每次执行结束后触发（成功或失败）                       |
| `onRetry`              | `null`                         | 每次失败且仍有重试机会时触发                         |
| `onTaskSkipped`        | `null`                         | 并发保护跳过一次执行时触发                          |
| `onTaskRejected`       | `null`                         | worker 队列拒绝一次执行时触发                     |
| `onSchedulerError`     | `null`                         | 钩子或调度循环抛异常时触发                          |

钩子可能在调度线程、worker 线程或调用方线程上执行——请保持快速、不阻塞。

---

## 调度方式

### 固定频率（fixed-rate）

```kotlin
tasks.task("metrics") {
    every(30.seconds)
    run { collectMetrics() }
}
```

下一次执行基于"计划时间"，而不是实际执行完成时间，因此不会因延迟产生漂移。
首次执行发生在任务被调度后的一个 interval 之后，可用 `initialDelay` 改变。

### 固定间隔（fixed-delay）

```kotlin
tasks.task("drain-queue") {
    fixedDelay(30.seconds)   // 每次执行完成后再等 30 秒
    run { drain() }
}
```

与 `every` 不同，间隔从上一次执行**完成**时（含重试）起算，慢任务不会堆积。

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

| 表达式                 | 含义        |
|---------------------|-----------|
| `0/30 * * * * ?`    | 每 30 秒    |
| `0 0/5 * * * ?`     | 每 5 分钟    |
| `0 0 8 * * ?`       | 每天 08:00  |
| `0 0 0 1 * ?`       | 每月 1 日午夜  |
| `0 0 9 ? * MON-FRI` | 工作日 09:00 |

### 一次性任务

```kotlin
tasks.task("scheduled-migration") {
    once(Instant.parse("2025-06-01T02:00:00Z"))
    run { runMigration() }
}
```

`once` 的时间点已经过去时会立即执行；触发过之后再 `enable()` 不会再次执行。

### 自定义 Trigger

内置调度无法表达的场景（如"每月最后一个工作日"）可以自己实现 `Trigger`：

```kotlin
tasks.task("custom-cadence") {
    custom(object : Trigger {
        override fun initialExecutionTime(armTime: Long): Long? = /* 首次触发时间 */
        override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long? =
            /* 严格大于 minTime 的下次触发时间；返回 null 终止 */
    }, description = "my cadence")
    run { work() }
}
```

### 初始延迟

`initialDelay` 控制每次 arm（启动、注册、重新启用）后的**首次**执行时间，
声明顺序不限：

```kotlin
tasks.task("warmup-then-poll") {
    every(1.minutes)
    initialDelay(30.seconds)   // 首次约在 30 秒后，之后每分钟一次
    run { poll() }
}

tasks.task("start-immediately") {
    every(1.hours)
    initialDelay(Duration.ZERO)   // 立即执行首次，之后每小时一次
    run { sync() }
}
```

- 搭配 `every` / `fixedDelay`：首次执行在 `now + delay`（取代默认的等一个 interval）
- 搭配 `cron`：在 `now + delay` 之后的第一个 cron 时间点触发
- 搭配 `once(at)`：在 `at + delay` 执行一次

### Misfire 策略

机器休眠或调度积压会导致错过触发点。默认策略是把迟到的触发执行一次，然后**跳到
未来的下一个触发点**（fixed-rate 会保持原网格）。如需补跑全部错过的触发点：

```kotlin
val tasks = taskScheduler {
    misfirePolicy = MisfirePolicy.CATCH_UP   // 默认为 MisfirePolicy.SKIP
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
* 重试等待发生在调度器的延迟队列中，**不占用 worker 线程**，失败任务不会拖垮线程池
* `InterruptedException` 和 `Error` 不会重试，立即以失败结束
* `onRetry` 在每次失败后触发（最后一次除外）；`onTaskComplete` 在最终结果后触发一次

---

## 超时

```kotlin
tasks.task("external-call") {
    every(1.minutes)
    timeout(10.seconds)
    run { callSlowService() }
}
```

超时的执行会被中断，并以携带 `TaskTimeoutException` 的 `TaskRunResult.Failure`
记录。超时按单次尝试计算，可与 `retry` 组合。

中断只对可中断的阻塞代码生效；CPU 密集型任务应轮询 `isCancelled`（shutdown 时同样为 true）：

```kotlin
run {
    while (!isCancelled && hasMoreWork()) {
        processNextChunk()
    }
}
```

---

## 并发控制

默认情况下，同一任务不会并发执行。如果任务尚未完成，下一个执行周期将被跳过；手动执行会返回
`TaskRunResult.Skipped`，所有执行都会触发 `onTaskSkipped`。跳过在进入 worker
线程池**之前**就被识别，卡住的任务不会用注定跳过的提交塞满 worker 队列。

```kotlin
tasks.task("slow-report") {
    every(1.seconds)
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

每个任务执行都有独立的 `TaskContext`；写入的值只对当前执行（含其重试）可见。
全局上下文作为只读默认值分层在下面（copy-on-write，只有真正写入时才复制）。

```kotlin
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
    }
}
```

API：

| 成员                             | 描述                             |
|--------------------------------|--------------------------------|
| `get("key")`                   | 原始值（`Any?`），不存在返回 null         |
| `getAs<T>("key")`              | 带类型的值；不存在**或类型不符**返回 null      |
| `getOrDefault("key", default)` | 带类型的值；不存在或类型不符返回默认值            |
| `require<T>("key")`            | 带类型的值；不存在或类型不符抛出带明确信息的异常       |
| `set("key", value)`            | 写入值（仅当前执行及其重试可见）               |
| `remove("key")`                | 从当前执行视图中删除值                    |
| `toMap()`                      | 当前可见值的快照                       |
| `taskName`                     | 当前任务名                          |
| `isCancelled`                  | shutdown 请求或当前尝试被中断时为 true     |

类型化访问器是 `reified` 的，类型不匹配能真正被检测到——`getAs<String>("count")`
在值是 `Int` 时返回 `null`，而不是留到后面抛 `ClassCastException`。

---

## 可观测性

### 全局钩子

```kotlin
val tasks = taskScheduler {
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

    onTaskSkipped = { event ->
        logger.info("SKIP ${event.taskName}: ${event.reason}")
    }

    onTaskRejected = { event ->
        logger.warn("REJECT ${event.taskName}: ${event.reason}")
    }

    onSchedulerError = { event ->
        logger.error("SCHEDULER ERROR ${event.phase}", event.error)
    }
}
```

钩子异常会被隔离：任何钩子抛出的异常会报告给 `onSchedulerError`，
不会改变任务本身的执行结果。

### 任务级钩子

每个任务可附加自己的钩子，在同名全局钩子**之前**触发：

```kotlin
tasks.task("payment-sync") {
    every(5.minutes)
    onComplete { event -> paymentMetrics.record(event) }
    onRetry { event -> logger.warn("payment-sync retrying: ${event.error.message}") }
    run { syncPayments() }
}
```

### 监听器

config 钩子是单槽属性；当多个观察方需要同一批事件（指标、日志、内置 dashboard）时，
注册 `TaskLifecycleListener`——数量不限：

```kotlin
tasks.addListener(object : TaskLifecycleListener {
    override fun onTaskComplete(event: TaskCompleteEvent) = metrics.record(event)
})
```

监听器在任务级和全局钩子之后触发，异常同样被隔离。

### 默认日志

未配置相应钩子时，Cleary 通过 JDK `System.Logger`（logger 名
`io.github.cymoo.cleary`）兜底：任务最终失败记 `WARNING`（除非配置了
`onTaskComplete`），调度器内部错误记 `ERROR`（除非配置了
`onSchedulerError`）。开箱即用，失败不会静默。

---

## 动态任务管理

```kotlin
tasks.task("new-poller") {
    every(10.seconds)
    tags("polling", "network")
    run { poll() }
}

tasks.disable("new-poller")
tasks.enable("new-poller")

// 原地替换 schedule 或任务体——统计数据保留
tasks.replace("new-poller") {
    every(30.seconds)
    run { pollV2() }
}

// 只换 schedule——任务体、设置和统计全部保留
tasks.reschedule("new-poller", Schedule.FixedRate(1.minutes))
tasks.reschedule("new-poller", null)   // 变为仅手动触发

println(tasks.listTaskNames())
println(tasks.listTasks())               // 全部任务的 List<TaskInfo>
println(tasks.listTasks("polling"))      // 仅带 "polling" 标签的任务
println(tasks.getTaskInfo("new-poller"))
println(tasks.exists("new-poller"))

tasks.remove("new-poller")
```

* `enabled(false)` 可以注册但先不启用，之后用 `enable()` 启动
* `replace()` 保留累计统计和（除非显式 `enabled()`）当前启用状态；旧定义的在途执行会正常结束
* `TaskInfo` 同时包含静态元数据（`scheduleDescription`、`allowConcurrent`、
  `retryPolicy`、`timeout`、`tags`）和运行时字段（`activeExecutions`、`running`、
  下一次/上一次时间、最近耗时/错误，以及成功、失败、跳过、拒绝计数）

---

## Web Dashboard

Cleary 内置了一个 Web 管理界面，由 JDK 自带的 HTTP 服务器提供——零额外依赖。
它实时展示调度器状态（概览计数、每个任务未来触发点的时间轴、以及完成/失败/重试/
超时/跳过/拒绝的活动流），并支持在浏览器中手动运行、暂停/恢复、删除和修改排程
（带实时表达式预览）。支持明暗主题。

```kotlin
import io.github.cymoo.cleary.dashboard.Dashboard

val dashboard = Dashboard(scheduler).start(port = 8378)
println("Dashboard at http://localhost:${dashboard.port}")
// ...
dashboard.stop()
```

配置项：

```kotlin
Dashboard(scheduler) {
    eventHistoryLimit = 300   // 活动流保留条数
    readOnly = true           // 所有修改类端点返回 403
}.start(port = 0)             // port 0 绑定随机端口，从 dashboard.port 读取
```

排程编辑器接受 `every <时长>`（`every 90s`、`every 1h30m`）、`fixed-delay <时长>`、
`once <ISO-8601 时刻>` 或 Quartz cron 表达式；修改通过 `reschedule` 应用，统计数据保留。

服务器默认绑定 `127.0.0.1` 且**无鉴权**——如需对外暴露，请置于带鉴权的反向代理之后，
或开启 `readOnly`。

页面背后是一个也可直接调用的 JSON API：

| 端点 | 说明 |
|---|---|
| `GET /api/state?window=秒数` | 全量快照：统计、任务（含未来触发点）、最近事件 |
| `GET /api/schedule/preview?expr=…` | 校验表达式并预测接下来的触发时间 |
| `POST /api/tasks/{name}/run` · `/pause` · `/resume` · `/remove` | 控制操作 |
| `POST /api/tasks/{name}/schedule` | 改排程；请求体 `{"expr": "every 30s"}` |

可运行的演示见 [`examples/task-dashboard`](examples/task-dashboard)。

---

## 手动执行

任何已注册任务（包括无 schedule 的）都可以手动触发。手动执行不受 disable
影响，也不会改变任务的调度计划。

```kotlin
// 异步执行 —— 返回 CompletableFuture<TaskRunResult>
val future = tasks.run("flush-cache")

when (val result = tasks.runBlocking("flush-cache")) {
    is TaskRunResult.Success -> println("done: ${result.value}")
    is TaskRunResult.Failure -> println("failed: ${result.error.message}")
    is TaskRunResult.Skipped -> println("skipped: ${result.reason}")
    is TaskRunResult.Rejected -> println("rejected: ${result.reason}")
}

tasks.runBlocking("generate-report", mapOf("format" to "pdf"))
```

---

## 生命周期

```kotlin
val tasks = taskScheduler()

tasks.task("t") {
    every(1.seconds)
    run { }
}

tasks.start()

println(tasks.isRunning)
println(tasks.isTerminated)

// 优雅关闭 —— 最多等待 shutdownTimeout（默认 30 秒）
tasks.shutdown()

// 立即关闭 —— 中断在途任务
tasks.shutdown(awaitTermination = false)

tasks.await()
```

说明：

* `start()` 在运行中重复调用是幂等的，`shutdown()` 也是幂等的
* 调度器是单次使用的：shutdown 后不能 restart，不能再注册新任务，控制操作会明确失败
* start 后注册的任务会立即加入调度
* shutdown 时仍在等待重试的执行会立即以最后一次错误结算为 `Failure`

---

## 线程安全

* 所有 public 方法线程安全
* 调度器使用单独线程运行；任务执行在固定大小线程池中
* 每次执行的 `TaskContext` 是全局上下文之上的 copy-on-write 视图，任务间不会
  通过上下文意外共享可变状态

---

## 依赖

| 库                    | 用途      |
|----------------------|---------|
| cron-utils           | cron 解析 |
| java.util.concurrent | 并发调度    |

测试依赖：JUnit 5。

---

## 0.3.0 破坏性变更

- **时长**：自定义的 `5.seconds` / `1.hour` 扩展属性已删除。请使用
  `kotlin.time.Duration` 字面量（`import kotlin.time.Duration.Companion.seconds`）
  或 `java.time.Duration` 重载。`RetryPolicy` 与 `Schedule` 现在持有
  `kotlin.time.Duration`。
- **`initialDelay` 语义**：搭配 `every` 时首次执行在 `now + delay`（此前是
  `now + interval + delay`）。`initialDelay(Duration.ZERO)` 立即执行首次。
- **`TaskContext`**：擦除泛型的 `get<T>` / `getOrNull<T>` 成员被替换为
  `get(key): Any?` 加 reified 的 `getAs<T>` / `getOrDefault` / `require<T>`
  扩展，类型检查真正生效。
- **`TaskStartEvent.context`** 类型由 `MutableMap` 改为 `TaskContext`。
- **`Schedule.WithInitialDelay`** 已删除；初始延迟属于任务定义，不再是 schedule 包装。
- **缺少 run 块**现在抛 `IllegalArgumentException`（此前是 `IllegalStateException`）。
- **`run()`** 返回 `CompletableFuture<TaskRunResult>`（此前是 `Future`）。
- **Misfire 行为**：错过的触发点默认跳过（此前会全部补跑）。可通过
  `misfirePolicy = MisfirePolicy.CATCH_UP` 恢复旧行为。
- **重试线程模型**：重试不再睡在 worker 线程上；`InterruptedException` / `Error`
  不再重试。

---

## 常见示例

内置 Web Dashboard 的可运行演示见 [`examples/task-dashboard`](examples/task-dashboard)：
一组覆盖固定频率、固定间隔、cron、一次性、重试、超时和手动任务的示例，
在 `http://localhost:8000` 监控管理。

更多可直接复制的示例（快速开始、cron、重试退避、超时与协作取消、可观测性、共享上下文、
并发控制、一次性任务、动态管理、`await()` 长驻进程、手动执行传参）见
[英文 README 的 Examples 章节](README.md#examples)，代码完全相同。

---

## License

MIT
