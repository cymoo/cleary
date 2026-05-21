import io.github.cymoo.cleary.RetryPolicy
import io.github.cymoo.cleary.TaskCompleteEvent
import io.github.cymoo.cleary.TaskInfo
import io.github.cymoo.cleary.TaskRejectedEvent
import io.github.cymoo.cleary.TaskRetryEvent
import io.github.cymoo.cleary.TaskScheduler
import io.github.cymoo.cleary.TaskSkippedEvent
import io.github.cymoo.cleary.TaskStartEvent
import io.github.cymoo.colleen.Colleen
import io.github.cymoo.colleen.Forbidden
import io.github.cymoo.colleen.NotFound
import io.github.cymoo.colleen.Path
import io.github.cymoo.colleen.Query
import io.github.cymoo.colleen.Result
import java.util.ArrayDeque
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.math.roundToLong

private const val DEFAULT_HISTORY_LIMIT = 500
private const val MAX_HISTORY_LIMIT = 100_000

/** Runtime options for the reusable task dashboard example service. */
data class TaskDashboardOptions(
    val historyLimit: Int = DEFAULT_HISTORY_LIMIT,
    val allowReset: Boolean = false,
    val concurrency: Int = 6,
    val threadNamePrefix: String = "task-dashboard"
) {
    init {
        require(historyLimit in 1..MAX_HISTORY_LIMIT) {
            "historyLimit must be between 1 and $MAX_HISTORY_LIMIT, got: $historyLimit"
        }
        require(concurrency > 0) { "concurrency must be positive, got: $concurrency" }
        require(threadNamePrefix.isNotBlank()) { "threadNamePrefix cannot be blank" }
    }

    companion object {
        /** Builds options from system properties or environment variables. */
        fun fromEnvironment(
            allowReset: Boolean = false,
            concurrency: Int = 6,
            threadNamePrefix: String = "task-dashboard"
        ): TaskDashboardOptions =
            TaskDashboardOptions(
                historyLimit = readHistoryLimit(),
                allowReset = allowReset,
                concurrency = concurrency,
                threadNamePrefix = threadNamePrefix
            )

        private fun readHistoryLimit(): Int {
            val raw = System.getProperty("taskDashboard.historyLimit")
                ?: System.getenv("TASK_DASHBOARD_HISTORY_LIMIT")
                ?: return DEFAULT_HISTORY_LIMIT
            return raw.toIntOrNull()?.takeIf { it in 1..MAX_HISTORY_LIMIT }
                ?: error("Invalid task dashboard history limit: '$raw'")
        }
    }
}

/** Human-facing metadata for a task registered by the hosting application. */
data class DashboardTaskDescriptor(
    val name: String,
    val description: String = "",
    val group: String = "default"
)

/** Aggregate scheduler health and counters rendered by the dashboard homepage. */
data class OverviewResponse(
    val schedulerRunning: Boolean,
    val health: String,
    val taskCount: Int,
    val enabledCount: Int,
    val disabledCount: Int,
    val runningCount: Int,
    val removedCount: Int,
    val totalRuns: Long,
    val totalSuccesses: Long,
    val totalFailures: Long,
    val totalRetries: Long,
    val totalSkipped: Long,
    val totalRejected: Long,
    val successRate: Double,
    val averageDurationMs: Long?,
    val historyRetained: Int,
    val historyCapacity: Int,
    val oldestEventSeq: Long?,
    val latestEventSeq: Long,
    val generation: Long,
    val uptimeMs: Long,
    val generatedAt: Long
)

/** Per-task runtime snapshot used by the list and detail pages. */
data class TaskSnapshot(
    val name: String,
    val description: String,
    val group: String,
    val scheduleDescription: String?,
    val manualOnly: Boolean,
    val enabled: Boolean,
    val allowConcurrent: Boolean,
    val status: String,
    val retry: RetryPolicySnapshot?,
    val activeExecutions: Int,
    val nextScheduledAt: Long?,
    val runCount: Long,
    val successCount: Long,
    val failureCount: Long,
    val skipCount: Long,
    val rejectedCount: Long,
    val retryCount: Long,
    val totalDurationMs: Long,
    val averageDurationMs: Long?,
    val successRate: Double,
    val lastStartedAt: Long?,
    val lastCompletedAt: Long?,
    val lastRunAgeMs: Long?,
    val lastDurationMs: Long?,
    val lastError: String?,
    val lastResult: String?
)

/** Serializable view of Cleary's retry policy. */
data class RetryPolicySnapshot(
    val maxAttempts: Int,
    val initialDelayMs: Long,
    val backoffMultiplier: Double,
    val maxDelayMs: Long
)

/** Event row retained in the dashboard's bounded in-memory history. */
data class DashboardEvent(
    val seq: Long,
    val timestamp: Long,
    val type: String,
    val status: String,
    val taskName: String?,
    val message: String,
    val detail: String? = null
)

/** Bounded event feed returned by live events and history endpoints. */
data class EventFeed(
    val events: List<DashboardEvent>,
    val latestSeq: Long,
    val oldestSeq: Long?,
    val capacity: Int,
    val truncated: Boolean,
    val generatedAt: Long = System.currentTimeMillis()
)

/** Response returned by dashboard control actions. */
data class ActionResponse(
    val ok: Boolean = true,
    val action: String,
    val taskName: String? = null,
    val message: String,
    val generatedAt: Long = System.currentTimeMillis()
)

private class MutableTaskStats {
    val retryCount = AtomicLong(0)
    val totalDurationMs = AtomicLong(0)

    @Volatile
    var lastStartedAt: Long? = null

    @Volatile
    var lastCompletedAt: Long? = null

    @Volatile
    var lastDurationMs: Long? = null

    @Volatile
    var lastError: String? = null

    @Volatile
    var lastResult: String? = null

    fun markStarted(at: Long) {
        lastStartedAt = at
        lastError = null
    }

    fun markRetry(error: Throwable) {
        retryCount.incrementAndGet()
        lastError = error.message ?: error::class.simpleName
    }

    fun markCompleted(event: TaskCompleteEvent) {
        lastCompletedAt = event.endTime
        lastDurationMs = event.duration
        totalDurationMs.addAndGet(event.duration)
        if (event.isSuccess) {
            lastError = null
            lastResult = summarize(event.result)
        } else {
            lastError = event.error?.message ?: event.error?.javaClass?.simpleName
            lastResult = null
        }
    }

    fun markSkipped(error: String) {
        lastError = error
        lastResult = null
    }

    fun markRejected(error: String) {
        lastError = error
        lastResult = null
    }
}

/** Reusable Colleen-mounted dashboard for controlling a [TaskScheduler]. */
class TaskDashboard(
    private val options: TaskDashboardOptions = TaskDashboardOptions(),
    private val registerTasks: (TaskScheduler) -> Collection<DashboardTaskDescriptor>
) {
    private val lifecycleLock = ReentrantLock()
    private val eventLock = ReentrantLock()
    private val activeGeneration = AtomicLong(0)
    private val eventSeq = AtomicLong(0)
    private val taskStats = ConcurrentHashMap<String, MutableTaskStats>()
    private val descriptors = ConcurrentHashMap<String, DashboardTaskDescriptor>()
    private val removedTasks = ConcurrentHashMap.newKeySet<String>()
    private val events = ArrayDeque<DashboardEvent>()

    @Volatile
    private var scheduler: TaskScheduler? = null

    @Volatile
    private var startedAt = System.currentTimeMillis()

    init {
        rebuild("start", "Dashboard started; tasks registered")
    }

    /** Mounts dashboard JSON endpoints under [basePath]. */
    fun mount(app: Colleen, basePath: String = "/api") {
        app.group(basePath) {
            get("/overview", ::getDashboardOverview)
            get("/events", ::getDashboardEvents)
            get("/history", ::getDashboardHistory)

            group("/tasks") {
                get("/", ::getDashboardTasks)
                get("/{name}", ::getDashboardTask)
                post("/{name}/run", ::runDashboardTask)
                post("/{name}/enable", ::enableDashboardTask)
                post("/{name}/disable", ::disableDashboardTask)
                delete("/{name}", ::removeDashboardTask)
            }

            group("/admin") {
                post("/reset", ::resetTaskDashboard)
            }
        }
    }

    /** Returns aggregate scheduler health and counters. */
    fun overview(): OverviewResponse {
        val tasks = tasks()
        val now = System.currentTimeMillis()
        val totalRuns = tasks.sumOf { it.runCount }
        val totalSuccesses = tasks.sumOf { it.successCount }
        val totalFailures = tasks.sumOf { it.failureCount }
        val totalDuration = tasks.sumOf { it.totalDurationMs }
        val completed = totalSuccesses + totalFailures
        val eventBounds = eventLock.withLock {
            (events.firstOrNull()?.seq to events.size)
        }
        val running = currentScheduler().isRunning

        return OverviewResponse(
            schedulerRunning = running,
            health = health(running, tasks),
            taskCount = tasks.size,
            enabledCount = tasks.count { it.enabled },
            disabledCount = tasks.count { !it.enabled },
            runningCount = tasks.sumOf { it.activeExecutions },
            removedCount = removedTasks.size,
            totalRuns = totalRuns,
            totalSuccesses = totalSuccesses,
            totalFailures = totalFailures,
            totalRetries = tasks.sumOf { it.retryCount },
            totalSkipped = tasks.sumOf { it.skipCount },
            totalRejected = tasks.sumOf { it.rejectedCount },
            successRate = ratio(totalSuccesses, completed),
            averageDurationMs = average(totalDuration, completed),
            historyRetained = eventBounds.second,
            historyCapacity = options.historyLimit,
            oldestEventSeq = eventBounds.first,
            latestEventSeq = eventSeq.get(),
            generation = activeGeneration.get(),
            uptimeMs = now - startedAt,
            generatedAt = now
        )
    }

    /** Returns all currently registered task snapshots. */
    fun tasks(): List<TaskSnapshot> {
        val current = currentScheduler()
        return current.listTaskNames()
            .sorted()
            .mapNotNull { name ->
                val info = current.getTaskInfo(name) ?: return@mapNotNull null
                val descriptor = descriptors[name] ?: DashboardTaskDescriptor(name = name)
                val stats = taskStats.getOrPut(name) { MutableTaskStats() }
                info.toSnapshot(descriptor, stats)
            }
    }

    /** Returns a single task snapshot or throws [NotFound]. */
    fun task(name: String): TaskSnapshot {
        val current = currentScheduler()
        ensureTaskExists(current, name)
        val info = current.getTaskInfo(name) ?: throw NotFound("Task '$name' was removed or never existed. Use reset to restore tasks.")
        val descriptor = descriptors[name] ?: DashboardTaskDescriptor(name = name)
        val stats = taskStats.getOrPut(name) { MutableTaskStats() }
        return info.toSnapshot(descriptor, stats)
    }

    /** Returns events after [after], optionally filtered to one task before limiting. */
    fun eventsAfter(after: Long?, requestLimit: Int?, taskName: String? = null): EventFeed {
        val floor = after ?: 0L
        val limit = requestLimit?.coerceIn(1, options.historyLimit) ?: options.historyLimit
        val (snapshot, oldestSeq, truncated) = eventLock.withLock {
            val matching = events.filter { taskName == null || it.taskName == taskName }
            val newer = matching.filter { it.seq > floor }
            val limited = newer.takeLast(limit)
            val oldest = matching.firstOrNull()?.seq
            val isTruncated = newer.size > limited.size || (oldest != null && floor > 0 && floor < oldest - 1)
            Triple(limited, oldest, isTruncated)
        }
        return EventFeed(
            events = snapshot,
            latestSeq = eventSeq.get(),
            oldestSeq = oldestSeq,
            capacity = options.historyLimit,
            truncated = truncated
        )
    }

    /** Requests immediate manual execution of a task. */
    fun runTask(name: String): ActionResponse = lifecycleLock.withLock {
        val current = currentScheduler()
        ensureTaskExists(current, name)
        current.run(name)
        recordEvent("control", "queued", name, "Manual execution requested")
        ActionResponse(action = "run", taskName = name, message = "Task '$name' queued for immediate execution")
    }

    /** Enables a registered task. */
    fun enableTask(name: String): ActionResponse = lifecycleLock.withLock {
        val current = currentScheduler()
        ensureTaskExists(current, name)
        current.enable(name)
        recordEvent("control", "enabled", name, "Task enabled")
        ActionResponse(action = "enable", taskName = name, message = "Task '$name' enabled")
    }

    /** Disables a registered task. */
    fun disableTask(name: String): ActionResponse = lifecycleLock.withLock {
        val current = currentScheduler()
        ensureTaskExists(current, name)
        current.disable(name)
        recordEvent("control", "disabled", name, "Task disabled")
        ActionResponse(action = "disable", taskName = name, message = "Task '$name' disabled")
    }

    /** Removes a task until the dashboard is reset. */
    fun removeTask(name: String): ActionResponse = lifecycleLock.withLock {
        val current = currentScheduler()
        ensureTaskExists(current, name)
        current.remove(name)
        taskStats.remove(name)
        removedTasks.add(name)
        recordEvent("control", "removed", name, "Task removed until reset")
        ActionResponse(action = "remove", taskName = name, message = "Task '$name' removed. Use reset to restore demo tasks.")
    }

    /** Rebuilds the scheduler and restores all registered demo tasks. */
    fun reset(): ActionResponse {
        if (!options.allowReset) {
            throw Forbidden("Task dashboard reset is disabled for this application")
        }
        return lifecycleLock.withLock {
            rebuild("reset", "Dashboard reset; tasks restored")
            ActionResponse(action = "reset", message = "Dashboard reset; tasks restored")
        }
    }

    /** Shuts down the owned scheduler. */
    fun shutdown() {
        lifecycleLock.withLock {
            activeGeneration.incrementAndGet()
            scheduler?.shutdown()
            scheduler = null
        }
    }

    private fun rebuild(status: String, message: String) {
        val previous = scheduler
        val generation = activeGeneration.incrementAndGet()
        previous?.shutdown(awaitTermination = false)

        startedAt = System.currentTimeMillis()
        taskStats.clear()
        descriptors.clear()
        removedTasks.clear()
        eventLock.withLock { events.clear() }

        val next = createScheduler(generation)
        registerTasks(next).forEach { descriptor ->
            descriptors[descriptor.name] = descriptor
        }
        next.listTaskNames().forEach { name ->
            taskStats[name] = MutableTaskStats()
            descriptors.putIfAbsent(name, DashboardTaskDescriptor(name = name))
        }
        next.start()
        scheduler = next

        recordEvent("system", status, null, message)
    }

    private fun createScheduler(generation: Long): TaskScheduler =
        TaskScheduler {
            concurrency = options.concurrency
            threadNamePrefix = options.threadNamePrefix
            registerShutdownHook = false
            onTaskStart = { event ->
                if (activeGeneration.get() == generation) handleStart(event)
            }
            onRetry = { event ->
                if (activeGeneration.get() == generation) handleRetry(event)
            }
            onTaskComplete = { event ->
                if (activeGeneration.get() == generation) handleComplete(event)
            }
            onTaskSkipped = { event ->
                if (activeGeneration.get() == generation) handleSkipped(event)
            }
            onTaskRejected = { event ->
                if (activeGeneration.get() == generation) handleRejected(event)
            }
        }

    private fun handleStart(event: TaskStartEvent) {
        taskStats[event.taskName]?.markStarted(event.actualTime)
        recordEvent(
            type = "task",
            status = "started",
            taskName = event.taskName,
            message = "Execution started",
            detail = "scheduled=${event.scheduledTime}, actual=${event.actualTime}"
        )
    }

    private fun handleRetry(event: TaskRetryEvent) {
        taskStats[event.taskName]?.markRetry(event.error)
        recordEvent(
            type = "retry",
            status = "retrying",
            taskName = event.taskName,
            message = "Attempt ${event.failedAttempts}/${event.maxAttempts} failed; retrying in ${event.nextRetryDelayMs} ms",
            detail = event.error.message ?: event.error::class.simpleName
        )
    }

    private fun handleComplete(event: TaskCompleteEvent) {
        taskStats[event.taskName]?.markCompleted(event)
        recordEvent(
            type = "task",
            status = if (event.isSuccess) "succeeded" else "failed",
            taskName = event.taskName,
            message = if (event.isSuccess) {
                "Execution completed in ${event.duration} ms"
            } else {
                "Execution failed after ${event.duration} ms"
            },
            detail = event.error?.message ?: summarize(event.result)
        )
    }

    private fun handleSkipped(event: TaskSkippedEvent) {
        val message = "Execution skipped: ${event.reason}"
        taskStats[event.taskName]?.markSkipped(message)
        recordEvent(
            type = "task",
            status = "skipped",
            taskName = event.taskName,
            message = message,
            detail = "type=${event.executionType}, scheduled=${event.scheduledTime}"
        )
    }

    private fun handleRejected(event: TaskRejectedEvent) {
        val message = "Execution rejected: ${event.reason}"
        taskStats[event.taskName]?.markRejected(message)
        recordEvent(
            type = "task",
            status = "rejected",
            taskName = event.taskName,
            message = message,
            detail = "type=${event.executionType}, scheduled=${event.scheduledTime}"
        )
    }

    private fun TaskInfo.toSnapshot(descriptor: DashboardTaskDescriptor, stats: MutableTaskStats): TaskSnapshot {
        val active = activeExecutions.toInt()
        val manualOnly = scheduleDescription == null
        val success = successCount
        val failure = failureCount
        val completed = success + failure
        val now = System.currentTimeMillis()
        val lastCompleted = stats.lastCompletedAt ?: lastCompletedAt

        return TaskSnapshot(
            name = name,
            description = descriptor.description,
            group = descriptor.group,
            scheduleDescription = scheduleDescription,
            manualOnly = manualOnly,
            enabled = enabled,
            allowConcurrent = allowConcurrent,
            status = when {
                active > 0 -> "running"
                !enabled -> "disabled"
                manualOnly -> "manual"
                else -> "scheduled"
            },
            retry = retryPolicy?.toSnapshot(),
            activeExecutions = active,
            nextScheduledAt = nextScheduledAt,
            runCount = runCount,
            successCount = success,
            failureCount = failure,
            skipCount = skipCount,
            rejectedCount = rejectedCount,
            retryCount = stats.retryCount.get(),
            totalDurationMs = stats.totalDurationMs.get(),
            averageDurationMs = average(stats.totalDurationMs.get(), completed),
            successRate = ratio(success, completed),
            lastStartedAt = stats.lastStartedAt ?: lastStartedAt,
            lastCompletedAt = lastCompleted,
            lastRunAgeMs = lastCompleted?.let { now - it },
            lastDurationMs = stats.lastDurationMs ?: lastDurationMs,
            lastError = stats.lastError ?: lastError?.message,
            lastResult = stats.lastResult
        )
    }

    private fun RetryPolicy.toSnapshot(): RetryPolicySnapshot =
        RetryPolicySnapshot(
            maxAttempts = maxAttempts,
            initialDelayMs = initialDelay.toMillis(),
            backoffMultiplier = backoffMultiplier,
            maxDelayMs = maxDelay.toMillis()
        )

    private fun currentScheduler(): TaskScheduler =
        scheduler ?: throw IllegalStateException("Task dashboard scheduler is not ready")

    private fun ensureTaskExists(current: TaskScheduler, name: String) {
        if (!current.exists(name)) {
            throw NotFound("Task '$name' was removed or never existed. Use reset to restore tasks.")
        }
    }

    private fun recordEvent(
        type: String,
        status: String,
        taskName: String?,
        message: String,
        detail: String? = null
    ) {
        val event = DashboardEvent(
            seq = eventSeq.incrementAndGet(),
            timestamp = System.currentTimeMillis(),
            type = type,
            status = status,
            taskName = taskName,
            message = message,
            detail = detail?.take(240)
        )
        eventLock.withLock {
            events.addLast(event)
            while (events.size > options.historyLimit) {
                events.removeFirst()
            }
        }
    }

    private fun health(running: Boolean, tasks: List<TaskSnapshot>): String =
        when {
            !running -> "offline"
            tasks.any { it.status == "running" } -> "busy"
            tasks.any { it.lastError != null } -> "degraded"
            tasks.any { !it.enabled } -> "attention"
            else -> "healthy"
        }
}

fun getDashboardOverview(dashboard: TaskDashboard): OverviewResponse = dashboard.overview()

fun getDashboardTasks(dashboard: TaskDashboard): List<TaskSnapshot> = dashboard.tasks()

fun getDashboardTask(name: Path<String>, dashboard: TaskDashboard): TaskSnapshot =
    dashboard.task(name.value)

fun getDashboardEvents(
    after: Query<Long?>,
    limit: Query<Int?>,
    task: Query<String?>,
    dashboard: TaskDashboard
): EventFeed = dashboard.eventsAfter(after.value, limit.value, task.value)

fun getDashboardHistory(
    after: Query<Long?>,
    limit: Query<Int?>,
    task: Query<String?>,
    dashboard: TaskDashboard
): EventFeed = dashboard.eventsAfter(after.value, limit.value, task.value)

fun runDashboardTask(name: Path<String>, dashboard: TaskDashboard): Result<ActionResponse> =
    Result.of(202, dashboard.runTask(name.value))

fun enableDashboardTask(name: Path<String>, dashboard: TaskDashboard): ActionResponse =
    dashboard.enableTask(name.value)

fun disableDashboardTask(name: Path<String>, dashboard: TaskDashboard): ActionResponse =
    dashboard.disableTask(name.value)

fun removeDashboardTask(name: Path<String>, dashboard: TaskDashboard): ActionResponse =
    dashboard.removeTask(name.value)

fun resetTaskDashboard(dashboard: TaskDashboard): ActionResponse = dashboard.reset()

private fun average(total: Long, count: Long): Long? =
    if (count == 0L) null else (total.toDouble() / count.toDouble()).roundToLong()

private fun ratio(part: Long, total: Long): Double =
    if (total == 0L) 1.0 else part.toDouble() / total.toDouble()

private fun summarize(value: Any?): String? =
    value?.toString()?.replace(Regex("\\s+"), " ")?.take(180)
