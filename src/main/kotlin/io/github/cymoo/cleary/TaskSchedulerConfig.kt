package io.github.cymoo.cleary

import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Mutable configuration used while constructing a [TaskScheduler]. */
class TaskSchedulerConfig {
    /** Number of worker threads used to execute task bodies. */
    var concurrency: Int =
        minOf(32, maxOf(4, Runtime.getRuntime().availableProcessors() * 4))
        set(value) {
            require(value > 0) { "concurrency must be > 0, got: $value" }
            field = value
        }

    /** Maximum number of queued worker submissions before runs are rejected. */
    var queueCapacity: Int = 10_000
        set(value) {
            require(value > 0) { "queueCapacity must be > 0, got: $value" }
            field = value
        }

    /** Prefix used for scheduler, worker, and shutdown-hook thread names. */
    var threadNamePrefix: String = "task-scheduler"
        set(value) {
            require(value.isNotBlank()) { "threadNamePrefix cannot be blank" }
            field = value
        }

    /** Start automatically after the configuration block finishes. */
    var autoStart: Boolean = false

    /** Register a JVM shutdown hook that calls [TaskScheduler.shutdown]. */
    var registerShutdownHook: Boolean = false

    /** How fire times that were missed (system sleep, backlog) are handled. */
    var misfirePolicy: MisfirePolicy = MisfirePolicy.SKIP

    /** How long [TaskScheduler.shutdown] waits for in-flight executions to finish. */
    var shutdownTimeout: Duration = 30.seconds
        set(value) {
            require(value.inWholeMilliseconds > 0) { "shutdownTimeout must be positive, got: $value" }
            field = value
        }

    /** Values visible to every task execution as read-only defaults. */
    val context: MutableMap<String, Any> = ConcurrentHashMap()

    /**
     * Called before a task body starts. Hooks run on scheduler, worker, or caller
     * threads and must return quickly; a slow hook delays dispatching.
     */
    var onTaskStart: ((TaskStartEvent) -> Unit)? = null

    /** Called once after a task finishes, after retries are exhausted or succeed. */
    var onTaskComplete: ((TaskCompleteEvent) -> Unit)? = null

    /** Called after a failed attempt when another retry will be attempted. */
    var onRetry: ((TaskRetryEvent) -> Unit)? = null

    /** Called when overlap protection skips an execution. */
    var onTaskSkipped: ((TaskSkippedEvent) -> Unit)? = null

    /** Called when the worker queue rejects an execution. */
    var onTaskRejected: ((TaskRejectedEvent) -> Unit)? = null

    /** Called when scheduler internals or user callbacks throw unexpectedly. */
    var onSchedulerError: ((SchedulerErrorEvent) -> Unit)? = null
}

/**
 * Multicast observer of task lifecycle events, registered via
 * [TaskScheduler.addListener]. Unlike the single-slot config hooks, any number
 * of listeners can observe a scheduler without displacing each other; they fire
 * after the task-level and global hooks. All methods default to no-ops.
 * Implementations must be fast and non-blocking.
 */
interface TaskLifecycleListener {
    fun onTaskStart(event: TaskStartEvent) {}
    fun onTaskComplete(event: TaskCompleteEvent) {}
    fun onRetry(event: TaskRetryEvent) {}
    fun onTaskSkipped(event: TaskSkippedEvent) {}
    fun onTaskRejected(event: TaskRejectedEvent) {}
    fun onSchedulerError(event: SchedulerErrorEvent) {}
}

/**
 * Policy for fire times that are already in the past when dispatched (after system
 * sleep, clock jumps, or a saturated scheduler).
 */
enum class MisfirePolicy {
    /** Run the late fire once, then skip ahead to the next future fire time. */
    SKIP,

    /** Run every missed fire time in quick succession until caught up. */
    CATCH_UP
}

/** Lifecycle state for a single-use [TaskScheduler]. */
enum class SchedulerState { NEW, RUNNING, SHUTDOWN }

/** Origin of a task execution attempt. */
enum class TaskExecutionType { SCHEDULED, MANUAL }

/** Reasons an execution can be skipped before it starts. */
enum class TaskSkipReason { ALREADY_RUNNING }

/** Reasons an execution can be rejected before it is queued. */
enum class TaskRejectedReason { WORKER_QUEUE_FULL }

/** Scheduler phase where an isolated error was observed. */
enum class SchedulerErrorPhase {
    ON_TASK_START,
    ON_TASK_COMPLETE,
    ON_RETRY,
    ON_TASK_SKIPPED,
    ON_TASK_REJECTED,
    SCHEDULER_LOOP
}

/** Failure error recorded when a task attempt exceeds its configured timeout. */
class TaskTimeoutException(
    val taskName: String,
    val timeout: Duration,
    cause: Throwable? = null
) : RuntimeException("Task '$taskName' exceeded timeout of $timeout", cause)

/** Explicit outcome for a task execution. */
sealed class TaskRunResult {
    data class Success(val value: Any?) : TaskRunResult()
    data class Failure(val error: Throwable) : TaskRunResult()
    data class Skipped(val taskName: String, val reason: TaskSkipReason) : TaskRunResult()
    data class Rejected(val taskName: String, val reason: TaskRejectedReason) : TaskRunResult()
}

/**
 * Event emitted immediately before a task body starts. [context] is the live
 * per-execution context: values written here are visible to the task body.
 */
data class TaskStartEvent(
    val taskName: String,
    val scheduledTime: Long,
    val actualTime: Long,
    val context: TaskContext
)

/** Event emitted once for the final outcome of a task execution. */
data class TaskCompleteEvent(
    val taskName: String,
    val startTime: Long,
    val endTime: Long,
    val result: Any?,
    val error: Throwable?
) {
    val duration: Long get() = endTime - startTime
    val isSuccess: Boolean get() = error == null
}

/** Event emitted between failed attempts when a retry is scheduled. */
data class TaskRetryEvent(
    val taskName: String,
    val failedAttempts: Int,
    val maxAttempts: Int,
    val error: Throwable,
    val nextRetryDelayMs: Long
)

/** Event emitted when an execution is skipped by the concurrency guard. */
data class TaskSkippedEvent(
    val taskName: String,
    val scheduledTime: Long?,
    val skippedAt: Long,
    val executionType: TaskExecutionType,
    val reason: TaskSkipReason
)

/** Event emitted when an execution cannot be queued by the worker pool. */
data class TaskRejectedEvent(
    val taskName: String,
    val scheduledTime: Long?,
    val rejectedAt: Long,
    val executionType: TaskExecutionType,
    val reason: TaskRejectedReason
)

/** Event emitted when a hook or scheduler phase throws and is isolated. */
data class SchedulerErrorEvent(
    val taskName: String?,
    val phase: SchedulerErrorPhase,
    val error: Throwable
)

/** Immutable runtime snapshot of a registered task. */
data class TaskInfo(
    val name: String,
    val scheduleDescription: String?,
    val enabled: Boolean,
    val allowConcurrent: Boolean,
    val retryPolicy: RetryPolicy?,
    val timeout: Duration?,
    val tags: Set<String>,
    val activeExecutions: Long,
    val running: Boolean,
    val nextScheduledAt: Long?,
    val lastStartedAt: Long?,
    val lastCompletedAt: Long?,
    val lastDurationMs: Long?,
    val lastError: Throwable?,
    val runCount: Long,
    val successCount: Long,
    val failureCount: Long,
    val skipCount: Long,
    val rejectedCount: Long
)
