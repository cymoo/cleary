package io.github.cymoo.cleary

import java.time.Instant
import java.time.ZoneId
import kotlin.math.min
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/** DSL builder used by [TaskScheduler.task] and [TaskScheduler.replace] to configure one task. */
class TaskBuilder internal constructor(val name: String) {
    internal var schedule: Schedule? = null
        private set

    internal var initialDelayValue: Duration? = null
        private set

    internal var allowConcurrent: Boolean = false
        private set

    internal var retryPolicy: RetryPolicy? = null
        private set

    internal var timeoutValue: Duration? = null
        private set

    internal var tagsValue: Set<String> = emptySet()
        private set

    /** Null means "not set": [TaskScheduler.replace] inherits the previous enabled state. */
    internal var enabledValue: Boolean? = null
        private set

    internal var taskBlock: (TaskContext.() -> Any?)? = null
        private set

    internal var startHook: ((TaskStartEvent) -> Unit)? = null
        private set

    internal var completeHook: ((TaskCompleteEvent) -> Unit)? = null
        private set

    internal var retryHook: ((TaskRetryEvent) -> Unit)? = null
        private set

    internal var skippedHook: ((TaskSkippedEvent) -> Unit)? = null
        private set

    internal var rejectedHook: ((TaskRejectedEvent) -> Unit)? = null
        private set

    /** Schedules the task with a Quartz-compatible cron expression. */
    fun cron(expr: String, zone: ZoneId = ZoneId.systemDefault()) {
        setSchedule(Schedule.Cron(expr, zone))
    }

    /** Schedules the task at a fixed rate anchored to the planned schedule grid. */
    fun every(interval: Duration) {
        setSchedule(Schedule.FixedRate(interval))
    }

    /** Schedules the task at a fixed rate anchored to the planned schedule grid. */
    fun every(interval: java.time.Duration): Unit = every(interval.toKotlinDuration())

    /** Schedules the task at a fixed interval measured from the previous run's completion. */
    fun fixedDelay(interval: Duration) {
        setSchedule(Schedule.FixedDelay(interval))
    }

    /** Schedules the task at a fixed interval measured from the previous run's completion. */
    fun fixedDelay(interval: java.time.Duration): Unit = fixedDelay(interval.toKotlinDuration())

    /** Schedules the task once at the given instant. */
    fun once(at: Instant) {
        setSchedule(Schedule.Once(at))
    }

    /** Schedules the task with a user-provided [Trigger]. */
    fun custom(trigger: Trigger, description: String = "custom") {
        setSchedule(Schedule.Custom(trigger, description))
    }

    /**
     * Delays the first run after each arm (start, registration, or re-enable).
     * With [every] or [fixedDelay], the first run happens at `now + delay` instead of
     * `now + interval`; with [cron], fires at the first cron point after `now + delay`;
     * with [once], fires at `at + delay`.
     */
    fun initialDelay(delay: Duration) {
        check(initialDelayValue == null) { "Task '$name': initialDelay already set" }
        require(!delay.isNegative()) { "initialDelay must be non-negative, got: $delay" }
        initialDelayValue = delay
    }

    /** See [initialDelay]. */
    fun initialDelay(delay: java.time.Duration): Unit = initialDelay(delay.toKotlinDuration())

    /** Allows or disallows overlapping executions of this task. */
    fun concurrent(allow: Boolean = true) {
        allowConcurrent = allow
    }

    /** Registers the task enabled (default) or disabled. */
    fun enabled(value: Boolean) {
        enabledValue = value
    }

    /** Attaches tags used for grouping and [TaskScheduler.listTasks] filtering. */
    fun tags(vararg values: String) {
        require(values.all { it.isNotBlank() }) { "Task '$name': tags cannot be blank" }
        tagsValue = values.toSet()
    }

    /**
     * Interrupts an attempt that runs longer than [duration] and records the failure
     * as [TaskTimeoutException]. Only effective for interruptible task bodies; tasks
     * can also poll [TaskContext.isCancelled] to cooperate.
     */
    fun timeout(duration: Duration) {
        require(duration.inWholeMilliseconds > 0) { "timeout must be positive, got: $duration" }
        timeoutValue = duration
    }

    /** See [timeout]. */
    fun timeout(duration: java.time.Duration): Unit = timeout(duration.toKotlinDuration())

    /**
     * Retries failed task bodies before final completion. Retries wait in the
     * scheduler's delay queue, not on a worker thread. [InterruptedException] and
     * [Error]s are never retried.
     */
    fun retry(
        maxAttempts: Int,
        initialDelay: Duration,
        backoffMultiplier: Double = 1.0,
        maxDelay: Duration = RetryPolicy.DEFAULT_MAX_DELAY
    ) {
        retryPolicy = RetryPolicy(maxAttempts, initialDelay, backoffMultiplier, maxDelay)
    }

    /** See [retry]. */
    fun retry(
        maxAttempts: Int,
        initialDelay: java.time.Duration,
        backoffMultiplier: Double = 1.0,
        maxDelay: java.time.Duration = RetryPolicy.DEFAULT_MAX_DELAY.toJavaDuration()
    ): Unit = retry(
        maxAttempts,
        initialDelay.toKotlinDuration(),
        backoffMultiplier,
        maxDelay.toKotlinDuration()
    )

    /** Defines the task body. */
    fun run(block: TaskContext.() -> Any?) {
        check(taskBlock == null) { "Task '$name': run { } block already defined" }
        taskBlock = block
    }

    /** Task-level [TaskSchedulerConfig.onTaskStart]; fires before the global hook. */
    fun onStart(hook: (TaskStartEvent) -> Unit) {
        startHook = hook
    }

    /** Task-level [TaskSchedulerConfig.onTaskComplete]; fires before the global hook. */
    fun onComplete(hook: (TaskCompleteEvent) -> Unit) {
        completeHook = hook
    }

    /** Task-level [TaskSchedulerConfig.onRetry]; fires before the global hook. */
    fun onRetry(hook: (TaskRetryEvent) -> Unit) {
        retryHook = hook
    }

    /** Task-level [TaskSchedulerConfig.onTaskSkipped]; fires before the global hook. */
    fun onSkipped(hook: (TaskSkippedEvent) -> Unit) {
        skippedHook = hook
    }

    /** Task-level [TaskSchedulerConfig.onTaskRejected]; fires before the global hook. */
    fun onRejected(hook: (TaskRejectedEvent) -> Unit) {
        rejectedHook = hook
    }

    internal fun buildHooks(): TaskHooks =
        if (startHook == null && completeHook == null && retryHook == null &&
            skippedHook == null && rejectedHook == null
        ) TaskHooks.EMPTY
        else TaskHooks(startHook, completeHook, retryHook, skippedHook, rejectedHook)

    private fun setSchedule(value: Schedule) {
        check(schedule == null) {
            "Task '$name': schedule already set (only one of cron/every/fixedDelay/once/custom is allowed)"
        }
        schedule = value
    }
}

/** Per-task lifecycle hooks; each fires before its global counterpart. */
internal class TaskHooks(
    val onStart: ((TaskStartEvent) -> Unit)?,
    val onComplete: ((TaskCompleteEvent) -> Unit)?,
    val onRetry: ((TaskRetryEvent) -> Unit)?,
    val onSkipped: ((TaskSkippedEvent) -> Unit)?,
    val onRejected: ((TaskRejectedEvent) -> Unit)?
) {
    companion object {
        val EMPTY = TaskHooks(null, null, null, null, null)
    }
}

/** Retry settings for a task body that throws. */
data class RetryPolicy(
    val maxAttempts: Int,
    val initialDelay: Duration,
    val backoffMultiplier: Double = 1.0,
    val maxDelay: Duration = DEFAULT_MAX_DELAY
) {
    constructor(
        maxAttempts: Int,
        initialDelay: java.time.Duration,
        backoffMultiplier: Double = 1.0,
        maxDelay: java.time.Duration = DEFAULT_MAX_DELAY.toJavaDuration()
    ) : this(
        maxAttempts,
        initialDelay.toKotlinDuration(),
        backoffMultiplier,
        maxDelay.toKotlinDuration()
    )

    companion object {
        /** Default cap applied to computed retry delays. */
        val DEFAULT_MAX_DELAY: Duration = 30.seconds
    }

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1, got: $maxAttempts" }
        require(!initialDelay.isNegative()) { "initialDelay must be non-negative, got: $initialDelay" }
        require(backoffMultiplier >= 1.0) { "backoffMultiplier must be >= 1.0, got: $backoffMultiplier" }
        require(maxDelay.inWholeMilliseconds > 0) { "maxDelay must be positive, got: $maxDelay" }
    }

    internal fun delayForFailedAttempts(failedAttempts: Int): Long {
        require(failedAttempts >= 0)
        val baseMs = initialDelay.inWholeMilliseconds
        val cappedMs = maxDelay.inWholeMilliseconds
        if (backoffMultiplier == 1.0) return min(baseMs, cappedMs)
        val raw = baseMs.toDouble() * backoffMultiplier.pow(failedAttempts.toDouble())
        if (raw.isInfinite() || raw.isNaN() || raw >= cappedMs.toDouble()) return cappedMs
        return raw.toLong()
    }
}
