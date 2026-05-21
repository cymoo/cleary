package io.github.cymoo.cleary

import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import kotlin.math.min
import kotlin.math.pow

/** DSL builder used by [TaskScheduler.task] to configure one task. */
class TaskBuilder internal constructor(val name: String) {
    private var scheduleBase: Schedule? = null
    private var _initialDelay: Duration? = null

    internal var allowConcurrent: Boolean = false
        private set

    internal var retryPolicy: RetryPolicy? = null
        private set

    internal var taskBlock: (TaskContext.() -> Any?)? = null
        private set

    /** Schedules the task with a Quartz-compatible cron expression. */
    fun cron(expr: String, zone: ZoneId = ZoneId.systemDefault()) {
        checkNoSchedule()
        scheduleBase = Schedule.Cron(expr, zone)
    }

    /** Schedules the task at a fixed rate anchored to planned trigger time. */
    fun every(interval: Duration) {
        checkNoSchedule()
        scheduleBase = Schedule.FixedRate(interval)
    }

    /** Schedules the task once at the given instant. */
    fun once(at: Instant) {
        checkNoSchedule()
        scheduleBase = Schedule.Once(at)
    }

    /** Delays the first scheduled execution; with [once], fires at `at + delay`. */
    fun initialDelay(delay: Duration) {
        check(_initialDelay == null) { "Task '$name': initialDelay already set" }
        _initialDelay = delay
    }

    /** Allows or disallows overlapping executions of this task. */
    fun concurrent(allow: Boolean = true) {
        allowConcurrent = allow
    }

    /** Retries failed task bodies on the same worker thread before final completion. */
    fun retry(
        maxAttempts: Int,
        initialDelay: Duration,
        backoffMultiplier: Double = 1.0,
        maxDelay: Duration = Duration.ofSeconds(30)
    ) {
        retryPolicy = RetryPolicy(maxAttempts, initialDelay, backoffMultiplier, maxDelay)
    }

    /** Defines the task body. */
    fun run(block: TaskContext.() -> Any?) {
        check(taskBlock == null) { "Task '$name': run { } block already defined" }
        taskBlock = block
    }

    internal fun buildSchedule(): Schedule? {
        val base = scheduleBase ?: return null
        val delay = _initialDelay ?: return base
        return Schedule.WithInitialDelay(delay, base)
    }

    private fun checkNoSchedule() {
        check(scheduleBase == null) {
            "Task '$name': schedule already set (only one of cron/every/once is allowed)"
        }
    }
}

/** Retry settings for a task body that throws. */
data class RetryPolicy(
    val maxAttempts: Int,
    val initialDelay: Duration,
    val backoffMultiplier: Double = 1.0,
    val maxDelay: Duration = Duration.ofSeconds(30)
) {
    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1, got: $maxAttempts" }
        require(!initialDelay.isNegative) { "initialDelay must be non-negative, got: $initialDelay" }
        require(backoffMultiplier >= 1.0) { "backoffMultiplier must be >= 1.0, got: $backoffMultiplier" }
        require(!maxDelay.isZero && !maxDelay.isNegative) { "maxDelay must be positive, got: $maxDelay" }
    }

    internal fun delayForFailedAttempts(failedAttempts: Int): Long {
        require(failedAttempts >= 0)
        val baseMs = initialDelay.toMillis()
        val cappedMs = maxDelay.toMillis()
        if (backoffMultiplier == 1.0) return min(baseMs, cappedMs)
        val raw = baseMs.toDouble() * backoffMultiplier.pow(failedAttempts.toDouble())
        if (raw.isInfinite() || raw.isNaN() || raw >= cappedMs.toDouble()) return cappedMs
        return raw.toLong()
    }
}
