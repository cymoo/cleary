package io.github.cymoo.cleary

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

/** Declarative schedule attached to a task. */
sealed class Schedule {
    /** Quartz-compatible cron expression with its evaluation time zone. */
    data class Cron(val expression: String, val zone: ZoneId = ZoneId.systemDefault()) : Schedule()

    /**
     * Fixed-rate interval; fire times are anchored to the planned schedule grid,
     * so execution latency never shifts the schedule.
     */
    data class FixedRate(val interval: Duration) : Schedule() {
        constructor(interval: java.time.Duration) : this(interval.toKotlinDuration())

        init {
            require(interval.inWholeMilliseconds > 0) {
                "FixedRate interval must be at least 1 ms, got: $interval"
            }
        }
    }

    /** Interval measured from the completion of the previous scheduled run. */
    data class FixedDelay(val interval: Duration) : Schedule() {
        constructor(interval: java.time.Duration) : this(interval.toKotlinDuration())

        init {
            require(interval.inWholeMilliseconds > 0) {
                "FixedDelay interval must be at least 1 ms, got: $interval"
            }
        }
    }

    /** One-shot execution at [at]. */
    data class Once(val at: Instant) : Schedule()

    /** User-provided [Trigger]; [description] is surfaced in [TaskInfo.scheduleDescription]. */
    data class Custom(val trigger: Trigger, val description: String = "custom") : Schedule()

    internal fun toTrigger(initialDelay: Duration?): Trigger {
        val delayMs = initialDelay?.inWholeMilliseconds
        return when (this) {
            is Cron -> CronTrigger(expression, zone).withInitialDelay(delayMs)
            is FixedRate -> FixedRateTrigger(interval.inWholeMilliseconds, delayMs)
            is FixedDelay -> FixedDelayTrigger(interval.inWholeMilliseconds, delayMs)
            is Once -> OnceTrigger(saturatedAdd(at.toEpochMilli(), delayMs ?: 0))
            is Custom -> trigger.withInitialDelay(delayMs)
        }
    }

    internal fun describe(): String = when (this) {
        is Cron -> "cron[$zone]: $expression"
        is FixedRate -> "every ${interval.toMillisDescription()}"
        is FixedDelay -> "fixed-delay ${interval.toMillisDescription()}"
        is Once -> "once at $at"
        is Custom -> description
    }
}

internal fun Duration.toMillisDescription(): String {
    val ms = inWholeMilliseconds
    return when {
        ms % 3_600_000L == 0L -> "${ms / 3_600_000L}h"
        ms % 60_000L == 0L -> "${ms / 60_000L}m"
        ms % 1_000L == 0L -> "${ms / 1_000L}s"
        else -> "${ms}ms"
    }
}

/**
 * Computes fire times (epoch milliseconds) for a task's schedule stream.
 *
 * Implementations must be thread-safe: the scheduler may call a trigger from its
 * dispatch thread and from threads invoking [TaskScheduler.enable].
 */
interface Trigger {
    /**
     * First fire time for a stream armed at [armTime] (when the scheduler starts, the
     * task is registered, or the task is re-enabled), or null if the stream never fires.
     * The result may be in the past; a past time fires immediately.
     */
    fun initialExecutionTime(armTime: Long): Long?

    /**
     * Next fire time strictly after [minTime], or null to end the stream.
     *
     * [lastScheduledTime] is the previous planned fire time and anchors grid-based
     * schedules. [minTime] is always >= [lastScheduledTime]; under [MisfirePolicy.SKIP]
     * it is the current time (so missed slots are skipped), under
     * [MisfirePolicy.CATCH_UP] it equals [lastScheduledTime].
     */
    fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long?
}

internal class CronTrigger(expression: String, private val zone: ZoneId) : Trigger {
    private val executionTime: ExecutionTime = try {
        ExecutionTime.forCron(cronParser.parse(expression).also { it.validate() })
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid cron expression: '$expression'", e)
    }

    // base - 1 so that a cron point exactly at the arm time still fires
    override fun initialExecutionTime(armTime: Long): Long? = nextAfter(armTime - 1)

    override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long? =
        nextAfter(minTime)

    private fun nextAfter(baseMs: Long): Long? {
        val base = ZonedDateTime.ofInstant(Instant.ofEpochMilli(baseMs), zone)
        return executionTime.nextExecution(base).map { it.toInstant().toEpochMilli() }.orElse(null)
    }

    companion object {
        private val cronParser: CronParser =
            CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
    }
}

/** Shared arm logic for interval-based triggers: first fire at arm + (initialDelay ?: interval). */
internal abstract class IntervalTrigger(
    protected val intervalMs: Long,
    private val initialDelayMs: Long?
) : Trigger {
    final override fun initialExecutionTime(armTime: Long): Long =
        saturatedAdd(armTime, initialDelayMs ?: intervalMs)
}

internal class FixedRateTrigger(intervalMs: Long, initialDelayMs: Long?) :
    IntervalTrigger(intervalMs, initialDelayMs) {

    override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long {
        // Jump over missed slots in one step while staying on the original grid.
        val steps = (minTime - lastScheduledTime) / intervalMs + 1
        if (steps >= Long.MAX_VALUE / intervalMs) return Long.MAX_VALUE
        return saturatedAdd(lastScheduledTime, steps * intervalMs)
    }
}

internal class FixedDelayTrigger(intervalMs: Long, initialDelayMs: Long?) :
    IntervalTrigger(intervalMs, initialDelayMs) {

    // lastScheduledTime is the completion time of the previous run; misfire does not apply.
    override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long =
        saturatedAdd(lastScheduledTime, intervalMs)
}

internal class OnceTrigger(private val atMs: Long) : Trigger {
    private val consumed = AtomicBoolean(false)

    override fun initialExecutionTime(armTime: Long): Long? =
        if (consumed.get()) null else atMs

    override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long? {
        consumed.set(true)
        return null
    }
}

internal class DelayedTrigger(
    private val delayMs: Long,
    private val inner: Trigger
) : Trigger {
    override fun initialExecutionTime(armTime: Long): Long? =
        inner.initialExecutionTime(saturatedAdd(armTime, delayMs))

    override fun nextExecutionTime(lastScheduledTime: Long, minTime: Long): Long? =
        inner.nextExecutionTime(lastScheduledTime, minTime)
}

private fun Trigger.withInitialDelay(delayMs: Long?): Trigger =
    if (delayMs == null) this else DelayedTrigger(delayMs, this)

/** Addition that clamps at Long.MAX_VALUE/MIN_VALUE instead of wrapping around. */
internal fun saturatedAdd(a: Long, b: Long): Long {
    val sum = a + b
    return if ((a xor sum) and (b xor sum) < 0) {
        if (a > 0) Long.MAX_VALUE else Long.MIN_VALUE
    } else {
        sum
    }
}
