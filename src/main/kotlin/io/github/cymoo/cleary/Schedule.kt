package io.github.cymoo.cleary

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.floor

/** Declarative schedule attached to a task. */
sealed class Schedule {
    /** Quartz-compatible cron expression with its evaluation time zone. */
    data class Cron(val expression: String, val zone: ZoneId = ZoneId.systemDefault()) : Schedule()

    /** Fixed-rate interval; executions are anchored to planned trigger time. */
    data class FixedRate(val interval: Duration) : Schedule() {
        init {
            require(!interval.isZero && !interval.isNegative && interval.toMillis() > 0) {
                "FixedRate interval must be at least 1 ms, got: $interval"
            }
        }
    }

    /** One-shot execution at [at]. */
    data class Once(val at: Instant) : Schedule()

    /** Wrapper that delays only the first execution of [schedule]. */
    data class WithInitialDelay(val delay: Duration, val schedule: Schedule) : Schedule() {
        init {
            require(!delay.isNegative) { "WithInitialDelay.delay must be non-negative, got: $delay" }
        }
    }

    internal fun toTrigger(): Trigger = when (this) {
        is Cron -> CronTrigger(expression, zone)
        is FixedRate -> FixedRateTrigger(interval)
        is Once -> OnceTrigger(at)
        is WithInitialDelay -> InitialDelayTrigger(delay, schedule.toTrigger())
    }

    internal fun describe(): String = when (this) {
        is Cron -> "cron[$zone]: $expression"
        is FixedRate -> "every ${interval.toMillisDescription()}"
        is Once -> "once at $at"
        is WithInitialDelay -> "initial-delay ${delay.toMillisDescription()}, then ${schedule.describe()}"
    }
}

private fun Duration.toMillisDescription(): String {
    val ms = toMillis()
    return when {
        ms % 3_600_000L == 0L -> "${ms / 3_600_000L}h"
        ms % 60_000L == 0L -> "${ms / 60_000L}m"
        ms % 1_000L == 0L -> "${ms / 1_000L}s"
        else -> "${ms}ms"
    }
}

internal interface Trigger {
    fun nextExecutionTime(lastScheduledTime: Long): Long?
}

internal class CronTrigger(expression: String, private val zone: ZoneId) : Trigger {
    private val executionTime: ExecutionTime = try {
        ExecutionTime.forCron(cronParser.parse(expression).also { it.validate() })
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid cron expression: '$expression'", e)
    }

    override fun nextExecutionTime(lastScheduledTime: Long): Long? {
        val base = ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastScheduledTime), zone)
        return executionTime.nextExecution(base).map { it.toInstant().toEpochMilli() }.orElse(null)
    }

    companion object {
        private val cronParser: CronParser =
            CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ))
    }
}

internal class FixedRateTrigger(private val interval: Duration) : Trigger {
    override fun nextExecutionTime(lastScheduledTime: Long): Long =
        lastScheduledTime + interval.toMillis()
}

internal class OnceTrigger(private val at: Instant) : Trigger {
    private val fired = AtomicBoolean(false)
    override fun nextExecutionTime(lastScheduledTime: Long): Long? =
        if (fired.compareAndSet(false, true)) at.toEpochMilli() else null
}

internal class InitialDelayTrigger(
    private val delay: Duration,
    private val inner: Trigger
) : Trigger {
    private val initialized = AtomicBoolean(false)

    override fun nextExecutionTime(lastScheduledTime: Long): Long? =
        if (initialized.compareAndSet(false, true)) {
            val innerFirst = inner.nextExecutionTime(lastScheduledTime) ?: return null
            innerFirst + delay.toMillis()
        } else {
            inner.nextExecutionTime(lastScheduledTime)
        }
}

private fun Double.toDurationOfSeconds(): Duration {
    val seconds = floor(this).toLong()
    val nanos = ((this - seconds) * 1_000_000_000L).toLong()
    return Duration.ofSeconds(seconds, nanos)
}

val Int.millisecond: Duration get() = toLong().millisecond
val Int.milliseconds: Duration get() = toLong().milliseconds
val Long.millisecond: Duration get() = Duration.ofMillis(this)
val Long.milliseconds: Duration get() = Duration.ofMillis(this)
val Double.millisecond: Duration get() = (this / 1_000).toDurationOfSeconds()
val Double.milliseconds: Duration get() = millisecond

val Int.second: Duration get() = toLong().second
val Int.seconds: Duration get() = toLong().seconds
val Long.second: Duration get() = Duration.ofSeconds(this)
val Long.seconds: Duration get() = Duration.ofSeconds(this)
val Double.second: Duration get() = toDurationOfSeconds()
val Double.seconds: Duration get() = second

val Int.minute: Duration get() = toLong().minute
val Int.minutes: Duration get() = toLong().minutes
val Long.minute: Duration get() = Duration.ofMinutes(this)
val Long.minutes: Duration get() = Duration.ofMinutes(this)
val Double.minute: Duration get() = (this * 60).toDurationOfSeconds()
val Double.minutes: Duration get() = minute

val Int.hour: Duration get() = toLong().hour
val Int.hours: Duration get() = toLong().hours
val Long.hour: Duration get() = Duration.ofHours(this)
val Long.hours: Duration get() = Duration.ofHours(this)
val Double.hour: Duration get() = (this * 3_600).toDurationOfSeconds()
val Double.hours: Duration get() = hour

val Int.day: Duration get() = toLong().day
val Int.days: Duration get() = toLong().days
val Long.day: Duration get() = Duration.ofDays(this)
val Long.days: Duration get() = Duration.ofDays(this)
val Double.day: Duration get() = (this * 86_400).toDurationOfSeconds()
val Double.days: Duration get() = day
