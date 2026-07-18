package io.github.cymoo.cleary.dashboard

import io.github.cymoo.cleary.Schedule
import java.time.Instant
import java.time.format.DateTimeParseException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Textual schedule expressions used by the dashboard's editor and preview:
 *
 *  - `every <duration>`        → [Schedule.FixedRate]
 *  - `fixed-delay <duration>`  → [Schedule.FixedDelay]
 *  - `once <ISO-8601 instant>` → [Schedule.Once]
 *  - anything else             → [Schedule.Cron] (Quartz), validated on parse
 *
 * Durations accept compound single-unit segments: `90s`, `5m`, `1h30m`, `250ms`, `1d`.
 */
internal object ScheduleExpr {
    private val durationSegment = Regex("(\\d+)(ms|s|m|h|d)")
    private val cronStep = Regex("^(?:\\*|0)/(\\d+)$")
    private val cronDigit = Regex("^\\d+$")

    /** Parses an expression into a schedule, throwing [IllegalArgumentException] when invalid. */
    fun parse(expression: String): Schedule {
        val expr = expression.trim()
        require(expr.isNotEmpty()) { "Expression is empty" }
        return when {
            expr.startsWith("every ") -> Schedule.FixedRate(parseDuration(expr.removePrefix("every ")))
            expr.startsWith("fixed-delay ") -> Schedule.FixedDelay(parseDuration(expr.removePrefix("fixed-delay ")))
            expr.startsWith("once ") -> Schedule.Once(parseInstant(expr.removePrefix("once ")))
            else -> Schedule.Cron(expr).also { it.toTrigger(null) } // validates the cron expression
        }
    }

    /** The editable canonical form of a schedule, or null when it cannot be edited as text. */
    fun canonical(schedule: Schedule?): String? = when (schedule) {
        null -> null
        is Schedule.Cron -> schedule.expression
        is Schedule.FixedRate -> "every ${formatDuration(schedule.interval)}"
        is Schedule.FixedDelay -> "fixed-delay ${formatDuration(schedule.interval)}"
        is Schedule.Once -> "once ${schedule.at}"
        is Schedule.Custom -> null
    }

    /** Short human phrase for a schedule ("every 30s", "daily 02:00"). */
    fun meaning(schedule: Schedule?): String = when (schedule) {
        null -> "manual only"
        is Schedule.Cron -> humanizeQuartz(schedule.expression)
        is Schedule.FixedRate -> "every ${formatDuration(schedule.interval)}"
        is Schedule.FixedDelay -> "every ${formatDuration(schedule.interval)} after completion"
        is Schedule.Once -> "once at ${schedule.at}"
        is Schedule.Custom -> schedule.description
    }

    private fun parseInstant(raw: String): Instant = try {
        Instant.parse(raw.trim())
    } catch (e: DateTimeParseException) {
        throw IllegalArgumentException("Invalid instant '${raw.trim()}', expected ISO-8601 like 2026-01-01T02:00:00Z", e)
    }

    internal fun parseDuration(raw: String): Duration {
        val text = raw.trim().replace(" ", "")
        val matches = durationSegment.findAll(text).toList()
        require(matches.isNotEmpty() && matches.joinToString("") { it.value } == text) {
            "Invalid duration '$raw', expected forms like 90s, 5m, 1h30m, 250ms"
        }
        var total = Duration.ZERO
        for (match in matches) {
            val amount = match.groupValues[1].toLong()
            total += when (match.groupValues[2]) {
                "ms" -> amount.milliseconds
                "s" -> amount.seconds
                "m" -> amount.minutes
                "h" -> amount.hours
                else -> amount.days
            }
        }
        require(total.inWholeMilliseconds > 0) { "Duration must be at least 1 ms, got '$raw'" }
        return total
    }

    internal fun formatDuration(duration: Duration): String {
        var ms = duration.inWholeMilliseconds
        val sb = StringBuilder()
        for ((unitMs, suffix) in listOf(86_400_000L to "d", 3_600_000L to "h", 60_000L to "m", 1_000L to "s", 1L to "ms")) {
            if (ms >= unitMs) {
                sb.append(ms / unitMs).append(suffix)
                ms %= unitMs
            }
        }
        return if (sb.isEmpty()) "0ms" else sb.toString()
    }

    /**
     * Renders common Quartz expressions as a short phrase; unrecognized
     * expressions are returned unchanged.
     */
    internal fun humanizeQuartz(expression: String): String {
        val fields = expression.trim().split(Regex("\\s+"))
        if (fields.size !in 6..7) return expression
        val (sec, min, hour, dom, mon, dow) = fields
        val anyDom = dom == "*" || dom == "?"
        val anyDow = dow == "*" || dow == "?"
        if (mon != "*" || !anyDom) return expression

        val dowSuffix = when {
            anyDow -> ""
            dow.equals("MON-FRI", ignoreCase = true) -> " on weekdays"
            else -> return expression
        }

        cronStep.find(sec)?.let { m ->
            if (min == "*" && hour == "*" && dowSuffix.isEmpty()) return "every ${m.groupValues[1]}s"
        }
        if (sec != "0") return expression

        cronStep.find(min)?.let { m ->
            if (hour == "*" && dowSuffix.isEmpty()) {
                return if (m.groupValues[1] == "1") "every minute" else "every ${m.groupValues[1]} min"
            }
        }
        if (min == "*" && hour == "*" && dowSuffix.isEmpty()) return "every minute"
        if (min == "0") {
            cronStep.find(hour)?.let { m ->
                if (dowSuffix.isEmpty()) {
                    return if (m.groupValues[1] == "1") "hourly" else "every ${m.groupValues[1]}h"
                }
            }
            if (hour == "*" && dowSuffix.isEmpty()) return "hourly"
        }
        if (cronDigit.matches(min) && cronDigit.matches(hour)) {
            return "daily %02d:%02d%s".format(hour.toInt(), min.toInt(), dowSuffix)
        }
        return expression
    }

    private operator fun List<String>.component6(): String = this[5]
}
