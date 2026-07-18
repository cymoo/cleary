package io.github.cymoo.cleary.dashboard

import io.github.cymoo.cleary.Schedule
import io.github.cymoo.cleary.SchedulerErrorEvent
import io.github.cymoo.cleary.TaskCompleteEvent
import io.github.cymoo.cleary.TaskInfo
import io.github.cymoo.cleary.TaskLifecycleListener
import io.github.cymoo.cleary.TaskRejectedEvent
import io.github.cymoo.cleary.TaskRetryEvent
import io.github.cymoo.cleary.TaskScheduler
import io.github.cymoo.cleary.TaskSkippedEvent
import io.github.cymoo.colleen.Colleen
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Options for the built-in web dashboard. */
class DashboardConfig {
    /** Maximum number of lifecycle events retained in the activity feed. */
    var eventHistoryLimit: Int = 300
        set(value) {
            require(value in 1..100_000) { "eventHistoryLimit must be in 1..100000, got: $value" }
            field = value
        }

    /** When true, all mutating endpoints (run/pause/resume/remove/schedule) answer 403. */
    var readOnly: Boolean = false
}

/**
 * Built-in web dashboard for a [TaskScheduler], implemented as a
 * [Colleen](https://github.com/cymoo/colleen) application. It shows live task
 * state, upcoming fire times, and an activity feed, and supports manual runs,
 * pause/resume, removal, and rescheduling from the browser.
 *
 * Standalone:
 * ```kotlin
 * val dashboard = Dashboard(scheduler).start(port = 8378)
 * ```
 *
 * Embedded in an existing Colleen application via [app]:
 * ```kotlin
 * val host = Colleen()
 * host.mount("/tasks", Dashboard(scheduler).app)
 * host.listen(8000)   // dashboard at http://localhost:8000/tasks/
 * ```
 *
 * The colleen dependency is optional in cleary's POM — add
 * `io.github.cymoo:colleen` (Java 21+) to use this class. The server has no
 * authentication; expose it beyond localhost only behind an authenticating
 * reverse proxy, or set [DashboardConfig.readOnly].
 */
class Dashboard(
    private val scheduler: TaskScheduler,
    configure: DashboardConfig.() -> Unit = {}
) {
    private val config = DashboardConfig().apply(configure)

    private val eventLock = ReentrantLock()
    private val events = ArrayDeque<FeedEvent>()
    private val standalone = AtomicBoolean(false)

    private val listener = object : TaskLifecycleListener {
        override fun onTaskComplete(event: TaskCompleteEvent) {
            val error = event.error
            if (error == null) {
                record("completed", event.taskName, "Completed", event.duration)
            } else {
                record("failed", event.taskName, "Failed: ${describeError(error)}", event.duration)
            }
        }

        override fun onRetry(event: TaskRetryEvent) {
            record(
                "retry", event.taskName,
                "Attempt ${event.failedAttempts}/${event.maxAttempts} failed; retrying in ${event.nextRetryDelayMs} ms — ${describeError(event.error)}"
            )
        }

        override fun onTaskSkipped(event: TaskSkippedEvent) {
            record("skipped", event.taskName, "Skipped: already running")
        }

        override fun onTaskRejected(event: TaskRejectedEvent) {
            record("rejected", event.taskName, "Rejected: worker queue full")
        }

        override fun onSchedulerError(event: SchedulerErrorEvent) {
            record("failed", event.taskName, "Scheduler error in ${event.phase}: ${describeError(event.error)}")
        }
    }

    /**
     * The dashboard as a mountable Colleen sub-application:
     * `host.mount("/tasks", dashboard.app)`. Also served directly by [start].
     */
    val app: Colleen = buildApp()

    init {
        scheduler.addListener(listener)
        record("lifecycle", null, "Dashboard attached")
    }

    /** Starts a standalone server for [app]. Binds 127.0.0.1 by default. */
    fun start(port: Int = DEFAULT_PORT, host: String = "127.0.0.1"): Dashboard {
        app.listen(port, host)
        standalone.set(true)
        return this
    }

    /** Stops the standalone server (if started) and detaches from the scheduler. */
    fun stop() {
        if (standalone.compareAndSet(true, false)) app.stop()
        scheduler.removeListener(listener)
    }

    // ------------------------------------------------------------------------
    // Routes
    // ------------------------------------------------------------------------

    private fun buildApp(): Colleen {
        val app = Colleen()
        app.config.propagateExceptions = false

        app.get("/") { ctx ->
            ctx.header("Cache-Control", "no-cache")
            ctx.html(pageHtml)
        }

        app.get("/assets/styles.css") { ctx ->
            ctx.header("Cache-Control", "no-cache")
            ctx.bytes(stylesCss, "text/css; charset=utf-8")
        }

        app.get("/api/state") { ctx ->
            ctx.header("Cache-Control", "no-store")
            val windowSec = (ctx.query("window")?.toIntOrNull() ?: DEFAULT_WINDOW_SEC).coerceIn(60, 3_600)
            stateSnapshot(windowSec)
        }

        app.get("/api/schedule/preview") { ctx ->
            val expr = ctx.query("expr")?.trim().orEmpty()
            if (expr.isEmpty()) throw IllegalArgumentException("expression is empty")
            val schedule = ScheduleExpr.parse(expr)
            PreviewResponse(
                expr = ScheduleExpr.canonical(schedule),
                meaning = ScheduleExpr.meaning(schedule),
                next = previewFires(schedule, count = 3)
            )
        }

        app.post("/api/tasks/{name}/{action}") { ctx ->
            val name = ctx.pathParam("name") ?: throw NoSuchElementException("missing task name")
            val action = ctx.pathParam("action") ?: throw NoSuchElementException("missing action")
            if (config.readOnly) {
                ctx.status(403).json(ErrorResponse(error = "dashboard is read-only"))
                return@post Unit
            }
            handleAction(ctx, name, action)
            ActionResponse()
        }

        app.onError<Exception> { e, ctx ->
            val status = when (e) {
                is NoSuchElementException -> 404
                is IllegalArgumentException -> 400
                is IllegalStateException -> 409
                else -> 500
            }
            ctx.status(status).json(ErrorResponse(error = describeError(e)))
        }

        return app
    }

    private fun handleAction(ctx: io.github.cymoo.colleen.Context, name: String, action: String) {
        when (action) {
            "run" -> {
                scheduler.run(name)
                record("lifecycle", name, "Manual run requested")
            }
            "pause" -> {
                scheduler.disable(name)
                record("lifecycle", name, "Paused")
            }
            "resume" -> {
                scheduler.enable(name)
                record("lifecycle", name, "Resumed")
            }
            "remove" -> {
                scheduler.remove(name)
                record("lifecycle", name, "Removed")
            }
            "schedule" -> {
                val payload = runCatching { ctx.json<SchedulePayload>() }.getOrNull()
                val expr = payload?.expr?.trim()
                require(!expr.isNullOrEmpty()) { "request body must be JSON with a non-empty \"expr\"" }
                val schedule = ScheduleExpr.parse(expr)
                scheduler.reschedule(name, schedule)
                record("lifecycle", name, "Rescheduled to ${ScheduleExpr.meaning(schedule)}")
            }
            else -> throw NoSuchElementException("unknown action '$action'")
        }
    }

    // ------------------------------------------------------------------------
    // Snapshots
    // ------------------------------------------------------------------------

    private fun stateSnapshot(windowSec: Int): DashboardState {
        val windowMs = windowSec * 1_000L
        val tasks = scheduler.listTasks().sortedBy { it.name }
        return DashboardState(
            now = System.currentTimeMillis(),
            windowSeconds = windowSec,
            stats = DashboardStats(
                totalTasks = tasks.size,
                enabledTasks = tasks.count { it.enabled },
                runningTasks = tasks.sumOf { it.activeExecutions },
                totalRuns = tasks.sumOf { it.runCount },
                totalErrors = tasks.sumOf { it.failureCount },
                totalSkips = tasks.sumOf { it.skipCount },
                workers = scheduler.workerConcurrency,
                startedAt = scheduler.startedAtMillis
            ),
            tasks = tasks.map { taskSnapshot(it, windowMs) },
            events = snapshotEvents(EVENT_RESPONSE_LIMIT)
        )
    }

    private fun taskSnapshot(info: TaskInfo, windowMs: Long): DashboardTask {
        val (schedule, _) = scheduler.scheduleOf(info.name) ?: (null to null)
        return DashboardTask(
            name = info.name,
            schedule = info.scheduleDescription,
            expr = ScheduleExpr.canonical(schedule),
            meaning = ScheduleExpr.meaning(schedule),
            enabled = info.enabled,
            running = info.running,
            runningCount = info.activeExecutions.toInt(),
            lastRun = info.lastStartedAt,
            nextRun = info.nextScheduledAt,
            runCount = info.runCount,
            errorCount = info.failureCount,
            skipCount = info.skipCount,
            rejectedCount = info.rejectedCount,
            lastError = info.lastError?.let(::describeError),
            timeoutMs = info.timeout?.inWholeMilliseconds ?: 0L,
            allowOverlapping = info.allowConcurrent,
            retry = info.retryPolicy?.let { policy ->
                buildString {
                    append("up to ").append(policy.maxAttempts).append(" attempts · ")
                    append(ScheduleExpr.formatDuration(policy.initialDelay)).append(" delay")
                    if (policy.backoffMultiplier > 1.0) append(" · ×").append(policy.backoffMultiplier)
                }
            },
            tags = info.tags.sorted(),
            upcoming = scheduler.upcomingFireTimes(info.name, windowMs, UPCOMING_LIMIT)
        )
    }

    /** Simulates the next fire times of a schedule using a fresh, detached trigger. */
    private fun previewFires(schedule: Schedule, count: Int): List<Long> {
        val trigger = schedule.toTrigger(null)
        val fires = mutableListOf<Long>()
        var last = trigger.initialExecutionTime(System.currentTimeMillis()) ?: return fires
        fires.add(last)
        while (fires.size < count) {
            last = trigger.nextExecutionTime(last, last) ?: break
            fires.add(last)
        }
        return fires
    }

    // ------------------------------------------------------------------------
    // Events
    // ------------------------------------------------------------------------

    private fun record(kind: String, task: String?, message: String, durationMs: Long = 0L) {
        val event = FeedEvent(System.currentTimeMillis(), kind, task, message, durationMs)
        eventLock.withLock {
            events.addFirst(event)
            while (events.size > config.eventHistoryLimit) events.removeLast()
        }
    }

    private fun snapshotEvents(limit: Int): List<FeedEvent> = eventLock.withLock {
        events.take(limit)
    }

    private fun describeError(t: Throwable): String =
        t.message?.takeIf { it.isNotBlank() } ?: t::class.simpleName ?: "error"

    companion object {
        const val DEFAULT_PORT = 8378
        private const val DEFAULT_WINDOW_SEC = 300
        private const val UPCOMING_LIMIT = 120
        private const val EVENT_RESPONSE_LIMIT = 100

        private val pageHtml: String by lazy { resource("index.html").toString(StandardCharsets.UTF_8) }
        private val stylesCss: ByteArray by lazy { resource("styles.css") }

        private fun resource(name: String): ByteArray =
            Dashboard::class.java.getResourceAsStream(name)?.readBytes()
                ?: throw IllegalStateException("Dashboard resource '$name' missing from classpath")
    }
}

// ----------------------------------------------------------------------------
// API payloads (serialized by Colleen's JSON mapper)
// ----------------------------------------------------------------------------

internal data class SchedulePayload(val expr: String? = null)

internal data class ActionResponse(val ok: Boolean = true)

internal data class ErrorResponse(val ok: Boolean = false, val error: String)

internal data class PreviewResponse(
    val ok: Boolean = true,
    val expr: String?,
    val meaning: String,
    val next: List<Long>
)

internal data class DashboardState(
    val now: Long,
    val windowSeconds: Int,
    val stats: DashboardStats,
    val tasks: List<DashboardTask>,
    val events: List<FeedEvent>
)

internal data class DashboardStats(
    val totalTasks: Int,
    val enabledTasks: Int,
    val runningTasks: Long,
    val totalRuns: Long,
    val totalErrors: Long,
    val totalSkips: Long,
    val workers: Int,
    val startedAt: Long?
)

internal data class DashboardTask(
    val name: String,
    val schedule: String?,
    val expr: String?,
    val meaning: String,
    val enabled: Boolean,
    val running: Boolean,
    val runningCount: Int,
    val lastRun: Long?,
    val nextRun: Long?,
    val runCount: Long,
    val errorCount: Long,
    val skipCount: Long,
    val rejectedCount: Long,
    val lastError: String?,
    val timeoutMs: Long,
    val allowOverlapping: Boolean,
    val retry: String?,
    val tags: List<String>,
    val upcoming: List<Long>
)

internal data class FeedEvent(
    val at: Long,
    val kind: String,
    val task: String?,
    val message: String,
    val durationMs: Long
)
