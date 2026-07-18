package io.github.cymoo.cleary.dashboard

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.github.cymoo.cleary.SchedulerErrorEvent
import io.github.cymoo.cleary.TaskCompleteEvent
import io.github.cymoo.cleary.TaskInfo
import io.github.cymoo.cleary.TaskLifecycleListener
import io.github.cymoo.cleary.TaskRejectedEvent
import io.github.cymoo.cleary.TaskRetryEvent
import io.github.cymoo.cleary.TaskScheduler
import io.github.cymoo.cleary.TaskSkippedEvent
import java.io.IOException
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.ArrayDeque
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
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
 * Built-in web dashboard for a [TaskScheduler], served by the JDK's embedded
 * HTTP server — no extra dependencies. It shows live task state, upcoming fire
 * times, and an activity feed, and supports manual runs, pause/resume, removal,
 * and rescheduling from the browser.
 *
 * ```kotlin
 * val scheduler = taskScheduler { autoStart = true }
 * // ... register tasks ...
 * Dashboard(scheduler).start(port = 8378)
 * ```
 *
 * The server binds to 127.0.0.1 by default and has no authentication; to expose
 * it beyond localhost, put it behind an authenticating reverse proxy or set
 * [DashboardConfig.readOnly].
 */
class Dashboard(
    private val scheduler: TaskScheduler,
    configure: DashboardConfig.() -> Unit = {}
) {
    private val config = DashboardConfig().apply(configure)

    private class Event(
        val at: Long,
        val kind: String,
        val task: String?,
        val message: String,
        val durationMs: Long
    )

    private val eventLock = ReentrantLock()
    private val events = ArrayDeque<Event>()

    private var server: HttpServer? = null
    private var executor: ExecutorService? = null

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

    /** The bound port after [start], or -1. */
    val port: Int get() = server?.address?.port ?: -1

    /**
     * Starts the dashboard HTTP server on daemon threads and begins recording
     * scheduler events. Pass port 0 to bind an ephemeral port (see [port]).
     */
    fun start(port: Int = DEFAULT_PORT, host: String = "127.0.0.1"): Dashboard {
        check(server == null) { "Dashboard is already running on port ${this.port}" }
        scheduler.addListener(listener)
        record("lifecycle", null, "Dashboard attached")
        val pool = Executors.newFixedThreadPool(2) { runnable ->
            Thread(runnable, "cleary-dashboard-http").apply { isDaemon = true }
        }
        val httpServer = HttpServer.create(InetSocketAddress(host, port), 0)
        httpServer.executor = pool
        httpServer.createContext("/") { exchange -> route(exchange) }
        httpServer.start()
        executor = pool
        server = httpServer
        return this
    }

    /** Stops the HTTP server and detaches from the scheduler. */
    fun stop() {
        server?.stop(0)
        server = null
        executor?.shutdownNow()
        executor = null
        scheduler.removeListener(listener)
    }

    // ------------------------------------------------------------------------
    // Routing
    // ------------------------------------------------------------------------

    private fun route(exchange: HttpExchange) {
        try {
            val path = exchange.requestURI.path
            when {
                path == "/" || path.isEmpty() -> serveStatic(exchange, "index.html", "text/html; charset=utf-8")
                path == "/assets/styles.css" -> serveStatic(exchange, "styles.css", "text/css; charset=utf-8")
                path == "/api/state" -> requireMethod(exchange, "GET") { handleState(exchange) }
                path == "/api/schedule/preview" -> requireMethod(exchange, "GET") { handlePreview(exchange) }
                path.startsWith("/api/tasks/") -> requireMethod(exchange, "POST") { handleAction(exchange, path) }
                else -> sendJson(exchange, 404, errorJson("not found"))
            }
        } catch (_: IOException) {
            // client went away mid-response; nothing to do
        } catch (t: Throwable) {
            runCatching { sendJson(exchange, 500, errorJson(describeError(t))) }
        } finally {
            exchange.close()
        }
    }

    private inline fun requireMethod(exchange: HttpExchange, method: String, handle: () -> Unit) {
        if (exchange.requestMethod != method) {
            sendJson(exchange, 405, errorJson("method not allowed"))
        } else {
            handle()
        }
    }

    // ------------------------------------------------------------------------
    // Handlers
    // ------------------------------------------------------------------------

    private fun handleState(exchange: HttpExchange) {
        val windowSec = (queryParam(exchange, "window")?.toIntOrNull() ?: DEFAULT_WINDOW_SEC)
            .coerceIn(60, 3_600)
        val windowMs = windowSec * 1_000L
        val tasks = scheduler.listTasks().sortedBy { it.name }
        val recent = snapshotEvents(EVENT_RESPONSE_LIMIT)

        val json = JsonWriter().obj {
            put("now", System.currentTimeMillis())
            put("window_seconds", windowSec)
            obj("stats") {
                put("total_tasks", tasks.size)
                put("enabled_tasks", tasks.count { it.enabled })
                put("running_tasks", tasks.sumOf { it.activeExecutions })
                put("total_runs", tasks.sumOf { it.runCount })
                put("total_errors", tasks.sumOf { it.failureCount })
                put("total_skips", tasks.sumOf { it.skipCount })
                put("workers", scheduler.workerConcurrency)
                put("started_at", scheduler.startedAtMillis)
            }
            arrayOfObjects("tasks", tasks.size) { i -> writeTask(tasks[i], windowMs) }
            arrayOfObjects("events", recent.size) { i ->
                val event = recent[i]
                put("at", event.at)
                put("kind", event.kind)
                put("task", event.task)
                put("message", event.message)
                put("duration_ms", event.durationMs)
            }
        }
        sendJson(exchange, 200, json)
    }

    private fun JsonObjectScope.writeTask(info: TaskInfo, windowMs: Long) {
        val (schedule, _) = scheduler.scheduleOf(info.name) ?: (null to null)
        put("name", info.name)
        put("schedule", info.scheduleDescription)
        put("expr", ScheduleExpr.canonical(schedule))
        put("meaning", ScheduleExpr.meaning(schedule))
        put("enabled", info.enabled)
        put("running", info.running)
        put("running_count", info.activeExecutions.toInt())
        put("last_run", info.lastStartedAt)
        put("next_run", info.nextScheduledAt)
        put("run_count", info.runCount)
        put("error_count", info.failureCount)
        put("skip_count", info.skipCount)
        put("rejected_count", info.rejectedCount)
        put("last_error", info.lastError?.let(::describeError))
        put("timeout_ms", info.timeout?.inWholeMilliseconds ?: 0L)
        put("allow_overlapping", info.allowConcurrent)
        put("retry", info.retryPolicy?.let { policy ->
            buildString {
                append("up to ").append(policy.maxAttempts).append(" attempts · ")
                append(ScheduleExpr.formatDuration(policy.initialDelay)).append(" delay")
                if (policy.backoffMultiplier > 1.0) append(" · ×").append(policy.backoffMultiplier)
            }
        })
        arrayOfStrings("tags", info.tags.sorted())
        arrayOfLongs("upcoming", scheduler.upcomingFireTimes(info.name, windowMs, UPCOMING_LIMIT))
    }

    private fun handlePreview(exchange: HttpExchange) {
        val expr = queryParam(exchange, "expr")?.trim().orEmpty()
        if (expr.isEmpty()) {
            sendJson(exchange, 400, JsonWriter().obj { put("ok", false); put("error", "expression is empty") })
            return
        }
        val schedule = try {
            ScheduleExpr.parse(expr)
        } catch (e: IllegalArgumentException) {
            sendJson(exchange, 400, JsonWriter().obj { put("ok", false); put("error", describeError(e)) })
            return
        }
        val fires = previewFires(schedule, count = 3)
        sendJson(exchange, 200, JsonWriter().obj {
            put("ok", true)
            put("expr", ScheduleExpr.canonical(schedule))
            put("meaning", ScheduleExpr.meaning(schedule))
            arrayOfLongs("next", fires)
        })
    }

    /** Simulates the next fire times of a schedule using a fresh, detached trigger. */
    private fun previewFires(schedule: io.github.cymoo.cleary.Schedule, count: Int): List<Long> {
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

    private fun handleAction(exchange: HttpExchange, path: String) {
        // /api/tasks/{name}/{action}
        val segments = path.removePrefix("/api/tasks/").split('/')
        if (segments.size != 2 || segments[0].isEmpty()) {
            sendJson(exchange, 404, errorJson("expected /api/tasks/{name}/{action}"))
            return
        }
        val name = URLDecoder.decode(segments[0], StandardCharsets.UTF_8)
        val action = segments[1]
        if (config.readOnly) {
            sendJson(exchange, 403, errorJson("dashboard is read-only"))
            return
        }

        try {
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
                    val body = exchange.requestBody.readBytes().toString(StandardCharsets.UTF_8)
                    val expr = extractJsonStringField(body, "expr")?.trim()
                    if (expr.isNullOrEmpty()) {
                        sendJson(exchange, 400, errorJson("request body must be JSON with a non-empty \"expr\""))
                        return
                    }
                    val schedule = ScheduleExpr.parse(expr)
                    scheduler.reschedule(name, schedule)
                    record("lifecycle", name, "Rescheduled to ${ScheduleExpr.meaning(schedule)}")
                }
                else -> {
                    sendJson(exchange, 404, errorJson("unknown action '$action'"))
                    return
                }
            }
            sendJson(exchange, 200, JsonWriter().obj { put("ok", true) })
        } catch (e: NoSuchElementException) {
            sendJson(exchange, 404, errorJson(describeError(e)))
        } catch (e: IllegalArgumentException) {
            sendJson(exchange, 400, errorJson(describeError(e)))
        } catch (e: IllegalStateException) {
            sendJson(exchange, 409, errorJson(describeError(e)))
        }
    }

    // ------------------------------------------------------------------------
    // Events
    // ------------------------------------------------------------------------

    private fun record(kind: String, task: String?, message: String, durationMs: Long = 0L) {
        val event = Event(System.currentTimeMillis(), kind, task, message, durationMs)
        eventLock.withLock {
            events.addFirst(event)
            while (events.size > config.eventHistoryLimit) events.removeLast()
        }
    }

    private fun snapshotEvents(limit: Int): List<Event> = eventLock.withLock {
        events.take(limit)
    }

    // ------------------------------------------------------------------------
    // HTTP plumbing
    // ------------------------------------------------------------------------

    private val staticCache = java.util.concurrent.ConcurrentHashMap<String, ByteArray>()

    private fun serveStatic(exchange: HttpExchange, resource: String, contentType: String) {
        if (exchange.requestMethod != "GET") {
            sendJson(exchange, 405, errorJson("method not allowed"))
            return
        }
        val bytes = staticCache.computeIfAbsent(resource) {
            Dashboard::class.java.getResourceAsStream(resource)?.readBytes()
                ?: throw IllegalStateException("Dashboard resource '$resource' missing from classpath")
        }
        exchange.responseHeaders.set("Content-Type", contentType)
        exchange.responseHeaders.set("Cache-Control", "no-cache")
        exchange.sendResponseHeaders(200, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun sendJson(exchange: HttpExchange, status: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.set("Content-Type", "application/json; charset=utf-8")
        exchange.responseHeaders.set("Cache-Control", "no-store")
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun queryParam(exchange: HttpExchange, name: String): String? {
        val query = exchange.requestURI.rawQuery ?: return null
        for (pair in query.split('&')) {
            val idx = pair.indexOf('=')
            if (idx <= 0) continue
            if (URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8) == name) {
                return URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8)
            }
        }
        return null
    }

    private fun errorJson(message: String): String =
        JsonWriter().obj {
            put("ok", false)
            put("error", message)
        }

    private fun describeError(t: Throwable): String =
        t.message?.takeIf { it.isNotBlank() } ?: t::class.simpleName ?: "error"

    companion object {
        const val DEFAULT_PORT = 8378
        private const val DEFAULT_WINDOW_SEC = 300
        private const val UPCOMING_LIMIT = 120
        private const val EVENT_RESPONSE_LIMIT = 100
    }
}
