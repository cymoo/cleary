package io.github.cymoo.cleary

import io.github.cymoo.cleary.dashboard.Dashboard
import io.github.cymoo.cleary.dashboard.ScheduleExpr
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class DashboardTest {

    private lateinit var tm: TaskScheduler
    private var dashboard: Dashboard? = null

    @AfterEach
    fun tearDown() {
        dashboard?.stop()
        if (::tm.isInitialized) tm.shutdown()
    }

    private fun scheduler(block: TaskSchedulerConfig.() -> Unit = {}): TaskScheduler =
        TaskScheduler {
            concurrency = 2
            autoStart = true
            block()
        }.also { tm = it }

    // =========================================================================
    // Core support APIs
    // =========================================================================

    @Nested
    @DisplayName("Reschedule")
    inner class Reschedule {

        @Test
        @DisplayName("reschedule() swaps only the schedule, keeping body and stats")
        fun rescheduleKeepsBodyAndStats() {
            tm = scheduler()
            val ran = CopyOnWriteArrayList<String>()
            tm.task("job") {
                every(1.minutes)
                retry(maxAttempts = 2, initialDelay = Duration.ofMillis(10))
                tags("etl")
                run { ran.add("v1") }
            }
            assertSuccessLike(tm.runBlocking("job"))
            assertEquals(1, tm.getTaskInfo("job")!!.runCount)

            tm.reschedule("job", Schedule.FixedRate(Duration.ofMillis(50)))
            // wait for a scheduled run under the new cadence
            val deadline = System.currentTimeMillis() + 3_000
            while (ran.size < 2 && System.currentTimeMillis() < deadline) Thread.sleep(20)
            assertTrue(ran.size >= 2) { "Expected the same body to run under the new schedule" }

            val info = tm.getTaskInfo("job")!!
            assertTrue(info.runCount >= 2) { "Stats must carry over" }
            assertEquals(setOf("etl"), info.tags)
            assertEquals(2, info.retryPolicy!!.maxAttempts)
        }

        @Test
        @DisplayName("reschedule(null) makes the task manual-only")
        fun rescheduleToManual() {
            tm = scheduler()
            tm.task("job") {
                every(Duration.ofMillis(50))
                run { }
            }
            tm.reschedule("job", null)
            assertNull(tm.getTaskInfo("job")!!.scheduleDescription)
            assertNull(tm.getTaskInfo("job")!!.nextScheduledAt)
            assertSuccessLike(tm.runBlocking("job"))
        }

        @Test
        @DisplayName("reschedule() of unknown task throws")
        fun rescheduleUnknownThrows() {
            tm = scheduler()
            assertThrows<NoSuchElementException> { tm.reschedule("ghost", null) }
        }
    }

    @Nested
    @DisplayName("Lifecycle listeners")
    inner class Listeners {

        @Test
        @DisplayName("listeners observe events after hooks and can be removed")
        fun listenerReceivesEvents() {
            val order = CopyOnWriteArrayList<String>()
            val done = CountDownLatch(2)
            tm = scheduler {
                onTaskComplete = { order.add("hook") }
            }
            val listener = object : TaskLifecycleListener {
                override fun onTaskComplete(event: TaskCompleteEvent) {
                    order.add("listener:${event.taskName}")
                    done.countDown()
                }
            }
            tm.addListener(listener)
            tm.task("t") { run { "ok" } }
            tm.runBlocking("t")
            tm.runBlocking("t")
            assertTrue(done.await(2, TimeUnit.SECONDS))
            assertEquals(listOf("hook", "listener:t", "hook", "listener:t"), order)

            tm.removeListener(listener)
            tm.runBlocking("t")
            // the global hook still fires; only the listener is gone
            assertEquals(listOf("hook", "listener:t", "hook", "listener:t", "hook"), order)
        }

        @Test
        @DisplayName("listener exceptions are isolated from the task result")
        fun listenerExceptionsIsolated() {
            tm = scheduler()
            tm.addListener(object : TaskLifecycleListener {
                override fun onTaskComplete(event: TaskCompleteEvent) = error("listener boom")
            })
            tm.task("t") { run { "ok" } }
            assertSuccessLike(tm.runBlocking("t"))
        }
    }

    // =========================================================================
    // Schedule expressions
    // =========================================================================

    @Nested
    @DisplayName("Schedule expressions")
    inner class Expressions {

        @Test
        @DisplayName("parses every / fixed-delay / once / cron forms")
        fun parsesAllForms() {
            assertEquals(Schedule.FixedRate(90.seconds), ScheduleExpr.parse("every 90s"))
            assertEquals(Schedule.FixedRate(90.minutes), ScheduleExpr.parse("every 1h30m"))
            assertEquals(Schedule.FixedDelay(5.minutes), ScheduleExpr.parse("fixed-delay 5m"))
            assertEquals(
                Schedule.Once(Instant.parse("2030-01-01T02:00:00Z")),
                ScheduleExpr.parse("once 2030-01-01T02:00:00Z")
            )
            assertEquals(Schedule.Cron("0 */5 * * * ?"), ScheduleExpr.parse("0 */5 * * * ?"))
        }

        @Test
        @DisplayName("rejects invalid expressions")
        fun rejectsInvalid() {
            assertThrows<IllegalArgumentException> { ScheduleExpr.parse("") }
            assertThrows<IllegalArgumentException> { ScheduleExpr.parse("every soon") }
            assertThrows<IllegalArgumentException> { ScheduleExpr.parse("every 0s") }
            assertThrows<IllegalArgumentException> { ScheduleExpr.parse("once tomorrow") }
            assertThrows<IllegalArgumentException> { ScheduleExpr.parse("not a cron") }
        }

        @Test
        @DisplayName("canonical form round-trips through parse")
        fun canonicalRoundTrips() {
            for (expr in listOf("every 90s", "every 1h30m", "fixed-delay 5m", "0 */5 * * * ?")) {
                val schedule = ScheduleExpr.parse(expr)
                assertEquals(schedule, ScheduleExpr.parse(ScheduleExpr.canonical(schedule)!!))
            }
            // durations are normalized into compound units
            assertEquals("every 1m30s", ScheduleExpr.canonical(ScheduleExpr.parse("every 90s")))
            assertEquals("0 */5 * * * ?", ScheduleExpr.canonical(ScheduleExpr.parse("0 */5 * * * ?")))
            assertNull(ScheduleExpr.canonical(null))
        }

        @Test
        @DisplayName("humanizes common quartz expressions")
        fun humanizesQuartz() {
            assertEquals("every 30s", ScheduleExpr.humanizeQuartz("0/30 * * * * ?"))
            assertEquals("every 5 min", ScheduleExpr.humanizeQuartz("0 */5 * * * ?"))
            assertEquals("hourly", ScheduleExpr.humanizeQuartz("0 0 * * * ?"))
            assertEquals("daily 02:00", ScheduleExpr.humanizeQuartz("0 0 2 * * ?"))
            assertEquals("daily 09:00 on weekdays", ScheduleExpr.humanizeQuartz("0 0 9 ? * MON-FRI"))
            // unrecognized forms fall through unchanged
            assertEquals("0 0 0 1 * ?", ScheduleExpr.humanizeQuartz("0 0 0 1 * ?"))
        }
    }

    // =========================================================================
    // HTTP endpoints
    // =========================================================================

    @Nested
    @DisplayName("Dashboard HTTP")
    inner class Http {

        private val client: HttpClient = HttpClient.newHttpClient()

        private fun startDashboard(readOnly: Boolean = false): Dashboard =
            Dashboard(tm) { this.readOnly = readOnly }.start(port = 0).also { dashboard = it }

        private fun get(dash: Dashboard, path: String): HttpResponse<String> =
            client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:${dash.port}$path")).GET().build(),
                HttpResponse.BodyHandlers.ofString()
            )

        private fun post(dash: Dashboard, path: String, body: String? = null): HttpResponse<String> {
            val builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:${dash.port}$path"))
            builder.POST(HttpRequest.BodyPublishers.ofString(body ?: ""))
            return client.send(builder.build(), HttpResponse.BodyHandlers.ofString())
        }

        @Test
        @DisplayName("serves the page, stylesheet, and a state snapshot")
        fun servesPageAndState() {
            tm = scheduler()
            tm.task("demo") {
                every(Duration.ofSeconds(30))
                tags("web")
                run { "ok" }
            }
            val dash = startDashboard()

            val page = get(dash, "/")
            assertEquals(200, page.statusCode())
            assertTrue(page.body().contains("cleary"))

            val css = get(dash, "/assets/styles.css")
            assertEquals(200, css.statusCode())
            assertTrue(css.body().contains("--accent"))

            val state = get(dash, "/api/state")
            assertEquals(200, state.statusCode())
            val body = state.body()
            assertTrue(body.contains("\"name\":\"demo\"")) { body }
            assertTrue(body.contains("\"expr\":\"every 30s\"")) { body }
            assertTrue(body.contains("\"tags\":[\"web\"]")) { body }
            assertTrue(body.contains("\"total_tasks\":1")) { body }
            assertTrue(body.contains("\"upcoming\":[")) { body }
        }

        @Test
        @DisplayName("actions run, pause, resume, reschedule, and remove tasks")
        fun actionsControlTasks() {
            tm = scheduler()
            val ran = CountDownLatch(1)
            tm.task("job") {
                every(Duration.ofHours(1))
                run { ran.countDown() }
            }
            val dash = startDashboard()

            assertEquals(200, post(dash, "/api/tasks/job/run").statusCode())
            assertTrue(ran.await(2, TimeUnit.SECONDS))

            assertEquals(200, post(dash, "/api/tasks/job/pause").statusCode())
            assertFalse(tm.getTaskInfo("job")!!.enabled)
            assertEquals(200, post(dash, "/api/tasks/job/resume").statusCode())
            assertTrue(tm.getTaskInfo("job")!!.enabled)

            val rescheduled = post(dash, "/api/tasks/job/schedule", "{\"expr\": \"every 45s\"}")
            assertEquals(200, rescheduled.statusCode())
            assertEquals("every 45s", get(dash, "/api/state").body().let {
                Regex("\"expr\":\"([^\"]+)\"").find(it)?.groupValues?.get(1)
            })

            assertEquals(400, post(dash, "/api/tasks/job/schedule", "{\"expr\": \"garbage !\"}").statusCode())
            assertEquals(404, post(dash, "/api/tasks/ghost/run").statusCode())
            assertEquals(404, post(dash, "/api/tasks/job/explode").statusCode())

            assertEquals(200, post(dash, "/api/tasks/job/remove").statusCode())
            assertFalse(tm.exists("job"))
        }

        @Test
        @DisplayName("schedule preview validates and projects fire times")
        fun previewValidates() {
            tm = scheduler()
            val dash = startDashboard()

            val ok = get(dash, "/api/schedule/preview?expr=every%2030s")
            assertEquals(200, ok.statusCode())
            assertTrue(ok.body().contains("\"ok\":true"))
            assertTrue(ok.body().contains("\"meaning\":\"every 30s\""))
            assertTrue(Regex("\"next\":\\[\\d+,\\d+,\\d+]").containsMatchIn(ok.body())) { ok.body() }

            val bad = get(dash, "/api/schedule/preview?expr=nope")
            assertEquals(400, bad.statusCode())
            assertTrue(bad.body().contains("\"ok\":false"))
        }

        @Test
        @DisplayName("completed and failed runs appear in the activity feed")
        fun activityFeedRecordsRuns() {
            tm = scheduler()
            tm.task("good") { run { "ok" } }
            tm.task("bad") { run { error("boom") } }
            val dash = startDashboard()

            tm.runBlocking("good")
            tm.runBlocking("bad")

            val body = get(dash, "/api/state").body()
            assertTrue(body.contains("\"kind\":\"completed\"")) { body }
            assertTrue(body.contains("\"kind\":\"failed\"")) { body }
            assertTrue(body.contains("Failed: boom")) { body }
        }

        @Test
        @DisplayName("read-only mode rejects mutations but serves state")
        fun readOnlyRejectsMutations() {
            tm = scheduler()
            tm.task("job") { run { } }
            val dash = startDashboard(readOnly = true)

            assertEquals(403, post(dash, "/api/tasks/job/run").statusCode())
            assertEquals(403, post(dash, "/api/tasks/job/pause").statusCode())
            assertEquals(200, get(dash, "/api/state").statusCode())
        }

        @Test
        @DisplayName("stop() releases the port and detaches the listener")
        fun stopReleasesPort() {
            tm = scheduler()
            tm.task("t") { run { "ok" } }
            val dash = startDashboard()
            val port = dash.port
            dash.stop()
            dashboard = null

            assertThrows<Exception> {
                client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/api/state"))
                        .timeout(Duration.ofMillis(500)).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
                )
            }
        }
    }

    private fun assertSuccessLike(result: TaskRunResult) {
        assertTrue(result is TaskRunResult.Success) { "Expected Success but got $result" }
    }
}
