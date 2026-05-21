package io.github.cymoo.cleary

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * Test suite for [TaskScheduler].
 *
 * Each test creates its own [TaskScheduler] instance and shuts it down in [tearDown],
 * so tests are fully isolated and can run in parallel without interfering with each
 * other's state.
 *
 * Timing-sensitive tests use generous timeouts (≥ 5× the expected delay) to remain
 * reliable on heavily loaded CI machines.
 */
@TestMethodOrder(MethodOrderer.DisplayName::class)
class TaskSchedulerTest {

    private lateinit var tm: TaskScheduler

    @AfterEach
    fun tearDown() {
        if (::tm.isInitialized) {
            tm.shutdown()
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /** Builds and starts a TaskScheduler with [concurrency] workers and optional config. */
    private fun scheduler(
        concurrency: Int = 2,
        block: TaskSchedulerConfig.() -> Unit = {}
    ): TaskScheduler = TaskScheduler {
        this.concurrency = concurrency
        autoStart = true
        block()
    }.also { tm = it }

    /** Awaits [latch] for up to [timeoutMs] ms, failing the test if it times out. */
    private fun awaitLatch(latch: CountDownLatch, timeoutMs: Long = 3_000) {
        assertTrue(latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            "Timed out waiting for latch (remaining=${latch.count})"
        }
    }

    private fun assertSuccessValue(expected: Any?, result: TaskRunResult) {
        assertTrue(result is TaskRunResult.Success) { "Expected Success but got $result" }
        assertEquals(expected, (result as TaskRunResult.Success).value)
    }

    private fun assertSuccess(result: TaskRunResult) {
        assertTrue(result is TaskRunResult.Success) { "Expected Success but got $result" }
    }

    private fun assertFailureMessage(expected: String, result: TaskRunResult) {
        assertTrue(result is TaskRunResult.Failure) { "Expected Failure but got $result" }
        assertEquals(expected, (result as TaskRunResult.Failure).error.message)
    }

    // =========================================================================
    // 1. Lifecycle
    // =========================================================================

    @Nested
    @DisplayName("Lifecycle")
    inner class Lifecycle {

        @Test
        @DisplayName("isRunning is false before start()")
        fun notRunningBeforeStart() {
            tm = TaskScheduler()          // autoStart = false by default
            assertEquals(SchedulerState.NEW, tm.state)
            assertFalse(tm.isRunning)
            assertFalse(tm.isTerminated)
        }

        @Test
        @DisplayName("isRunning is true after start()")
        fun runningAfterStart() {
            tm = TaskScheduler()
            tm.start()
            assertEquals(SchedulerState.RUNNING, tm.state)
            assertTrue(tm.isRunning)
        }

        @Test
        @DisplayName("autoStart = true starts the scheduler immediately")
        fun autoStart() {
            tm = TaskScheduler { autoStart = true }
            assertTrue(tm.isRunning)
        }

        @Test
        @DisplayName("calling start() twice is a no-op")
        fun startIdempotent() {
            tm = TaskScheduler()
            tm.start()
            assertDoesNotThrow { tm.start() }
            assertTrue(tm.isRunning)
        }

        @Test
        @DisplayName("isRunning is false after shutdown()")
        fun notRunningAfterShutdown() {
            tm = scheduler()
            tm.shutdown()
            assertEquals(SchedulerState.SHUTDOWN, tm.state)
            assertFalse(tm.isRunning)
        }

        @Test
        @DisplayName("isTerminated is true after shutdown() completes")
        fun terminatedAfterShutdown() {
            tm = scheduler()
            tm.shutdown()
            assertTrue(tm.isTerminated)
        }

        @Test
        @DisplayName("shutdown() before start() is a no-op")
        fun shutdownBeforeStart() {
            tm = TaskScheduler()
            assertDoesNotThrow { tm.shutdown() }
            assertEquals(SchedulerState.SHUTDOWN, tm.state)
        }

        @Test
        @DisplayName("run() throws when TaskScheduler is not running")
        fun runThrowsWhenNotRunning() {
            tm = TaskScheduler()
            tm.task("t") { run { } }
            assertThrows<IllegalStateException> { tm.run("t") }
        }

        @Test
        @DisplayName("shutdown scheduler is single-use and rejects mutating operations")
        fun shutdownRejectsFurtherUse() {
            tm = scheduler()
            tm.task("t") { run { } }
            tm.shutdown()

            assertThrows<IllegalStateException> { tm.start() }
            assertThrows<IllegalStateException> { tm.task("new") { run { } } }
            assertThrows<IllegalStateException> { tm.run("t") }
            assertThrows<IllegalStateException> { tm.enable("t") }
            assertThrows<IllegalStateException> { tm.disable("t") }
            assertThrows<IllegalStateException> { tm.remove("t") }
        }

        @Test
        @DisplayName("config rejects invalid concurrency and queue capacity")
        fun configRejectsInvalidValues() {
            assertThrows<IllegalArgumentException> { TaskScheduler { concurrency = 0 } }
            assertThrows<IllegalArgumentException> { TaskScheduler { queueCapacity = 0 } }
            assertThrows<IllegalArgumentException> { TaskScheduler { threadNamePrefix = "  " } }
        }
    }

    // =========================================================================
    // 2. Task Registration
    // =========================================================================

    @Nested
    @DisplayName("Task Registration")
    inner class Registration {

        @Test
        @DisplayName("registered task appears in listTaskNames()")
        fun registeredTaskIsListed() {
            tm = scheduler()
            tm.task("ping") { run { } }
            assertIn("ping", tm.listTaskNames())
        }

        @Test
        @DisplayName("exists() returns true for registered tasks")
        fun existsReturnsTrueForRegistered() {
            tm = scheduler()
            tm.task("ping") { run { } }
            assertTrue(tm.exists("ping"))
        }

        @Test
        @DisplayName("exists() returns false for unknown tasks")
        fun existsReturnsFalseForUnknown() {
            tm = scheduler()
            assertFalse(tm.exists("unknown"))
        }

        @Test
        @DisplayName("duplicate task name throws IllegalArgumentException")
        fun duplicateNameThrows() {
            tm = scheduler()
            tm.task("ping") { run { } }
            assertThrows<IllegalArgumentException> {
                tm.task("ping") { run { } }
            }
        }

        @Test
        @DisplayName("blank task name throws IllegalArgumentException")
        fun blankNameThrows() {
            tm = scheduler()
            assertThrows<IllegalArgumentException> {
                tm.task("  ") { run { } }
            }
        }

        @Test
        @DisplayName("missing run block throws IllegalStateException")
        fun missingRunBlockThrows() {
            tm = scheduler()
            assertThrows<IllegalStateException> {
                tm.task("bad") { every(Duration.ofSeconds(1)) }
            }
        }

        @Test
        @DisplayName("setting two schedules in one task throws")
        fun twoSchedulesThrows() {
            tm = scheduler()
            assertThrows<IllegalStateException> {
                tm.task("bad") {
                    every(Duration.ofSeconds(1))
                    every(Duration.ofSeconds(2))
                    run { }
                }
            }
        }

        @Test
        @DisplayName("task registered after start() is enqueued immediately")
        fun registrationAfterStartEnqueues() {
            tm = scheduler()
            val latch = CountDownLatch(1)
            tm.task("late") {
                every(Duration.ofMillis(50))
                run { latch.countDown() }
            }
            awaitLatch(latch)
        }

        @Test
        @DisplayName("getTaskInfo() returns correct metadata")
        fun getTaskInfoReturnsMetadata() {
            tm = scheduler()
            tm.task("info-task") {
                every(Duration.ofSeconds(10))
                retry(maxAttempts = 3, initialDelay = Duration.ofSeconds(1))
                run { }
            }
            val info = tm.getTaskInfo("info-task")
            assertNotNull(info)
            assertEquals("info-task", info!!.name)
            assertTrue(info.enabled)
            assertFalse(info.allowConcurrent)
            assertNotNull(info.retryPolicy)
            assertEquals(3, info.retryPolicy!!.maxAttempts)
            assertFalse(info.running)
            assertEquals(0, info.activeExecutions)
            assertNotNull(info.nextScheduledAt)
            assertNull(info.lastStartedAt)
            assertEquals(0, info.runCount)
            assertEquals(0, info.successCount)
            assertEquals(0, info.failureCount)
            assertEquals(0, info.skipCount)
            assertEquals(0, info.rejectedCount)
        }

        @Test
        @DisplayName("getTaskInfo() reports manual-only tasks without schedule metadata")
        fun getTaskInfoForManualOnlyTask() {
            tm = scheduler()
            tm.task("manual-only") { run { "ok" } }

            val info = tm.getTaskInfo("manual-only")
            assertNotNull(info)
            assertEquals("manual-only", info!!.name)
            assertNull(info.scheduleDescription)
            assertNull(info.nextScheduledAt)
            assertTrue(info.enabled)
            assertEquals(0, info.runCount)
        }

        @Test
        @DisplayName("getTaskInfo() returns null for unknown task")
        fun getTaskInfoNullForUnknown() {
            tm = scheduler()
            assertNull(tm.getTaskInfo("ghost"))
        }
    }

    // =========================================================================
    // 3. Manual Execution
    // =========================================================================

    @Nested
    @DisplayName("Manual Execution")
    inner class ManualExecution {

        @Test
        @DisplayName("run() executes the task asynchronously")
        fun runExecutesAsync() {
            tm = scheduler()
            val latch = CountDownLatch(1)
            tm.task("t") { run { latch.countDown() } }
            tm.run("t")
            awaitLatch(latch)
        }

        @Test
        @DisplayName("runBlocking() returns Success with the task's return value")
        fun runBlockingReturnsValue() {
            tm = scheduler()
            tm.task("t") { run { 42 } }
            assertSuccessValue(42, tm.runBlocking("t"))
        }

        @Test
        @DisplayName("runBlocking() returns Failure for task exception")
        fun runBlockingReturnsFailure() {
            tm = scheduler()
            tm.task("t") { run { error("boom") } }
            assertFailureMessage("boom", tm.runBlocking("t"))
        }

        @Test
        @DisplayName("runBlocking() restores interrupt flag and returns Failure when interrupted")
        fun runBlockingReturnsFailureWhenInterrupted() {
            tm = scheduler()
            val started = CountDownLatch(1)
            val blocker = CountDownLatch(1)
            val result = AtomicReference<TaskRunResult>()
            val interruptRestored = AtomicBoolean(false)

            tm.task("blocked") {
                run {
                    started.countDown()
                    blocker.await(2, TimeUnit.SECONDS)
                    "done"
                }
            }

            val thread = Thread {
                result.set(tm.runBlocking("blocked"))
                interruptRestored.set(Thread.currentThread().isInterrupted)
            }.also { it.start() }

            assertTrue(started.await(2, TimeUnit.SECONDS))
            thread.interrupt()
            thread.join(2_000)

            assertFalse(thread.isAlive)
            val outcome = result.get()
            assertTrue(outcome is TaskRunResult.Failure) { "Expected Failure but got $outcome" }
            assertTrue((outcome as TaskRunResult.Failure).error is InterruptedException)
            assertTrue(interruptRestored.get(), "interrupt flag should be restored")
            blocker.countDown()
        }

        @Test
        @DisplayName("run() passes contextValues into the task")
        fun runPassesContextValues() {
            tm = scheduler()
            val captured = AtomicReference<String>()
            tm.task("t") {
                run { captured.set(getOrNull("key")) }
            }
            tm.runBlocking("t", mapOf("key" to "hello"))
            assertEquals("hello", captured.get())
        }

        @Test
        @DisplayName("run() throws NoSuchElementException for unknown task")
        fun runThrowsForUnknownTask() {
            tm = scheduler()
            assertThrows<NoSuchElementException> { tm.run("ghost") }
        }

        @Test
        @DisplayName("run() can be called multiple times on the same task")
        fun runMultipleTimes() {
            tm = scheduler()
            val counter = AtomicInteger(0)
            tm.task("t") { run { counter.incrementAndGet() } }
            repeat(5) { tm.runBlocking("t") }
            assertEquals(5, counter.get())
        }

        @Test
        @DisplayName("global context values are visible inside task blocks")
        fun globalContextVisible() {
            val captured = AtomicReference<String>()
            tm = TaskScheduler {
                autoStart = true
                context["env"] = "test"
            }
            tm.task("t") {
                run { captured.set(getOrNull("env")) }
            }
            tm.runBlocking("t")
            assertEquals("test", captured.get())
        }

        @Test
        @DisplayName("task can write to context; change is not visible to other executions")
        fun contextWriteIsIsolated() {
            tm = scheduler()
            tm.task("t") {
                run {
                    this["temp"] = "value"
                    getOrNull<String>("temp")
                }
            }
            // Two independent executions should not share state
            val r1 = tm.runBlocking("t")
            val r2 = tm.runBlocking("t")
            assertSuccessValue("value", r1)
            assertSuccessValue("value", r2)
        }
    }

    // =========================================================================
    // 4. Scheduling — FixedRate
    // =========================================================================

    @Nested
    @DisplayName("Scheduling — FixedRate")
    inner class FixedRateScheduling {

        @Test
        @DisplayName("task runs repeatedly at the configured interval")
        fun runsRepeatedly() {
            tm = scheduler()
            val latch = CountDownLatch(3)
            tm.task("t") {
                every(Duration.ofMillis(80))
                run { latch.countDown() }
            }
            awaitLatch(latch, 2_000)
        }

        @Test
        @DisplayName("FixedRate interval must be positive")
        fun intervalMustBePositive() {
            assertThrows<IllegalArgumentException> {
                Schedule.FixedRate(Duration.ZERO)
            }
            assertThrows<IllegalArgumentException> {
                Schedule.FixedRate(Duration.ofMillis(-1))
            }
        }

        @Test
        @DisplayName("initialDelay defers the first execution")
        fun initialDelayDefers() {
            tm = scheduler()
            val firstRunAt = AtomicReference<Long>()
            val latch = CountDownLatch(1)
            val startedAt = System.currentTimeMillis()

            tm.task("t") {
                every(Duration.ofMillis(50))
                initialDelay(Duration.ofMillis(200))
                run {
                    if (firstRunAt.compareAndSet(null, System.currentTimeMillis())) {
                        latch.countDown()
                    }
                }
            }
            awaitLatch(latch, 2_000)
            val elapsed = firstRunAt.get()!! - startedAt
            assertTrue(elapsed >= 180) { "Expected ≥ 180 ms initial delay, got $elapsed ms" }
        }

        @Test
        @DisplayName("initialDelay order relative to every() does not matter")
        fun initialDelayOrderIndependent() {
            tm = scheduler()
            val latch = CountDownLatch(1)
            // initialDelay declared before every()
            tm.task("t") {
                initialDelay(Duration.ofMillis(100))
                every(Duration.ofMillis(50))
                run { latch.countDown() }
            }
            awaitLatch(latch, 2_000)
        }
    }

    // =========================================================================
    // 5. Scheduling — Cron
    // =========================================================================

    @Nested
    @DisplayName("Scheduling — Cron")
    inner class CronScheduling {

        @Test
        @DisplayName("invalid cron expression throws at registration time")
        fun invalidCronThrows() {
            tm = scheduler()
            assertThrows<IllegalArgumentException> {
                tm.task("bad-cron") {
                    cron("not-a-cron")
                    run { }
                }
            }
        }

        @Test
        @DisplayName("valid cron expression is accepted")
        fun validCronAccepted() {
            tm = scheduler()
            assertDoesNotThrow {
                tm.task("ok-cron") {
                    cron("0/1 * * * * ?")   // every second
                    run { }
                }
            }
        }

        @Test
        @DisplayName("cron task fires at least once within 3 s for every-second expression")
        fun cronFiresWithinTimeout() {
            tm = scheduler()
            val latch = CountDownLatch(1)
            tm.task("cron-task") {
                cron("0/1 * * * * ?")
                run { latch.countDown() }
            }
            awaitLatch(latch, 3_000)
        }
    }

    // =========================================================================
    // 6. Scheduling — Once
    // =========================================================================

    @Nested
    @DisplayName("Scheduling — Once")
    inner class OnceScheduling {

        @Test
        @DisplayName("once task fires exactly once")
        fun firesExactlyOnce() {
            tm = scheduler()
            val count = AtomicInteger(0)
            val latch = CountDownLatch(1)

            tm.task("once-task") {
                once(Instant.now().plusMillis(80))
                run {
                    count.incrementAndGet()
                    latch.countDown()
                }
            }
            awaitLatch(latch)
            // Give extra time to confirm it does not fire again
            Thread.sleep(300)
            assertEquals(1, count.get())
        }

        @Test
        @DisplayName("once task with past instant fires immediately")
        fun pastInstantFiresImmediately() {
            tm = scheduler()
            val latch = CountDownLatch(1)
            tm.task("once-past") {
                once(Instant.now().minusSeconds(10))
                run { latch.countDown() }
            }
            awaitLatch(latch, 2_000)
        }

        @Test
        @DisplayName("re-enabling a fired once task is a no-op")
        fun reEnablingFiredOnceIsNoop() {
            tm = scheduler()
            val count = AtomicInteger(0)
            val latch = CountDownLatch(1)

            tm.task("once-reenable") {
                once(Instant.now().plusMillis(80))
                run {
                    count.incrementAndGet()
                    latch.countDown()
                }
            }
            awaitLatch(latch)
            Thread.sleep(100)
            tm.enable("once-reenable")
            Thread.sleep(300)
            assertEquals(1, count.get())
        }
    }

    // =========================================================================
    // 7. Concurrency Guard
    // =========================================================================

    @Nested
    @DisplayName("Concurrency Guard")
    inner class ConcurrencyGuard {

        @Test
        @DisplayName("allowConcurrent=false skips overlapping executions")
        fun skipsOverlappingExecutions() {
            tm = scheduler(concurrency = 4)
            val running = AtomicInteger(0)
            val maxConcurrent = AtomicInteger(0)
            val latch = CountDownLatch(3)

            tm.task("slow") {
                every(Duration.ofMillis(50))
                // concurrent = false by default
                run {
                    val current = running.incrementAndGet()
                    maxConcurrent.getAndUpdate { max -> maxOf(max, current) }
                    Thread.sleep(200)   // longer than interval → would overlap
                    running.decrementAndGet()
                    latch.countDown()
                }
            }
            awaitLatch(latch, 5_000)
            assertEquals(1, maxConcurrent.get()) { "Expected at most 1 concurrent execution" }
        }

        @Test
        @DisplayName("concurrent(true) allows parallel executions")
        fun allowsConcurrentExecutions() {
            tm = scheduler(concurrency = 4)
            val maxConcurrent = AtomicInteger(0)
            val running = AtomicInteger(0)
            val latch = CountDownLatch(3)

            tm.task("parallel") {
                every(Duration.ofMillis(50))
                concurrent(true)
                run {
                    val cur = running.incrementAndGet()
                    maxConcurrent.getAndUpdate { max -> maxOf(max, cur) }
                    Thread.sleep(200)
                    running.decrementAndGet()
                    latch.countDown()
                }
            }
            awaitLatch(latch, 5_000)
            assertTrue(maxConcurrent.get() > 1) { "Expected >1 concurrent execution" }
        }

        @Test
        @DisplayName("scheduled overlap is reported as Skipped with counters")
        fun scheduledOverlapReportsSkippedOutcome() {
            val skippedEvents = CopyOnWriteArrayList<TaskSkippedEvent>()
            val skipped = CountDownLatch(1)
            tm = TaskScheduler {
                autoStart = true
                concurrency = 2
                onTaskSkipped = {
                    skippedEvents.add(it)
                    skipped.countDown()
                }
            }
            val started = CountDownLatch(1)
            val blocker = CountDownLatch(1)

            tm.task("scheduled-busy") {
                every(Duration.ofMillis(10))
                run {
                    started.countDown()
                    blocker.await(2, TimeUnit.SECONDS)
                }
            }

            assertTrue(started.await(2, TimeUnit.SECONDS))
            awaitLatch(skipped, 2_000)

            val event = skippedEvents.first()
            assertEquals("scheduled-busy", event.taskName)
            assertEquals(TaskExecutionType.SCHEDULED, event.executionType)
            assertEquals(TaskSkipReason.ALREADY_RUNNING, event.reason)
            assertNotNull(event.scheduledTime)
            assertTrue(tm.getTaskInfo("scheduled-busy")!!.skipCount >= 1)
            blocker.countDown()
        }

        @Test
        @DisplayName("TaskInfo reports running concurrent executions")
        fun taskInfoReportsConcurrentRunningExecutions() {
            tm = scheduler(concurrency = 2)
            val started = CountDownLatch(2)
            val blocker = CountDownLatch(1)
            tm.task("parallel") {
                concurrent(true)
                run {
                    started.countDown()
                    blocker.await(2, TimeUnit.SECONDS)
                }
            }

            val first = tm.run("parallel")
            val second = tm.run("parallel")
            awaitLatch(started)

            val info = tm.getTaskInfo("parallel")!!
            assertTrue(info.running)
            assertEquals(2, info.activeExecutions)

            blocker.countDown()
            assertSuccess(first.get())
            assertSuccess(second.get())
        }

        @Test
        @DisplayName("manual run() with allowConcurrent=false returns Skipped when busy")
        fun manualRunSkipsWhenBusy() {
            val skippedEvents = CopyOnWriteArrayList<TaskSkippedEvent>()
            tm = scheduler(concurrency = 4) {
                onTaskSkipped = { skippedEvents.add(it) }
            }
            val blocker = CountDownLatch(1)
            val started = CountDownLatch(1)

            tm.task("busy") {
                run {
                    started.countDown()
                    blocker.await(2, TimeUnit.SECONDS)
                }
            }

            // First call — starts execution
            tm.run("busy")
            assertTrue(started.await(2, TimeUnit.SECONDS))

            // Second call while first is still in progress — should be skipped
            val result = tm.runBlocking("busy")
            assertTrue(result is TaskRunResult.Skipped) { "Expected Skipped but got $result" }
            assertEquals("busy", (result as TaskRunResult.Skipped).taskName)
            assertEquals(TaskSkipReason.ALREADY_RUNNING, result.reason)
            assertEquals(1, skippedEvents.size)
            assertEquals(TaskExecutionType.MANUAL, skippedEvents[0].executionType)

            blocker.countDown()
        }

        @Test
        @DisplayName("worker queue full returns Rejected and fires onTaskRejected")
        fun workerQueueFullRejectsManualRun() {
            val rejectedEvents = CopyOnWriteArrayList<TaskRejectedEvent>()
            tm = TaskScheduler {
                autoStart = true
                concurrency = 1
                queueCapacity = 1
                onTaskRejected = { rejectedEvents.add(it) }
            }
            val started = CountDownLatch(1)
            val blocker = CountDownLatch(1)

            tm.task("queued") {
                concurrent(true)
                run {
                    started.countDown()
                    blocker.await(2, TimeUnit.SECONDS)
                }
            }

            tm.run("queued")
            assertTrue(started.await(2, TimeUnit.SECONDS))
            tm.run("queued")
            val result = tm.runBlocking("queued")

            assertTrue(result is TaskRunResult.Rejected) { "Expected Rejected but got $result" }
            assertEquals(TaskRejectedReason.WORKER_QUEUE_FULL, (result as TaskRunResult.Rejected).reason)
            assertEquals(1, rejectedEvents.size)
            assertEquals(TaskExecutionType.MANUAL, rejectedEvents[0].executionType)
            blocker.countDown()
        }

        @Test
        @DisplayName("scheduled worker queue overflow is reported as Rejected with counters")
        fun scheduledQueueOverflowReportsRejectedOutcome() {
            val rejectedEvents = CopyOnWriteArrayList<TaskRejectedEvent>()
            val rejected = CountDownLatch(1)
            tm = TaskScheduler {
                autoStart = true
                concurrency = 1
                queueCapacity = 1
                onTaskRejected = {
                    rejectedEvents.add(it)
                    rejected.countDown()
                }
            }
            val started = CountDownLatch(1)
            val blocker = CountDownLatch(1)

            tm.task("scheduled-burst") {
                every(Duration.ofMillis(10))
                concurrent(true)
                run {
                    started.countDown()
                    blocker.await(2, TimeUnit.SECONDS)
                }
            }

            assertTrue(started.await(2, TimeUnit.SECONDS))
            awaitLatch(rejected, 2_000)

            val event = rejectedEvents.first()
            assertEquals("scheduled-burst", event.taskName)
            assertEquals(TaskExecutionType.SCHEDULED, event.executionType)
            assertEquals(TaskRejectedReason.WORKER_QUEUE_FULL, event.reason)
            assertNotNull(event.scheduledTime)
            assertTrue(tm.getTaskInfo("scheduled-burst")!!.rejectedCount >= 1)
            blocker.countDown()
        }
    }

    // =========================================================================
    // 8. Retry
    // =========================================================================

    @Nested
    @DisplayName("Retry")
    inner class Retry {

        @Test
        @DisplayName("task succeeds on second attempt after one failure")
        fun succeedsOnRetry() {
            tm = scheduler()
            val attempts = AtomicInteger(0)
            tm.task("retry-ok") {
                retry(maxAttempts = 3, initialDelay = Duration.ofMillis(50))
                run {
                    if (attempts.incrementAndGet() < 2) error("fail")
                }
            }
            assertSuccess(tm.runBlocking("retry-ok"))
            assertEquals(2, attempts.get())
        }

        @Test
        @DisplayName("Failure is returned after all attempts are exhausted")
        fun failureAfterAllAttempts() {
            tm = scheduler()
            val attempts = AtomicInteger(0)
            tm.task("retry-fail") {
                retry(maxAttempts = 3, initialDelay = Duration.ofMillis(10))
                run {
                    attempts.incrementAndGet()
                    error("always fails")
                }
            }
            assertFailureMessage("always fails", tm.runBlocking("retry-fail"))
            assertEquals(3, attempts.get())
        }

        @Test
        @DisplayName("onRetry callback fires for each failed attempt except the last")
        fun onRetryFiresCorrectly() {
            val retryEvents = CopyOnWriteArrayList<TaskRetryEvent>()
            tm = TaskScheduler {
                autoStart = true
                onRetry = { retryEvents.add(it) }
            }
            tm.task("t") {
                retry(maxAttempts = 3, initialDelay = Duration.ofMillis(10))
                run { error("boom") }
            }
            assertFailureMessage("boom", tm.runBlocking("t"))
            // 3 attempts → 2 intermediate failures → 2 onRetry events
            assertEquals(2, retryEvents.size)
            assertEquals(1, retryEvents[0].failedAttempts)
            assertEquals(2, retryEvents[1].failedAttempts)
        }

        @Test
        @DisplayName("onRetry is NOT fired after the final failure")
        fun onRetryNotFiredAfterFinalFailure() {
            val retryCount = AtomicInteger(0)
            tm = TaskScheduler {
                autoStart = true
                onRetry = { retryCount.incrementAndGet() }
            }
            tm.task("t") {
                retry(maxAttempts = 2, initialDelay = Duration.ofMillis(10))
                run { error("boom") }
            }
            assertFailureMessage("boom", tm.runBlocking("t"))
            assertEquals(1, retryCount.get()) { "onRetry should fire once (between attempt 1 and 2)" }
        }

        @Test
        @DisplayName("exponential backoff: each delay is longer than the previous")
        fun exponentialBackoffIncreases() {
            val delays = CopyOnWriteArrayList<Long>()
            tm = TaskScheduler {
                autoStart = true
                onRetry = { delays.add(it.nextRetryDelayMs) }
            }
            tm.task("t") {
                retry(
                    maxAttempts = 4,
                    initialDelay = Duration.ofMillis(50),
                    backoffMultiplier = 2.0
                )
                run { error("boom") }
            }
            assertFailureMessage("boom", tm.runBlocking("t"))
            assertEquals(3, delays.size)
            // 50, 100, 200
            assertTrue(delays[0] < delays[1]) { "delays[0]=${delays[0]} should be < delays[1]=${delays[1]}" }
            assertTrue(delays[1] < delays[2]) { "delays[1]=${delays[1]} should be < delays[2]=${delays[2]}" }
        }

        @Test
        @DisplayName("constant backoff: all delays are equal")
        fun constantBackoffEqual() {
            val delays = CopyOnWriteArrayList<Long>()
            tm = TaskScheduler {
                autoStart = true
                onRetry = { delays.add(it.nextRetryDelayMs) }
            }
            tm.task("t") {
                retry(maxAttempts = 3, initialDelay = Duration.ofMillis(30), backoffMultiplier = 1.0)
                run { error("boom") }
            }
            assertFailureMessage("boom", tm.runBlocking("t"))
            assertEquals(2, delays.size)
            assertEquals(delays[0], delays[1])
        }

        @Test
        @DisplayName("retry delay is capped by maxDelay")
        fun delayCappedByMaxDelay() {
            val delays = CopyOnWriteArrayList<Long>()
            tm = TaskScheduler {
                autoStart = true
                onRetry = { delays.add(it.nextRetryDelayMs) }
            }
            tm.task("t") {
                retry(
                    maxAttempts = 5,
                    initialDelay = Duration.ofMillis(100),
                    backoffMultiplier = 10.0,
                    maxDelay = Duration.ofMillis(200)
                )
                run { error("boom") }
            }
            assertFailureMessage("boom", tm.runBlocking("t"))
            assertTrue(delays.all { it <= 200 }) { "All delays should be ≤ 200 ms: $delays" }
        }
    }

    // =========================================================================
    // 9. RetryPolicy validation
    // =========================================================================

    @Nested
    @DisplayName("RetryPolicy Validation")
    inner class RetryPolicyValidation {

        @Test
        @DisplayName("maxAttempts < 1 throws")
        fun maxAttemptsBelowOne() {
            assertThrows<IllegalArgumentException> {
                RetryPolicy(maxAttempts = 0, initialDelay = Duration.ofMillis(10))
            }
        }

        @Test
        @DisplayName("negative initialDelay throws")
        fun negativeInitialDelay() {
            assertThrows<IllegalArgumentException> {
                RetryPolicy(maxAttempts = 2, initialDelay = Duration.ofMillis(-1))
            }
        }

        @Test
        @DisplayName("backoffMultiplier < 1.0 throws")
        fun backoffMultiplierBelowOne() {
            assertThrows<IllegalArgumentException> {
                RetryPolicy(
                    maxAttempts = 2,
                    initialDelay = Duration.ofMillis(10),
                    backoffMultiplier = 0.5
                )
            }
        }

        @Test
        @DisplayName("zero maxDelay throws")
        fun zeroMaxDelay() {
            assertThrows<IllegalArgumentException> {
                RetryPolicy(
                    maxAttempts = 2,
                    initialDelay = Duration.ofMillis(10),
                    maxDelay = Duration.ZERO
                )
            }
        }

        @Test
        @DisplayName("zero initialDelay is valid")
        fun zeroInitialDelayValid() {
            assertDoesNotThrow {
                RetryPolicy(maxAttempts = 2, initialDelay = Duration.ZERO)
            }
        }
    }

    // =========================================================================
    // 10. Task Control — enable / disable / remove
    // =========================================================================

    @Nested
    @DisplayName("Task Control")
    inner class TaskControl {

        @Test
        @DisplayName("disabled task does not fire on schedule")
        fun disabledTaskDoesNotFire() {
            tm = scheduler()
            val count = AtomicInteger(0)
            tm.task("t") {
                every(Duration.ofMillis(80))
                run { count.incrementAndGet() }
            }
            tm.disable("t")
            Thread.sleep(400)
            assertEquals(0, count.get())
        }

        @Test
        @DisplayName("disabled task can still be run manually")
        fun disabledTaskCanBeRunManually() {
            tm = scheduler()
            val count = AtomicInteger(0)
            tm.task("t") { run { count.incrementAndGet() } }
            tm.disable("t")
            tm.runBlocking("t")
            assertEquals(1, count.get())
        }

        @Test
        @DisplayName("re-enabling a disabled task resumes scheduled execution")
        fun reenablingResumesSchedule() {
            tm = scheduler()
            val latch = CountDownLatch(1)
            tm.task("t") {
                every(Duration.ofMillis(80))
                run { latch.countDown() }
            }
            tm.disable("t")
            Thread.sleep(200)
            tm.enable("t")
            awaitLatch(latch, 2_000)
        }

        @Test
        @DisplayName("disable() clears next scheduled time and enable() recalculates it")
        fun disableClearsNextScheduledTimeAndEnableRestoresIt() {
            tm = scheduler()
            tm.task("poller") {
                every(Duration.ofSeconds(10))
                run { }
            }

            assertNotNull(tm.getTaskInfo("poller")!!.nextScheduledAt)
            tm.disable("poller")
            val disabledInfo = tm.getTaskInfo("poller")!!
            assertFalse(disabledInfo.enabled)
            assertNull(disabledInfo.nextScheduledAt)

            tm.enable("poller")
            val enabledInfo = tm.getTaskInfo("poller")!!
            assertTrue(enabledInfo.enabled)
            assertNotNull(enabledInfo.nextScheduledAt)
        }

        @Test
        @DisplayName("remove() deletes the task from registry")
        fun removeDeletesTask() {
            tm = scheduler()
            tm.task("t") { run { } }
            tm.remove("t")
            assertFalse(tm.exists("t"))
            assertNull(tm.getTaskInfo("t"))
        }

        @Test
        @DisplayName("remove() makes task unreachable via run()")
        fun removedTaskCannotBeRun() {
            tm = scheduler()
            tm.task("t") { run { } }
            tm.remove("t")
            assertThrows<NoSuchElementException> { tm.run("t") }
        }

        @Test
        @DisplayName("disable() on unknown task throws NoSuchElementException")
        fun disableUnknownThrows() {
            tm = scheduler()
            assertThrows<NoSuchElementException> { tm.disable("ghost") }
        }

        @Test
        @DisplayName("enable() on unknown task throws NoSuchElementException")
        fun enableUnknownThrows() {
            tm = scheduler()
            assertThrows<NoSuchElementException> { tm.enable("ghost") }
        }
    }

    // =========================================================================
    // 11. Observability — callbacks
    // =========================================================================

    @Nested
    @DisplayName("Observability — Callbacks")
    inner class Observability {

        @Test
        @DisplayName("onTaskStart fires before task executes")
        fun onTaskStartFires() {
            val startEvents = CopyOnWriteArrayList<TaskStartEvent>()
            val bodyLatch = CountDownLatch(1)

            tm = TaskScheduler {
                autoStart = true
                onTaskStart = { startEvents.add(it) }
            }
            tm.task("t") { run { bodyLatch.countDown() } }
            tm.run("t")
            awaitLatch(bodyLatch)

            assertEquals(1, startEvents.size)
            assertEquals("t", startEvents[0].taskName)
        }

        @Test
        @DisplayName("onTaskComplete fires after task executes")
        fun onTaskCompleteFires() {
            val completeEvents = CopyOnWriteArrayList<TaskCompleteEvent>()
            val doneLatch = CountDownLatch(1)

            tm = TaskScheduler {
                autoStart = true
                onTaskComplete = {
                    completeEvents.add(it)
                    doneLatch.countDown()
                }
            }
            tm.task("t") { run { "result" } }
            tm.run("t")
            awaitLatch(doneLatch)

            assertEquals(1, completeEvents.size)
            val event = completeEvents[0]
            assertEquals("t", event.taskName)
            assertTrue(event.isSuccess)
            assertNull(event.error)
            assertEquals("result", event.result)
        }

        @Test
        @DisplayName("onTaskComplete carries error on failure")
        fun onTaskCompleteCarriesError() {
            val completeEvents = CopyOnWriteArrayList<TaskCompleteEvent>()
            val doneLatch = CountDownLatch(1)

            tm = TaskScheduler {
                autoStart = true
                onTaskComplete = {
                    completeEvents.add(it)
                    doneLatch.countDown()
                }
            }
            tm.task("t") { run { error("kaboom") } }
            runCatching { tm.runBlocking("t") }
            awaitLatch(doneLatch)

            val event = completeEvents[0]
            assertFalse(event.isSuccess)
            assertNotNull(event.error)
            assertEquals("kaboom", event.error!!.message)
        }

        @Test
        @DisplayName("onTaskStart context values are visible in the task block")
        fun onTaskStartContextInjection() {
            val captured = AtomicReference<String>()
            tm = TaskScheduler {
                autoStart = true
                onTaskStart = { event -> event.context["traceId"] = "trace-123" }
            }
            tm.task("t") { run { captured.set(getOrNull("traceId")) } }
            tm.runBlocking("t")
            assertEquals("trace-123", captured.get())
        }

        @Test
        @DisplayName("TaskCompleteEvent.duration is non-negative")
        fun durationIsNonNegative() {
            val event = AtomicReference<TaskCompleteEvent>()
            tm = TaskScheduler {
                autoStart = true
                onTaskComplete = { event.set(it) }
            }
            tm.task("t") { run { Thread.sleep(20) } }
            tm.runBlocking("t")
            assertTrue(event.get().duration >= 0)
        }

        @Test
        @DisplayName("callback exceptions are isolated and reported")
        fun callbackExceptionsAreReported() {
            val errors = CopyOnWriteArrayList<SchedulerErrorEvent>()
            val completed = CountDownLatch(1)
            tm = TaskScheduler {
                autoStart = true
                onTaskStart = { error("hook boom") }
                onTaskComplete = { completed.countDown() }
                onSchedulerError = { errors.add(it) }
            }
            tm.task("t") { run { "ok" } }

            assertSuccessValue("ok", tm.runBlocking("t"))
            awaitLatch(completed)
            assertEquals(1, errors.size)
            assertEquals("t", errors[0].taskName)
            assertEquals(SchedulerErrorPhase.ON_TASK_START, errors[0].phase)
            assertEquals("hook boom", errors[0].error.message)
        }

        @Test
        @DisplayName("TaskInfo exposes runtime counters and last failure")
        fun taskInfoExposesRuntimeCounters() {
            tm = scheduler()
            val calls = AtomicInteger(0)
            tm.task("tracked") {
                run {
                    if (calls.incrementAndGet() == 1) "ok" else error("bad")
                }
            }

            assertSuccessValue("ok", tm.runBlocking("tracked"))
            assertFailureMessage("bad", tm.runBlocking("tracked"))

            val info = tm.getTaskInfo("tracked")!!
            assertEquals(2, info.runCount)
            assertEquals(1, info.successCount)
            assertEquals(1, info.failureCount)
            assertNotNull(info.lastStartedAt)
            assertNotNull(info.lastCompletedAt)
            assertNotNull(info.lastDurationMs)
            assertEquals("bad", info.lastError?.message)
            assertFalse(info.running)
        }
    }

    // =========================================================================
    // 12. Triggers (unit tests — no TaskScheduler needed)
    // =========================================================================

    @Nested
    @DisplayName("Triggers")
    inner class Triggers {

        @Test
        @DisplayName("FixedRateTrigger advances by interval each call")
        fun fixedRateTriggerAdvances() {
            val trigger = FixedRateTrigger(Duration.ofMillis(100))
            val t0 = 1_000L
            assertEquals(1_100L, trigger.nextExecutionTime(t0))
            assertEquals(1_200L, trigger.nextExecutionTime(1_100L))
        }

        @Test
        @DisplayName("OnceTrigger returns time on first call and null thereafter")
        fun onceTriggerFiresOnce() {
            val at = Instant.ofEpochMilli(5_000L)
            val trigger = OnceTrigger(at)
            assertEquals(5_000L, trigger.nextExecutionTime(0L))
            assertNull(trigger.nextExecutionTime(0L))
            assertNull(trigger.nextExecutionTime(0L))
        }

        @Test
        @DisplayName("InitialDelayTrigger shifts first execution and delegates thereafter")
        fun initialDelayTriggerShiftsOnce() {
            val inner = FixedRateTrigger(Duration.ofMillis(100))
            val trigger = InitialDelayTrigger(Duration.ofMillis(200), inner)
            val t0 = 1_000L
            // First call: inner gives 1100, then +200 delay = 1300
            assertEquals(1_300L, trigger.nextExecutionTime(t0))
            // Subsequent calls: delegate directly to inner (no more delay)
            assertEquals(1_400L, trigger.nextExecutionTime(1_300L))
            assertEquals(1_500L, trigger.nextExecutionTime(1_400L))
        }

        @Test
        @DisplayName("InitialDelayTrigger with OnceTrigger fires once at delayed time")
        fun initialDelayWithOnceTrigger() {
            val at = Instant.ofEpochMilli(1_000L)
            val inner = OnceTrigger(at)
            val trigger = InitialDelayTrigger(Duration.ofMillis(500), inner)
            // First call returns once-time + delay
            assertEquals(1_500L, trigger.nextExecutionTime(0L))
            // Second call: inner already fired, returns null
            assertNull(trigger.nextExecutionTime(1_500L))
        }

        @Test
        @DisplayName("CronTrigger rejects invalid expression")
        fun cronTriggerRejectsInvalidExpression() {
            assertThrows<IllegalArgumentException> {
                CronTrigger("not-valid", ZoneId.systemDefault())
            }
        }

        @Test
        @DisplayName("CronTrigger returns a future time for a valid expression")
        fun cronTriggerReturnsFutureTime() {
            val trigger = CronTrigger("0/1 * * * * ?", ZoneId.systemDefault())
            val next = trigger.nextExecutionTime(System.currentTimeMillis())
            assertNotNull(next)
            assertTrue(next!! > System.currentTimeMillis() - 1_100)
        }
    }

    // =========================================================================
    // 13. RetryPolicy.delayForFailedAttempts
    // =========================================================================

    @Nested
    @DisplayName("RetryPolicy.delayForFailedAttempts")
    inner class RetryPolicyDelay {

        @Test
        @DisplayName("constant backoff always returns initialDelay")
        fun constantBackoff() {
            val policy = RetryPolicy(
                maxAttempts = 5,
                initialDelay = Duration.ofMillis(100),
                backoffMultiplier = 1.0
            )
            repeat(4) { i -> assertEquals(100L, policy.delayForFailedAttempts(i)) }
        }

        @Test
        @DisplayName("exponential backoff doubles each time")
        fun exponentialBackoff() {
            val policy = RetryPolicy(
                maxAttempts = 5,
                initialDelay = Duration.ofMillis(100),
                backoffMultiplier = 2.0,
                maxDelay = Duration.ofSeconds(10)
            )
            assertEquals(100L, policy.delayForFailedAttempts(0))
            assertEquals(200L, policy.delayForFailedAttempts(1))
            assertEquals(400L, policy.delayForFailedAttempts(2))
            assertEquals(800L, policy.delayForFailedAttempts(3))
        }

        @Test
        @DisplayName("delay is capped at maxDelay")
        fun cappedAtMaxDelay() {
            val policy = RetryPolicy(
                maxAttempts = 10,
                initialDelay = Duration.ofMillis(100),
                backoffMultiplier = 10.0,
                maxDelay = Duration.ofMillis(500)
            )
            // 100 * 10^3 = 100_000 ms, but capped at 500 ms
            assertEquals(500L, policy.delayForFailedAttempts(3))
        }

        @ParameterizedTest
        @ValueSource(ints = [50, 100, 200])
        @DisplayName("large exponent with huge base stays within maxDelay (overflow guard)")
        fun overflowGuard(failedAttempts: Int) {
            val policy = RetryPolicy(
                maxAttempts = 300,
                initialDelay = Duration.ofMillis(Long.MAX_VALUE / 2),
                backoffMultiplier = 2.0,
                maxDelay = Duration.ofSeconds(30)
            )
            val delay = policy.delayForFailedAttempts(failedAttempts)
            assertTrue(delay > 0) { "Delay should be positive, got $delay" }
            assertTrue(delay <= 30_000L) { "Delay should be ≤ maxDelay (30 s), got $delay" }
        }
    }

    // =========================================================================
    // 14. Schedule descriptions
    // =========================================================================

    @Nested
    @DisplayName("Schedule descriptions")
    inner class ScheduleDescriptions {

        @Test
        @DisplayName("FixedRate describes seconds correctly")
        fun fixedRateSeconds() {
            assertEquals("every 30s", Schedule.FixedRate(Duration.ofSeconds(30)).describe())
        }

        @Test
        @DisplayName("FixedRate describes minutes correctly")
        fun fixedRateMinutes() {
            assertEquals("every 5m", Schedule.FixedRate(Duration.ofMinutes(5)).describe())
        }

        @Test
        @DisplayName("FixedRate describes hours correctly")
        fun fixedRateHours() {
            assertEquals("every 2h", Schedule.FixedRate(Duration.ofHours(2)).describe())
        }

        @Test
        @DisplayName("FixedRate describes milliseconds correctly")
        fun fixedRateMillis() {
            assertEquals("every 250ms", Schedule.FixedRate(Duration.ofMillis(250)).describe())
        }

        @Test
        @DisplayName("Once describes its instant")
        fun onceDescription() {
            val at = Instant.ofEpochMilli(0)
            assertTrue(Schedule.Once(at).describe().startsWith("once at"))
        }

        @Test
        @DisplayName("WithInitialDelay wraps inner description")
        fun withInitialDelayDescription() {
            val inner = Schedule.FixedRate(Duration.ofSeconds(10))
            val s = Schedule.WithInitialDelay(Duration.ofSeconds(5), inner).describe()
            assertTrue(s.contains("initial-delay") && s.contains("10s"))
        }
    }

    // =========================================================================
    // 15. Await
    // =========================================================================
    @Nested
    @DisplayName("Await")
    inner class Await {

        @Test
        @DisplayName("await() returns promptly after shutdown() is called")
        fun awaitReturnsAfterShutdown() {
            tm = scheduler()
            val awaitReturned = AtomicBoolean(false)
            val thread = Thread {
                tm.await()
                awaitReturned.set(true)
            }.also { it.start() }

            Thread.sleep(100)
            assertFalse(awaitReturned.get(), "await() should still be blocking")
            tm.shutdown()
            thread.join(2_000)
            assertTrue(awaitReturned.get(), "await() should have returned after shutdown()")
        }

        @Test
        @DisplayName("await() returns immediately if shutdown() was already called")
        fun awaitReturnsImmediatelyWhenAlreadyShutdown() {
            tm = scheduler()
            tm.shutdown()
            val latch = CountDownLatch(1)
            Thread { tm.await(); latch.countDown() }.start()
            awaitLatch(latch, timeoutMs = 1_000)
        }

        @Test
        @DisplayName("await() unblocks when the calling thread is interrupted")
        fun awaitUnblocksOnInterrupt() {
            tm = scheduler()
            val awaitReturned = AtomicBoolean(false)
            val thread = Thread {
                tm.await()
                awaitReturned.set(true)
            }.also { it.start() }

            Thread.sleep(100)
            thread.interrupt()
            thread.join(2_000)
            assertTrue(awaitReturned.get(), "await() should return when the thread is interrupted")
            assertTrue(thread.isInterrupted, "interrupt flag should be restored after await() returns")
        }

        @Test
        @DisplayName("multiple threads can await() the same scheduler concurrently")
        fun multipleThreadsCanAwaitConcurrently() {
            tm = scheduler()
            val latch = CountDownLatch(3)
            repeat(3) {
                Thread { tm.await(); latch.countDown() }.start()
            }
            Thread.sleep(100)
            assertEquals(3L, latch.count, "all threads should still be blocked")
            tm.shutdown()
            awaitLatch(latch, timeoutMs = 2_000)
        }
    }

    // =========================================================================
    // 16. Edge cases
    // =========================================================================

    @Nested
    @DisplayName("Edge Cases")
    inner class EdgeCases {

        @Test
        @DisplayName("manual-only task (no schedule) does not auto-fire")
        fun manualOnlyTaskDoesNotAutoFire() {
            tm = scheduler()
            val count = AtomicInteger(0)
            tm.task("manual") { run { count.incrementAndGet() } }
            Thread.sleep(300)
            assertEquals(0, count.get())
        }

        @Test
        @DisplayName("task registered without start() is not enqueued until start()")
        fun taskRegisteredBeforeStartEnqueuedOnStart() {
            tm = TaskScheduler()
            val latch = CountDownLatch(1)
            tm.task("t") {
                every(Duration.ofMillis(50))
                run { latch.countDown() }
            }
            // Not started yet — no execution should happen
            Thread.sleep(150)
            assertEquals(1, latch.count) { "Task should not fire before start()" }
            tm.start()
            awaitLatch(latch)
        }

        @Test
        @DisplayName("multiple tasks execute independently")
        fun multipleTasksIndependent() {
            tm = scheduler(concurrency = 4)
            val latchA = CountDownLatch(2)
            val latchB = CountDownLatch(2)

            tm.task("a") {
                every(Duration.ofMillis(80))
                run { latchA.countDown() }
            }
            tm.task("b") {
                every(Duration.ofMillis(100))
                run { latchB.countDown() }
            }
            awaitLatch(latchA, 2_000)
            awaitLatch(latchB, 2_000)
        }

        @Test
        @DisplayName("shutdown() waits for in-flight tasks to complete")
        fun shutdownAwaitsInFlightTasks() {
            tm = scheduler()
            val taskCompleted = AtomicBoolean(false)
            val started = CountDownLatch(1)

            tm.task("slow") {
                run {
                    started.countDown()
                    Thread.sleep(200)
                    taskCompleted.set(true)
                }
            }
            tm.run("slow")
            assertTrue(started.await(2, TimeUnit.SECONDS))
            tm.shutdown(awaitTermination = true)
            assertTrue(taskCompleted.get()) { "Task should have completed before shutdown returned" }
        }

        @Test
        @DisplayName("context key not found throws NoSuchElementException")
        fun contextKeyNotFoundThrows() {
            tm = scheduler()
            tm.task("t") {
                run {
                    assertThrows<NoSuchElementException> {
                        this.get<String>("nonexistent")
                    }
                }
            }
            tm.runBlocking("t")
        }

        @Test
        @DisplayName("context getOrNull returns null for missing key")
        fun contextGetOrNullReturnsNull() {
            tm = scheduler()
            tm.task("t") {
                run { assertNull(getOrNull<String>("missing")) }
            }
            tm.runBlocking("t")
        }

        @Test
        @DisplayName("context getOrDefault returns default for missing key")
        fun contextGetOrDefault() {
            tm = scheduler()
            tm.task("t") {
                run { assertEquals("fallback", getOrDefault("missing", "fallback")) }
            }
            tm.runBlocking("t")
        }

        @Test
        @DisplayName("context remove() deletes a key")
        fun contextRemove() {
            tm = scheduler()
            tm.task("t") {
                run {
                    this["k"] = "v"
                    this.remove("k")
                    assertNull(getOrNull<String>("k"))
                }
            }
            tm.runBlocking("t")
        }
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    private fun assertIn(element: String, collection: List<String>) {
        assertTrue(collection.contains(element)) {
            "Expected '$element' in $collection"
        }
    }

    @Nested
    inner class DurationExtensionsTest {
        // ---------- Milliseconds ----------

        @Test
        fun `Int milliseconds`() {
            assertEquals(Duration.ofMillis(500), 500.milliseconds)
            assertEquals(Duration.ofMillis(500), 500.millisecond)
        }

        @Test
        fun `Long milliseconds`() {
            assertEquals(Duration.ofMillis(500), 500L.milliseconds)
            assertEquals(Duration.ofMillis(500), 500L.millisecond)
        }

        @Test
        fun `Double milliseconds`() {
            assertEquals(Duration.ofMillis(500), 500.0.milliseconds)
            assertEquals(Duration.ofMillis(250), 250.0.milliseconds)
            // 500.5ms: allow ±1µs tolerance due to Double precision limits
            val nanos = 500.5.milliseconds.toNanos()
            assertTrue(kotlin.math.abs(nanos - 500_500_000L) <= 1_000L)
        }

        // ---------- Seconds ----------

        @Test
        fun `Int seconds`() {
            assertEquals(Duration.ofSeconds(30), 30.seconds)
            assertEquals(Duration.ofSeconds(30), 30.second)
        }

        @Test
        fun `Long seconds`() {
            assertEquals(Duration.ofSeconds(30), 30L.seconds)
            assertEquals(Duration.ofSeconds(30), 30L.second)
        }

        @Test
        fun `Double seconds`() {
            assertEquals(Duration.ofSeconds(1), 1.0.seconds)
            assertEquals(Duration.ofMillis(1500), 1.5.seconds)
            assertEquals(Duration.ofSeconds(1, 100_000_000), 1.1.seconds)
        }

        // ---------- Minutes ----------

        @Test
        fun `Int minutes`() {
            assertEquals(Duration.ofMinutes(5), 5.minutes)
            assertEquals(Duration.ofMinutes(5), 5.minute)
        }

        @Test
        fun `Long minutes`() {
            assertEquals(Duration.ofMinutes(5), 5L.minutes)
            assertEquals(Duration.ofMinutes(5), 5L.minute)
        }

        @Test
        fun `Double minutes`() {
            assertEquals(Duration.ofMinutes(1), 1.0.minutes)
            assertEquals(Duration.ofSeconds(90), 1.5.minutes)
        }

        // ---------- Hours ----------

        @Test
        fun `Int hours`() {
            assertEquals(Duration.ofHours(2), 2.hours)
            assertEquals(Duration.ofHours(2), 2.hour)
        }

        @Test
        fun `Long hours`() {
            assertEquals(Duration.ofHours(2), 2L.hours)
            assertEquals(Duration.ofHours(2), 2L.hour)
        }

        @Test
        fun `Double hours`() {
            assertEquals(Duration.ofHours(1), 1.0.hours)
            assertEquals(Duration.ofMinutes(90), 1.5.hours)
        }

        // ---------- Days ----------

        @Test
        fun `Int days`() {
            assertEquals(Duration.ofDays(7), 7.days)
            assertEquals(Duration.ofDays(7), 7.day)
        }

        @Test
        fun `Long days`() {
            assertEquals(Duration.ofDays(7), 7L.days)
            assertEquals(Duration.ofDays(7), 7L.day)
        }

        @Test
        fun `Double days`() {
            assertEquals(Duration.ofDays(1), 1.0.days)
            assertEquals(Duration.ofHours(36), 1.5.days)
        }

        // ---------- Negative values ----------

        @Test
        fun `negative Double seconds floor correctly`() {
            // -0.3.seconds should be ofSeconds(-1, 700_000_000), not ofSeconds(0, -300_000_000)
            assertEquals(Duration.ofSeconds(-1, 700_000_000L), (-0.3).seconds)
            assertEquals(Duration.ofSeconds(-2, 500_000_000L), (-1.5).seconds)
        }

        // ---------- Arithmetic sanity ----------

        @Test
        fun `durations can be composed`() {
            assertEquals(Duration.ofSeconds(90), 1.minutes + 30.seconds)
            assertEquals(Duration.ofMillis(1500), 1.seconds + 500.milliseconds)
            assertEquals(Duration.ofHours(25), 1.days + 1.hours)
        }
    }
}
