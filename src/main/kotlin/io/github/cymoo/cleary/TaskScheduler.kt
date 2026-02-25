package io.github.cymoo.cleary

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.floor
import kotlin.math.min
import kotlin.math.pow

/**
 * A lightweight task scheduler supporting cron expressions,
 * fixed-rate scheduling, and manual task execution.
 *
 * ### Quick start
 * ```kotlin
 * val tasks = TaskScheduler {
 *     autoStart = true
 *     registerShutdownHook = true
 *     context["db"] = database
 * }
 *
 * tasks.task("heartbeat") {
 *     every(5.seconds)
 *     run { println("heartbeat: $taskName") }
 * }
 *
 * tasks.task("cleanup") {
 *     cron("0 0 * * * ?")
 *     retry(maxAttempts = 3, initialDelay = 1.seconds, backoffMultiplier = 2.0)
 *     run { cleanup() }
 * }
 * ```
 *
 * Tasks may also be registered after [start] is called; they are enqueued immediately.
 *
 * ### Scheduling semantics
 * - **Cron**: next execution is anchored to the previously *scheduled* time, keeping
 *   the schedule on the wall-clock grid regardless of execution duration.
 * - **FixedRate**: next execution = last *scheduled* time + interval, preventing drift.
 * - **Once**: executes exactly once at the given [Instant].
 * - **WithInitialDelay**: delays the first execution of any inner [Schedule].
 *
 * ### Concurrency guard
 * When `allowConcurrent = false` (default), an execution that is still running when
 * the next slot arrives is simply **skipped** — no queuing, no backpressure.
 *
 * ### Retry
 * When a [RetryPolicy] is configured, a failed execution is retried on the same worker
 * thread. Between attempts the thread sleeps and correctly propagates [InterruptedException]
 * so that [shutdown] is never delayed by an in-progress retry sleep. The task's next
 * *scheduled* slot is re-enqueued before the first attempt begins, so retries never delay
 * future scheduled runs.
 *
 * ### Observability
 * [TaskSchedulerConfig.onTaskStart] and [TaskSchedulerConfig.onTaskComplete] fire once per
 * logical execution (after all retries). [TaskSchedulerConfig.onRetry] fires after each
 * failed attempt, before the next retry sleep, and is not fired after the final failure.
 */
class TaskScheduler private constructor(
    private val config: TaskSchedulerConfig
) {
    companion object {
        operator fun invoke(block: TaskSchedulerConfig.() -> Unit = {}): TaskScheduler =
            TaskScheduler(TaskSchedulerConfig().apply(block)).also { tm ->
                if (tm.config.autoStart) tm.start()
            }
    }

    // ---------- Execution Pool ----------
    private val executor: ExecutorService = run {
        val counter = AtomicLong(0)
        val factory = ThreadFactory { r ->
            Thread(r, "${config.threadNamePrefix}-worker-${counter.incrementAndGet()}")
                .apply { isDaemon = true }
        }
        ThreadPoolExecutor(
            config.concurrency,
            config.concurrency,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(10_000),
            factory,
            ThreadPoolExecutor.CallerRunsPolicy()
        )
    }

    // ---------- Scheduler ----------
    // DelayQueue.take() blocks until the head element is due.
    // Inserting an element earlier than the current head automatically wakes the
    // waiting thread — no manual sleep/interrupt tricks needed.
    private val taskQueue = DelayQueue<ScheduledTask>()
    private val schedulerRunning = AtomicBoolean(false)
    private val started = AtomicBoolean(false)
    private val schedulerThread = Thread(::runScheduler, "${config.threadNamePrefix}-scheduler")
        .apply { isDaemon = true }

    private val tasks = ConcurrentHashMap<String, TaskEntry>()

    private val globalContext = ConcurrentHashMap<String, Any>().apply {
        putAll(config.context)
    }

    // Released by shutdown() to unblock any thread waiting in await().
    private val shutdownLatch = CountDownLatch(1)


    // ---------- Lifecycle State ----------
    /**
     * True while the scheduler loop is running and accepting new task submissions.
     * Transitions to false as soon as [shutdown] is called, even before in-flight
     * tasks finish. Note that the worker pool may still be executing tasks after
     * this returns false — use [isTerminated] to check for full shutdown.
     */
    val isRunning: Boolean get() = schedulerRunning.get()

    /**
     * True after [shutdown] has been called **and** all in-flight tasks have finished
     * (or been forcibly interrupted). This is the definitive signal that the
     * TaskScheduler has fully stopped.
     */
    val isTerminated: Boolean get() = executor.isTerminated

    // =========================================================================
    // Task Registration
    // =========================================================================

    /**
     * Registers a task using a DSL builder block.
     *
     * ```kotlin
     * tasks.task("sync") {
     *     every(30.seconds)
     *     initialDelay(5.seconds)   // order relative to every/cron does not matter
     *     concurrent(false)
     *     retry(maxAttempts = 3, initialDelay = 1.seconds, backoffMultiplier = 2.0)
     *     run { doSync() }
     * }
     * ```
     *
     * Omitting a schedule creates a manual-only task triggerable via [run] / [runBlocking].
     * Tasks may be registered after [start] has been called; they are enqueued immediately.
     */
    fun task(name: String, block: TaskBuilder.() -> Unit) {
        val builder = TaskBuilder(name).apply(block)
        val taskBlock = builder.taskBlock
            ?: error("Task '$name': no run { } block defined")
        register(
            name = name,
            schedule = builder.buildSchedule(),
            allowConcurrent = builder.allowConcurrent,
            retryPolicy = builder.retryPolicy,
            block = taskBlock
        )
    }

    private fun register(
        name: String,
        schedule: Schedule?,
        allowConcurrent: Boolean,
        retryPolicy: RetryPolicy?,
        block: TaskContext.() -> Any?
    ) {
        require(name.isNotBlank()) { "Task name cannot be blank" }
        require(!tasks.containsKey(name)) { "Task '$name' is already registered" }

        val entry = TaskEntry(
            name = name,
            schedule = schedule,
            trigger = schedule?.toTrigger(),
            allowConcurrent = allowConcurrent,
            retryPolicy = retryPolicy,
            enabled = AtomicBoolean(true),
            executing = AtomicBoolean(false),
            block = block
        )
        tasks[name] = entry

        if (started.get() && entry.trigger != null && entry.enabled.get()) {
            enqueue(name, entry.trigger)
        }
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /**
     * Starts the scheduler. Safe to call only once; subsequent calls are no-ops.
     *
     * When [TaskSchedulerConfig.autoStart] is true this is called automatically at the
     * end of the `TaskScheduler { }` constructor block.
     */
    fun start() {
        if (!started.compareAndSet(false, true)) return
        schedulerRunning.set(true)
        schedulerThread.start()
        tasks.values
            .filter { it.trigger != null && it.enabled.get() }
            .forEach { enqueue(it.name, it.trigger!!) }
        if (config.registerShutdownHook) {
            Runtime.getRuntime().addShutdownHook(
                Thread({ shutdown() }, "${config.threadNamePrefix}-shutdown-hook")
            )
        }
    }

    /**
     * Shuts down the scheduler and optionally waits for in-flight tasks to complete.
     *
     * The scheduler loop is stopped first so no new tasks are submitted after this
     * point. If [awaitTermination] is true, blocks up to 30 s for running tasks to
     * finish; tasks still running after that are interrupted.
     *
     * Safe to call from a JVM shutdown hook — if the hook's grace period expires
     * before tasks finish, the JVM will terminate them forcibly.
     * Releases the [shutdownLatch] so that any thread blocked in [await] returns promptly.
     */
    fun shutdown(awaitTermination: Boolean = true) {
        if (!started.get()) return
        schedulerRunning.set(false)
        schedulerThread.interrupt()
        // Wait for the scheduler thread to stop before shutting down the executor,
        // so it cannot submit new tasks after executor.shutdown() is called.
        // A 5 s timeout guards against any unforeseen hang in the scheduler loop.
        try {
            schedulerThread.join(5_000)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
        executor.shutdown()
        if (awaitTermination) {
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) executor.shutdownNow()
            } catch (_: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            }
        } else {
            executor.shutdownNow()
        }

        shutdownLatch.countDown()
    }

    /**
     * Blocks the calling thread until [shutdown] is called or the thread is interrupted.
     *
     * Intended for use in a `main` function as a clean alternative to `Thread.sleep`
     * or a manual [CountDownLatch]. Pair with [TaskSchedulerConfig.registerShutdownHook]
     * so that a SIGTERM or Ctrl+C triggers [shutdown] automatically, allowing the
     * process to exit gracefully without any boilerplate in the caller:
     *
     * ```kotlin
     * val tasks = TaskScheduler {
     *     autoStart = true
     *     registerShutdownHook = true
     * }
     * tasks.task("ping") { … }
     * tasks.await()  // returns when the JVM begins shutting down
     * ```
     */
    fun await() {
        try {
            shutdownLatch.await()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    // =========================================================================
    // Manual Execution
    // =========================================================================

    /**
     * Submits a task for immediate asynchronous execution and returns a [Future].
     *
     * Respects the `allowConcurrent` guard: if another execution is already running
     * and `allowConcurrent = false`, the future completes with `null` immediately.
     *
     * If a [RetryPolicy] is configured, the future does not complete until all retry
     * attempts finish or the task succeeds. Retries occupy a worker thread for the
     * duration of the sleep, so keep [RetryPolicy.maxDelay] reasonable relative to
     * [TaskSchedulerConfig.concurrency].
     *
     * @throws IllegalStateException if the scheduler is not running
     * @throws IllegalStateException if [shutdown] is called concurrently
     */
    fun run(name: String, contextValues: Map<String, Any> = emptyMap()): Future<Any?> {
        check(isRunning) { "TaskScheduler is not running" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        return try {
            executor.submit(Callable {
                entry.guardedExecute {
                    executeTask(entry, contextValues, scheduledTime = null)
                }
            })
        } catch (e: RejectedExecutionException) {
            throw IllegalStateException("TaskScheduler is shutting down", e)
        }
    }

    /**
     * Runs a task synchronously on a worker thread, blocking the caller until done.
     * Re-throws any exception the task raised (after all retries are exhausted).
     *
     * @throws IllegalStateException if the scheduler is not running
     */
    fun runBlocking(name: String, contextValues: Map<String, Any> = emptyMap()): Any? =
        try {
            run(name, contextValues).get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }

    // =========================================================================
    // Task Control
    // =========================================================================

    /**
     * Re-enables a previously disabled task and re-enqueues it for its next scheduled time.
     *
     * Note: if the task uses [Schedule.Once] and has already fired, re-enabling it is a
     * no-op because the underlying trigger will not produce a future execution time.
     */
    fun enable(name: String) {
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        if (entry.enabled.compareAndSet(false, true) && started.get() && entry.trigger != null) {
            enqueue(name, entry.trigger)
        }
    }

    /**
     * Disables scheduled execution of a task.
     * In-flight executions are not interrupted.
     * Stale queue entries are silently discarded when they come due.
     */
    fun disable(name: String) {
        (tasks[name] ?: throw NoSuchElementException("Task '$name' not found")).enabled.set(false)
    }

    /**
     * Removes a task from the registry.
     * In-flight executions are not interrupted.
     * Any pending queue entry is silently discarded when it comes due.
     */
    fun remove(name: String) {
        tasks.remove(name)
    }

    // =========================================================================
    // Query
    // =========================================================================

    fun exists(name: String): Boolean = tasks.containsKey(name)
    fun listTaskNames(): List<String> = tasks.keys.toList()

    fun getTaskInfo(name: String): TaskInfo? {
        val e = tasks[name] ?: return null
        return TaskInfo(
            name = e.name,
            scheduleDescription = e.schedule?.describe(),
            enabled = e.enabled.get(),
            allowConcurrent = e.allowConcurrent,
            retryPolicy = e.retryPolicy
        )
    }

    // =========================================================================
    // Internal scheduling
    // =========================================================================

    /**
     * Computes the first scheduled time for [trigger] and adds it to the queue.
     *
     * We subtract 1 ms from "now" before calling [Trigger.nextExecutionTime] because
     * cron implementations treat the base time as exclusive (they return the next time
     * *strictly after* the base). Without this adjustment, a task registered at exactly
     * a cron boundary would silently skip that slot.
     */
    private fun enqueue(taskName: String, trigger: Trigger) {
        val nextTime = trigger.nextExecutionTime(System.currentTimeMillis() - 1) ?: return
        taskQueue.offer(ScheduledTask(taskName, nextTime))
    }

    private fun runScheduler() {
        while (schedulerRunning.get()) {
            try {
                val scheduled = taskQueue.take()
                dispatch(scheduled)
            } catch (_: InterruptedException) {
                break
            } catch (t: Throwable) {
                // Should never happen in practice — dispatch() guards its own errors
                // and taskQueue.take() is a standard JDK call. If we do land here it
                // indicates an unexpected bug; print the full stack trace so it can be
                // diagnosed, but keep the scheduler loop alive.
                t.printStackTrace()
            }
        }
    }

    private fun dispatch(scheduled: ScheduledTask) {
        // Look up the live entry by name rather than holding a direct reference in
        // ScheduledTask, so stale entries (from remove() or disable()) are naturally
        // garbage-collected once the queue drains them.
        val entry = tasks[scheduled.taskName] ?: return
        if (!entry.enabled.get()) return

        // Re-enqueue *before* submitting to the executor.
        // Anchoring to scheduledTime (not wall clock) prevents FixedRate / Cron drift.
        val nextTime = entry.trigger?.nextExecutionTime(scheduled.scheduledTime)
        if (nextTime != null) {
            taskQueue.offer(ScheduledTask(scheduled.taskName, nextTime))
        }

        try {
            executor.submit {
                entry.guardedExecute {
                    executeTask(entry, emptyMap(), scheduledTime = scheduled.scheduledTime)
                }
            }
        } catch (_: RejectedExecutionException) {
            // Executor shut down between the schedulerRunning check and submit().
            // Harmless — the scheduler loop will exit on its next iteration.
        }
    }

    private fun executeTask(
        entry: TaskEntry,
        extra: Map<String, Any>,
        scheduledTime: Long?
    ): Any? {
        val ctx = ConcurrentHashMap(globalContext).apply { putAll(extra) }
        val actualStartTime = System.currentTimeMillis()

        config.onTaskStart?.invoke(
            TaskStartEvent(
                taskName = entry.name,
                scheduledTime = scheduledTime ?: actualStartTime,
                actualTime = actualStartTime,
                context = ctx
            )
        )

        var lastError: Throwable? = null
        var result: Any? = null
        val maxAttempts = entry.retryPolicy?.maxAttempts ?: 1

        for (failedAttempts in 0 until maxAttempts) {
            try {
                result = entry.block(TaskContextImpl(entry.name, ctx))
                lastError = null
                break
            } catch (t: Throwable) {
                lastError = t
                val isLastAttempt = failedAttempts == maxAttempts - 1
                if (!isLastAttempt) {
                    val policy = entry.retryPolicy!!
                    val delayMs = policy.delayForFailedAttempts(failedAttempts)
                    config.onRetry?.invoke(
                        TaskRetryEvent(
                            taskName = entry.name,
                            failedAttempts = failedAttempts + 1,
                            maxAttempts = maxAttempts,
                            error = t,
                            nextRetryDelayMs = delayMs
                        )
                    )
                    try {
                        Thread.sleep(delayMs)
                    } catch (ie: InterruptedException) {
                        // Shutdown was requested during retry sleep.
                        // Restore the interrupt flag and abort remaining retries so the
                        // worker thread can terminate promptly.
                        Thread.currentThread().interrupt()
                        lastError = ie
                        break
                    }
                }
            }
        }

        val endTime = System.currentTimeMillis()
        config.onTaskComplete?.invoke(
            TaskCompleteEvent(entry.name, actualStartTime, endTime, result, lastError)
        )

        if (lastError != null) throw lastError
        return result
    }

    // =========================================================================
    // Internal data structures
    // =========================================================================

    private data class TaskEntry(
        val name: String,
        val schedule: Schedule?,
        val trigger: Trigger?,
        val allowConcurrent: Boolean,
        val retryPolicy: RetryPolicy?,
        val enabled: AtomicBoolean,
        val executing: AtomicBoolean,
        val block: TaskContext.() -> Any?
    )

    /**
     * Executes [block] while respecting the [TaskEntry.allowConcurrent] guard.
     *
     * If `allowConcurrent = false` and another execution is already in progress,
     * [block] is skipped and `null` is returned immediately.
     */
    private inline fun TaskEntry.guardedExecute(block: () -> Any?): Any? {
        if (!allowConcurrent && !executing.compareAndSet(false, true)) return null
        return try {
            block()
        } finally {
            if (!allowConcurrent) executing.set(false)
        }
    }

    /**
     * An element in the [DelayQueue] that becomes due at [scheduledTime].
     *
     * Stores the task name rather than a direct [TaskEntry] reference so that
     * removed tasks can be garbage-collected once the queue drains their entry,
     * instead of being kept alive by the queue itself.
     *
     * [scheduledTime] is the *planned* trigger time and is passed to
     * [Trigger.nextExecutionTime] on re-queue, keeping FixedRate and Cron schedules
     * anchored to the original time grid rather than the (potentially delayed) wall clock.
     */
    private class ScheduledTask(
        val taskName: String,
        val scheduledTime: Long
    ) : Delayed {
        override fun getDelay(unit: TimeUnit): Long =
            unit.convert(scheduledTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS)

        // `other` is always a ScheduledTask inside a single DelayQueue instance.
        override fun compareTo(other: Delayed): Int =
            scheduledTime.compareTo((other as ScheduledTask).scheduledTime)
    }
}

// =============================================================================
// Task DSL Builder
// =============================================================================

/**
 * Receiver for the `task("name") { }` DSL block.
 *
 * All setters are **order-independent**: [initialDelay] may appear before or after
 * [every]/[cron]/[once]; the final schedule is assembled in [buildSchedule].
 */
class TaskBuilder internal constructor(val name: String) {
    private var scheduleBase: Schedule? = null
    private var _initialDelay: Duration? = null

    internal var allowConcurrent: Boolean = false
        private set

    internal var retryPolicy: RetryPolicy? = null
        private set

    internal var taskBlock: (TaskContext.() -> Any?)? = null
        private set

    /**
     * Schedules the task using a Quartz cron expression, e.g. `"0 0/5 * * * ?"`.
     * @throws IllegalStateException if a schedule has already been set
     */
    fun cron(expr: String, zone: ZoneId = ZoneId.systemDefault()) {
        checkNoSchedule()
        scheduleBase = Schedule.Cron(expr, zone)
    }

    /**
     * Schedules the task at a fixed rate regardless of execution duration.
     * @throws IllegalStateException if a schedule has already been set
     */
    fun every(interval: Duration) {
        checkNoSchedule()
        scheduleBase = Schedule.FixedRate(interval)
    }

    /**
     * Schedules the task to run exactly once at [at].
     * @throws IllegalStateException if a schedule has already been set
     */
    fun once(at: Instant) {
        checkNoSchedule()
        scheduleBase = Schedule.Once(at)
    }

    /**
     * Delays the first execution by [delay]. May be called before or after
     * [every]/[cron]/[once] — order does not matter.
     * @throws IllegalStateException if an initial delay has already been set
     */
    fun initialDelay(delay: Duration) {
        check(_initialDelay == null) { "Task '$name': initialDelay already set" }
        _initialDelay = delay
    }

    /**
     * Controls whether multiple executions of this task may run concurrently.
     *
     * When `false` (the default), an execution that is still running when the next
     * scheduled slot arrives is simply skipped.
     */
    fun concurrent(allow: Boolean = true) {
        allowConcurrent = allow
    }

    /**
     * Configures a retry policy for failed executions.
     *
     * Retries block a worker thread for the sleep duration. Keep [maxDelay]
     * reasonably small relative to [TaskSchedulerConfig.concurrency] to avoid
     * pool exhaustion under sustained failures.
     *
     * @param maxAttempts        total number of attempts including the first (>= 1)
     * @param initialDelay       wait before the second attempt
     * @param backoffMultiplier  1.0 = constant delay; 2.0 = exponential backoff
     * @param maxDelay           upper bound on the per-retry sleep
     */
    fun retry(
        maxAttempts: Int,
        initialDelay: Duration,
        backoffMultiplier: Double = 1.0,
        maxDelay: Duration = Duration.ofSeconds(30)
    ) {
        retryPolicy = RetryPolicy(maxAttempts, initialDelay, backoffMultiplier, maxDelay)
    }

    /**
     * Defines the work to perform. Must be called exactly once per task.
     * @throws IllegalStateException if called more than once
     */
    fun run(block: TaskContext.() -> Any?) {
        check(taskBlock == null) { "Task '$name': run { } block already defined" }
        taskBlock = block
    }

    internal fun buildSchedule(): Schedule? {
        val base = scheduleBase ?: return null
        val delay = _initialDelay ?: return base
        // Assembled here so call order between initialDelay() and every()/cron() is irrelevant.
        return Schedule.WithInitialDelay(delay, base)
    }

    private fun checkNoSchedule() {
        check(scheduleBase == null) {
            "Task '$name': schedule already set (only one of cron/every/once is allowed)"
        }
    }
}

// =============================================================================
// Retry Policy
// =============================================================================

/**
 * Describes how a failed task execution should be retried.
 *
 * @param maxAttempts        total attempts including the first (must be >= 1)
 * @param initialDelay       wait before the second attempt (non-negative)
 * @param backoffMultiplier  1.0 = constant, 2.0 = exponential; must be >= 1.0
 * @param maxDelay           upper bound on computed delay (must be positive)
 */
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

    /**
     * Computes the sleep duration (in milliseconds) before the next retry.
     *
     * @param failedAttempts number of attempts that have already failed (0-indexed:
     *   0 means one failure has occurred and the first retry sleep is being computed)
     */
    internal fun delayForFailedAttempts(failedAttempts: Int): Long {
        require(failedAttempts >= 0)
        val baseMs = initialDelay.toMillis()
        val cappedMs = maxDelay.toMillis()
        // Short-circuit for constant delay to avoid floating-point arithmetic entirely.
        if (backoffMultiplier == 1.0) return min(baseMs, cappedMs)
        val raw = baseMs.toDouble() * backoffMultiplier.pow(failedAttempts.toDouble())
        // Guard against overflow: Infinity.toLong() returns Long.MIN_VALUE on the JVM.
        if (raw.isInfinite() || raw.isNaN() || raw >= cappedMs.toDouble()) return cappedMs
        return raw.toLong()
    }
}

// =============================================================================
// Configuration
// =============================================================================

class TaskSchedulerConfig {
    /**
     * Number of worker threads.
     *
     * Default is tuned for IO-bound workloads typical of schedulers.
     *
     * Formula: min(32, max(4, CPU cores × 4))
     *
     * This provides sufficient parallelism without excessive memory usage.
     */
    var concurrency: Int =
        minOf(32, maxOf(4, Runtime.getRuntime().availableProcessors() * 4))

    var threadNamePrefix: String = "task-scheduler"

    /**
     * When true, [TaskScheduler.start] is called automatically at the end of the
     * `TaskScheduler { }` constructor block.
     *
     * Tasks registered *after* construction are also supported: when [TaskScheduler.start] has
     * already been called, [TaskScheduler.task] enqueues the new task immediately.
     */
    var autoStart: Boolean = false

    /**
     * When true, a JVM shutdown hook is registered (in [TaskScheduler.start]) that
     * calls [TaskScheduler.shutdown] with `awaitTermination = true`.
     *
     * The JVM allows shutdown hooks a limited grace period before forcibly terminating
     * all threads. Tasks that exceed this window will be interrupted.
     */
    var registerShutdownHook: Boolean = false

    /** Key-value pairs copied into every task's execution context at creation time. */
    val context: MutableMap<String, Any> = ConcurrentHashMap()

    /**
     * Called synchronously on a worker thread immediately before the task block runs.
     * Values added to [TaskStartEvent.context] are visible to the task itself, making
     * this a good place to inject per-execution data such as trace IDs.
     */
    var onTaskStart: ((TaskStartEvent) -> Unit)? = null

    /**
     * Called after the final attempt (success or failure after all retries are exhausted).
     * [TaskCompleteEvent.error] is non-null if the task ultimately failed.
     */
    var onTaskComplete: ((TaskCompleteEvent) -> Unit)? = null

    /**
     * Called after each failed attempt when at least one retry remains.
     * Not called after the final failure (that is reported via [onTaskComplete]).
     */
    var onRetry: ((TaskRetryEvent) -> Unit)? = null
}

// =============================================================================
// Events
// =============================================================================

data class TaskStartEvent(
    val taskName: String,
    /** The time the task was *planned* to run (epoch millis). Equals [actualTime] for manual runs. */
    val scheduledTime: Long,
    /** The time execution *actually* began (epoch millis). */
    val actualTime: Long,
    /**
     * The live execution context. Values added here are visible to the task block
     * but do not affect the global context or other executions.
     */
    val context: MutableMap<String, Any>
)

data class TaskCompleteEvent(
    val taskName: String,
    val startTime: Long,
    val endTime: Long,
    val result: Any?,
    /** Non-null if all attempts failed; holds the last exception thrown. */
    val error: Throwable?
) {
    val duration: Long get() = endTime - startTime
    val isSuccess: Boolean get() = error == null
}

/**
 * Fired after each failed attempt when a retry is about to be made.
 * Not fired after the final failure.
 */
data class TaskRetryEvent(
    val taskName: String,
    /** How many attempts have failed so far (>= 1). */
    val failedAttempts: Int,
    val maxAttempts: Int,
    val error: Throwable,
    /** How long the worker thread will sleep before the next attempt (milliseconds). */
    val nextRetryDelayMs: Long
)

// =============================================================================
// Context
// =============================================================================

interface TaskContext {
    val taskName: String

    /** Returns the value for [key], throwing [NoSuchElementException] if absent or the cast fails. */
    operator fun <T : Any> get(key: String): T
    fun <T> getOrNull(key: String): T?
    fun <T> getOrDefault(key: String, default: T): T
    operator fun set(key: String, value: Any)
    fun remove(key: String)
}

internal class TaskContextImpl(
    override val taskName: String,
    private val states: MutableMap<String, Any>
) : TaskContext {
    @Suppress("UNCHECKED_CAST")
    override fun <T : Any> get(key: String): T =
        states[key] as? T ?: throw NoSuchElementException("Context key '$key' not found")

    @Suppress("UNCHECKED_CAST")
    override fun <T> getOrNull(key: String): T? = states[key] as? T

    @Suppress("UNCHECKED_CAST")
    override fun <T> getOrDefault(key: String, default: T): T = states[key] as? T ?: default

    override fun set(key: String, value: Any) {
        states[key] = value
    }

    override fun remove(key: String) {
        states.remove(key)
    }
}

// =============================================================================
// Query results
// =============================================================================

data class TaskInfo(
    val name: String,
    val scheduleDescription: String?,
    val enabled: Boolean,
    val allowConcurrent: Boolean,
    val retryPolicy: RetryPolicy?
)

// =============================================================================
// Schedule (public API)
// =============================================================================

sealed class Schedule {
    /** Quartz cron expression, e.g. `"0 0/5 * * * ?"` */
    data class Cron(val expression: String, val zone: ZoneId = ZoneId.systemDefault()) : Schedule()

    /** Execute repeatedly at a fixed rate regardless of how long each execution takes. */
    data class FixedRate(val interval: Duration) : Schedule() {
        init {
            require(!interval.isZero && !interval.isNegative) {
                "FixedRate interval must be positive, got: $interval"
            }
        }
    }

    /**
     * Execute exactly once at the given [Instant].
     *
     * If [at] is in the past when the scheduler processes it, the task fires immediately
     * (the [DelayQueue] treats a non-positive delay as "due now"). No at-construction
     * validation is performed because a `data class` `init` with `Instant.now()` would
     * break `copy()`.
     */
    data class Once(val at: Instant) : Schedule()

    /** Delay the first execution of [schedule] by [delay]. */
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

/** Formats a [Duration] as a human-readable string, choosing the largest exact unit. */
private fun Duration.toMillisDescription(): String {
    val ms = toMillis()
    return when {
        ms % 3_600_000L == 0L -> "${ms / 3_600_000L}h"
        ms % 60_000L == 0L -> "${ms / 60_000L}m"
        ms % 1_000L == 0L -> "${ms / 1_000L}s"
        else -> "${ms}ms"
    }
}

// =============================================================================
// Triggers (internal)
// =============================================================================

internal interface Trigger {
    /**
     * Returns the next execution time in epoch-millis, or `null` if there are no
     * further executions.
     *
     * @param lastScheduledTime epoch-millis of the **previously planned** trigger time,
     *   not the actual wall-clock time of the last execution. Anchoring to the planned
     *   time is what keeps FixedRate and Cron schedules from drifting under load.
     */
    fun nextExecutionTime(lastScheduledTime: Long): Long?
}

internal class CronTrigger(expression: String, private val zone: ZoneId) : Trigger {
    private val executionTime: ExecutionTime = try {
        ExecutionTime.forCron(cronParser.parse(expression).also { it.validate() })
    } catch (e: Exception) {
        throw IllegalArgumentException("Invalid cron expression: '$expression'", e)
    }

    override fun nextExecutionTime(lastScheduledTime: Long): Long? {
        // Anchor to the last *planned* time so the cron stays on the wall-clock grid.
        val base = ZonedDateTime.ofInstant(Instant.ofEpochMilli(lastScheduledTime), zone)
        return executionTime.nextExecution(base).map { it.toInstant().toEpochMilli() }.orElse(null)
    }

    companion object {
        // CronParser is thread-safe and stateless after construction; share one instance.
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
    /**
     * Tracks whether the first scheduled time has been computed.
     *
     * On the very first call (from [TaskScheduler.enqueue]), we ask [inner] for its
     * own first execution time and add [delay] on top of it. This ensures [inner]'s
     * state is consumed exactly once per logical execution, keeping the number of
     * `inner.nextExecutionTime` calls in sync with the number of actual task runs.
     *
     * Without this approach, [TaskScheduler.enqueue] and [TaskScheduler.dispatch] would each
     * call [nextExecutionTime] independently, causing [inner]'s one-shot state (e.g.
     * [OnceTrigger.fired]) to be consumed at enqueue time rather than at dispatch time —
     * which produces a stale, already-expired queue entry and a spurious second execution.
     */
    private val initialized = AtomicBoolean(false)

    override fun nextExecutionTime(lastScheduledTime: Long): Long? {
        return if (initialized.compareAndSet(false, true)) {
            // First call (enqueue): delegate to inner to consume its initial state,
            // then shift the resulting time forward by the requested delay.
            val innerFirst = inner.nextExecutionTime(lastScheduledTime) ?: return null
            innerFirst + delay.toMillis()
        } else {
            // Subsequent calls (dispatch re-enqueue): forward directly to inner.
            inner.nextExecutionTime(lastScheduledTime)
        }
    }
}

// ---------- Internal utility ----------

/**
 * Converts a fractional number of seconds to a [Duration] with nanosecond precision.
 *
 * Uses [floor] to correctly handle negative values (e.g. -0.3.seconds should round
 * toward negative infinity, not toward zero). The nanosecond remainder is then computed
 * from the floored boundary to stay non-negative, matching [Duration] semantics.
 */
private fun Double.toDurationOfSeconds(): Duration {
    val seconds = floor(this).toLong()
    val nanos = ((this - seconds) * 1_000_000_000L).toLong()
    return Duration.ofSeconds(seconds, nanos)
}


// ---------- Milliseconds ----------

val Int.millisecond: Duration get() = toLong().millisecond
val Int.milliseconds: Duration get() = toLong().milliseconds

val Long.millisecond: Duration get() = Duration.ofMillis(this)
val Long.milliseconds: Duration get() = Duration.ofMillis(this)

val Double.millisecond: Duration get() = (this / 1_000).toDurationOfSeconds()
val Double.milliseconds: Duration get() = millisecond


// ---------- Seconds ----------

val Int.second: Duration get() = toLong().second
val Int.seconds: Duration get() = toLong().seconds

val Long.second: Duration get() = Duration.ofSeconds(this)
val Long.seconds: Duration get() = Duration.ofSeconds(this)

val Double.second: Duration get() = toDurationOfSeconds()
val Double.seconds: Duration get() = second


// ---------- Minutes ----------

val Int.minute: Duration get() = toLong().minute
val Int.minutes: Duration get() = toLong().minutes

val Long.minute: Duration get() = Duration.ofMinutes(this)
val Long.minutes: Duration get() = Duration.ofMinutes(this)

val Double.minute: Duration get() = (this * 60).toDurationOfSeconds()
val Double.minutes: Duration get() = minute


// ---------- Hours ----------

val Int.hour: Duration get() = toLong().hour
val Int.hours: Duration get() = toLong().hours

val Long.hour: Duration get() = Duration.ofHours(this)
val Long.hours: Duration get() = Duration.ofHours(this)

val Double.hour: Duration get() = (this * 3_600).toDurationOfSeconds()
val Double.hours: Duration get() = hour


// ---------- Days ----------

val Int.day: Duration get() = toLong().day
val Int.days: Duration get() = toLong().days

val Long.day: Duration get() = Duration.ofDays(this)
val Long.days: Duration get() = Duration.ofDays(this)

val Double.day: Duration get() = (this * 86_400).toDurationOfSeconds()
val Double.days: Duration get() = day