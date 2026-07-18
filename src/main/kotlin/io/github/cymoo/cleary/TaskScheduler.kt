package io.github.cymoo.cleary

import java.lang.System.Logger.Level
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max
import kotlin.time.Duration

/** Creates and configures a [TaskScheduler]. */
fun taskScheduler(block: TaskSchedulerConfig.() -> Unit = {}): TaskScheduler =
    TaskScheduler(block)

/**
 * A lightweight task scheduler supporting cron expressions, fixed-rate and
 * fixed-delay scheduling, one-shot execution, retries, timeouts, manual runs,
 * and runtime task management.
 *
 * A [TaskScheduler] instance is single-use: after [shutdown] it cannot be started
 * again and no new tasks can be registered.
 */
class TaskScheduler private constructor(
    private val config: TaskSchedulerConfig
) {
    companion object {
        private val logger: System.Logger = System.getLogger("io.github.cymoo.cleary")
        private const val SCHEDULER_JOIN_TIMEOUT_MS = 5_000L

        operator fun invoke(block: TaskSchedulerConfig.() -> Unit = {}): TaskScheduler =
            TaskScheduler(TaskSchedulerConfig().apply(block)).also { scheduler ->
                if (scheduler.config.autoStart) scheduler.start()
            }
    }

    private val executor: ExecutorService = run {
        val counter = AtomicLong(0)
        val factory = ThreadFactory { runnable ->
            Thread(runnable, "${config.threadNamePrefix}-worker-${counter.incrementAndGet()}")
                .apply { isDaemon = true }
        }
        ThreadPoolExecutor(
            config.concurrency,
            config.concurrency,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(config.queueCapacity),
            factory,
            ThreadPoolExecutor.AbortPolicy()
        )
    }

    private val taskQueue = DelayQueue<QueueItem>()
    private val lifecycleState = AtomicReference(SchedulerState.NEW)
    private val schedulerThread = Thread(::runScheduler, "${config.threadNamePrefix}-scheduler")
        .apply { isDaemon = true }
    private val tasks = ConcurrentHashMap<String, TaskEntry>()
    private val globalContext: Map<String, Any> = HashMap(config.context)
    private val shutdownLatch = CountDownLatch(1)

    /** Executions whose future has not been settled yet; swept on shutdown so no caller hangs. */
    private val livePendings = ConcurrentHashMap.newKeySet<PendingExecution>()

    /** Current lifecycle state of this single-use scheduler. */
    val state: SchedulerState get() = lifecycleState.get()

    /** True after [start] succeeds and before [shutdown] begins. */
    val isRunning: Boolean get() = state == SchedulerState.RUNNING

    /** True after shutdown has completed and the worker pool has terminated. */
    val isTerminated: Boolean get() = state == SchedulerState.SHUTDOWN && executor.isTerminated

    /** Registers a task. Omitting a schedule creates a manual-only task. */
    fun task(name: String, block: TaskBuilder.() -> Unit) {
        ensureAcceptsConfiguration()
        require(name.isNotBlank()) { "Task name cannot be blank" }
        val entry = buildEntry(name, block, stats = TaskStats(), defaultEnabled = true)
        require(tasks.putIfAbsent(name, entry) == null) { "Task '$name' is already registered" }
        if (state == SchedulerState.RUNNING) armInitial(entry)
    }

    /**
     * Replaces a registered task's definition while preserving its statistics and,
     * unless [TaskBuilder.enabled] is set, its enabled state. In-flight executions
     * of the previous definition run to completion.
     */
    fun replace(name: String, block: TaskBuilder.() -> Unit) {
        ensureAcceptsConfiguration()
        val old = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        val entry = buildEntry(name, block, stats = old.stats, defaultEnabled = old.enabled.get())
        tasks[name] = entry
        cancelStream(old)
        if (state == SchedulerState.RUNNING) armInitial(entry)
    }

    private fun buildEntry(
        name: String,
        block: TaskBuilder.() -> Unit,
        stats: TaskStats,
        defaultEnabled: Boolean
    ): TaskEntry {
        val builder = TaskBuilder(name).apply(block)
        val taskBlock = builder.taskBlock
        require(taskBlock != null) { "Task '$name': no run { } block defined" }
        val schedule = builder.schedule
        val initialDelay = builder.initialDelayValue
        return TaskEntry(
            name = name,
            schedule = schedule,
            initialDelay = initialDelay,
            trigger = schedule?.toTrigger(initialDelay),
            allowConcurrent = builder.allowConcurrent,
            retryPolicy = builder.retryPolicy,
            timeout = builder.timeoutValue,
            tags = builder.tagsValue,
            hooks = builder.buildHooks(),
            enabled = AtomicBoolean(builder.enabledValue ?: defaultEnabled),
            stats = stats,
            block = taskBlock
        )
    }

    /** Starts the scheduler and arms all enabled scheduled tasks. */
    fun start() {
        when {
            lifecycleState.compareAndSet(SchedulerState.NEW, SchedulerState.RUNNING) -> {
                schedulerThread.start()
                tasks.values.forEach(::armInitial)
                if (config.registerShutdownHook) {
                    Runtime.getRuntime().addShutdownHook(
                        Thread({ shutdown() }, "${config.threadNamePrefix}-shutdown-hook")
                    )
                }
            }
            state == SchedulerState.RUNNING -> return
            else -> error("TaskScheduler has been shut down and cannot be restarted")
        }
    }

    /** Stops scheduling new work and optionally waits for in-flight executions. */
    fun shutdown(awaitTermination: Boolean = true) {
        val previous = lifecycleState.getAndSet(SchedulerState.SHUTDOWN)
        if (previous == SchedulerState.SHUTDOWN) return

        schedulerThread.interrupt()
        if (previous == SchedulerState.RUNNING) {
            try {
                schedulerThread.join(SCHEDULER_JOIN_TIMEOUT_MS)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }

        failPendingRetries()

        if (awaitTermination) {
            executor.shutdown()
            try {
                if (!executor.awaitTermination(config.shutdownTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow()
                }
            } catch (_: InterruptedException) {
                executor.shutdownNow()
                Thread.currentThread().interrupt()
            }
        } else {
            executor.shutdownNow()
        }

        // shutdownNow drops queued-but-unstarted attempts; settle their futures so no caller hangs.
        for (pending in livePendings) {
            finish(pending, null, pending.lastError ?: InterruptedException("Scheduler shut down"))
        }

        shutdownLatch.countDown()
    }

    /** Blocks until [shutdown] is called or the waiting thread is interrupted. */
    fun await() {
        try {
            shutdownLatch.await()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /**
     * Submits a registered task for immediate manual execution. Manual runs execute
     * even when the task is disabled and never affect its schedule.
     */
    fun run(name: String, contextValues: Map<String, Any> = emptyMap()): CompletableFuture<TaskRunResult> {
        check(state == SchedulerState.RUNNING) { "TaskScheduler is not running" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        return submitExecution(entry, scheduledTime = null, contextValues, TaskExecutionType.MANUAL)
    }

    /** Runs a task manually and waits for its explicit [TaskRunResult]. */
    fun runBlocking(name: String, contextValues: Map<String, Any> = emptyMap()): TaskRunResult =
        try {
            run(name, contextValues).get()
        } catch (e: ExecutionException) {
            TaskRunResult.Failure(e.cause ?: e)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            TaskRunResult.Failure(e)
        }

    /** Enables a task and arms its schedule if the scheduler is active. */
    fun enable(name: String) {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        if (entry.enabled.compareAndSet(false, true) && state == SchedulerState.RUNNING) {
            armInitial(entry)
        }
    }

    /** Disables a task and cancels its pending scheduled fire. In-flight executions finish. */
    fun disable(name: String) {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        entry.enabled.set(false)
        cancelStream(entry)
        entry.stats.nextScheduledAt = null
    }

    /** Removes a task from the registry and cancels its pending scheduled fire. */
    fun remove(name: String) {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
        tasks.remove(name)?.let(::cancelStream)
    }

    /** Returns whether a task is currently registered. */
    fun exists(name: String): Boolean = tasks.containsKey(name)

    /** Lists registered task names in unspecified order. */
    fun listTaskNames(): List<String> = tasks.keys.toList()

    /** Returns runtime snapshots of all registered tasks in unspecified order. */
    fun listTasks(): List<TaskInfo> = tasks.values.map { it.toTaskInfo() }

    /** Returns runtime snapshots of all registered tasks carrying [tag]. */
    fun listTasks(tag: String): List<TaskInfo> =
        tasks.values.filter { tag in it.tags }.map { it.toTaskInfo() }

    /** Returns a runtime snapshot for a task, or null when it is not registered. */
    fun getTaskInfo(name: String): TaskInfo? = tasks[name]?.toTaskInfo()

    // ------------------------------------------------------------------------
    // Scheduling
    // ------------------------------------------------------------------------

    /**
     * Arms a task's schedule stream. [TaskEntry.currentFire] guarantees at most one
     * live [QueueItem.Fire] per entry, so concurrent arm attempts (registration
     * during [start], rapid enable cycles) cannot create duplicate streams.
     */
    private fun armInitial(entry: TaskEntry) {
        val trigger = entry.trigger ?: return
        if (!entry.enabled.get()) return
        val time = trigger.initialExecutionTime(System.currentTimeMillis())
        if (time == null) {
            entry.stats.nextScheduledAt = null
            return
        }
        offerFire(entry, time)
    }

    private fun offerFire(entry: TaskEntry, time: Long) {
        val fire = QueueItem.Fire(entry, time)
        if (entry.currentFire.compareAndSet(null, fire)) {
            entry.stats.nextScheduledAt = time
            taskQueue.offer(fire)
        }
    }

    private fun cancelStream(entry: TaskEntry) {
        entry.currentFire.getAndSet(null)?.let { taskQueue.remove(it) }
    }

    private fun runScheduler() {
        while (state == SchedulerState.RUNNING) {
            try {
                when (val item = taskQueue.take()) {
                    is QueueItem.Fire -> dispatchFire(item)
                    is QueueItem.Retry -> submitAttempt(item.pending)
                    is QueueItem.Timeout -> fireTimeout(item.attempt)
                }
            } catch (_: InterruptedException) {
                break
            } catch (t: Throwable) {
                reportSchedulerError(null, SchedulerErrorPhase.SCHEDULER_LOOP, t)
            }
        }
    }

    private fun dispatchFire(fire: QueueItem.Fire) {
        val entry = fire.entry
        if (tasks[entry.name] !== entry || !entry.enabled.get()) {
            clearFire(entry, fire)
            return
        }
        rearmFromFire(entry, fire)
        submitExecution(entry, fire.time, emptyMap(), TaskExecutionType.SCHEDULED)
    }

    /** Re-arms the next fire before this one executes, so retries and slow runs never delay it. */
    private fun rearmFromFire(entry: TaskEntry, fire: QueueItem.Fire) {
        val trigger = entry.trigger ?: return
        if (entry.rearmAfterCompletion) {
            // FixedDelay: the next fire is armed when this execution completes.
            clearFire(entry, fire)
            return
        }
        val minTime = when (config.misfirePolicy) {
            MisfirePolicy.CATCH_UP -> fire.time
            MisfirePolicy.SKIP -> max(fire.time, System.currentTimeMillis())
        }
        val next = trigger.nextExecutionTime(fire.time, minTime)
        if (next == null) {
            clearFire(entry, fire)
            return
        }
        val nextFire = QueueItem.Fire(entry, next)
        if (entry.currentFire.compareAndSet(fire, nextFire)) {
            entry.stats.nextScheduledAt = next
            taskQueue.offer(nextFire)
        }
    }

    private fun clearFire(entry: TaskEntry, fire: QueueItem.Fire) {
        entry.currentFire.compareAndSet(fire, null)
        entry.stats.nextScheduledAt = null
    }

    private fun rearmAfterCompletion(entry: TaskEntry) {
        if (state != SchedulerState.RUNNING || tasks[entry.name] !== entry || !entry.enabled.get()) return
        val trigger = entry.trigger ?: return
        val now = System.currentTimeMillis()
        val next = trigger.nextExecutionTime(now, now)
        if (next != null) offerFire(entry, next) else entry.stats.nextScheduledAt = null
    }

    // ------------------------------------------------------------------------
    // Execution
    // ------------------------------------------------------------------------

    private fun submitExecution(
        entry: TaskEntry,
        scheduledTime: Long?,
        contextValues: Map<String, Any>,
        type: TaskExecutionType
    ): CompletableFuture<TaskRunResult> {
        val future = CompletableFuture<TaskRunResult>()
        if (type == TaskExecutionType.SCHEDULED && entry.rearmAfterCompletion) {
            future.whenComplete { _, _ -> rearmAfterCompletion(entry) }
        }
        // Fast-path overlap check: avoids burning a worker queue slot per skip.
        if (!entry.allowConcurrent && entry.executing.get()) {
            recordSkip(entry, scheduledTime, type)
            future.complete(TaskRunResult.Skipped(entry.name, TaskSkipReason.ALREADY_RUNNING))
            return future
        }
        val context = TaskContextImpl(entry.name, globalContext) { state == SchedulerState.SHUTDOWN }
        context.seed(contextValues)
        val pending = PendingExecution(entry, scheduledTime, type, context, future)
        livePendings.add(pending)
        submitAttempt(pending)
        return future
    }

    private fun submitAttempt(pending: PendingExecution) {
        try {
            executor.execute {
                try {
                    runAttempt(pending)
                } catch (t: Throwable) {
                    reportSchedulerError(pending.entry.name, SchedulerErrorPhase.SCHEDULER_LOOP, t)
                    finish(pending, null, t)
                }
            }
        } catch (_: RejectedExecutionException) {
            val lastError = pending.lastError
            if (pending.started && lastError != null) {
                // A retry attempt could not be queued: settle with the last failure.
                finish(pending, null, lastError)
            } else {
                recordRejected(pending.entry, pending.scheduledTime, pending.type)
                settle(pending, TaskRunResult.Rejected(pending.entry.name, TaskRejectedReason.WORKER_QUEUE_FULL))
            }
        }
    }

    private fun runAttempt(pending: PendingExecution) {
        // Clear any interrupt a previous attempt's timeout watchdog may have leaked onto this thread.
        Thread.interrupted()
        val entry = pending.entry
        if (!pending.started) {
            if (!entry.beginExecution()) {
                recordSkip(entry, pending.scheduledTime, pending.type)
                settle(pending, TaskRunResult.Skipped(entry.name, TaskSkipReason.ALREADY_RUNNING))
                return
            }
            pending.started = true
            pending.startedAt = System.currentTimeMillis()
            entry.stats.markStarted(pending.startedAt)
            val startEvent = TaskStartEvent(
                taskName = entry.name,
                scheduledTime = pending.scheduledTime ?: pending.startedAt,
                actualTime = pending.startedAt,
                context = pending.context
            )
            fireHooks(entry.name, SchedulerErrorPhase.ON_TASK_START, startEvent, entry.hooks.onStart, config.onTaskStart)
        }

        val timeout = entry.timeout
        var attempt: Attempt? = null
        var timeoutItem: QueueItem.Timeout? = null
        if (timeout != null) {
            attempt = Attempt(Thread.currentThread())
            timeoutItem = QueueItem.Timeout(
                attempt,
                saturatedAdd(System.currentTimeMillis(), timeout.inWholeMilliseconds)
            )
            taskQueue.offer(timeoutItem)
        }

        var result: Any? = null
        var error: Throwable? = null
        try {
            result = entry.block(pending.context)
        } catch (t: Throwable) {
            error = t
        }

        var timedOut = false
        if (attempt != null) {
            timedOut = !attempt.done.compareAndSet(false, true) && attempt.timedOut
            if (timedOut) Thread.interrupted() // clear the watchdog's interrupt before returning to the pool
            taskQueue.remove(timeoutItem)     // drop the dead watchdog instead of letting it linger
        }

        when {
            error == null -> finish(pending, result, null)
            error is InterruptedException && !timedOut -> {
                // Interrupted from outside (shutdownNow): never retried.
                if (state == SchedulerState.SHUTDOWN) Thread.currentThread().interrupt()
                finish(pending, null, error)
            }
            error is Error -> finish(pending, null, error)
            else -> {
                val effective =
                    if (timedOut) TaskTimeoutException(entry.name, timeout!!, error) else error
                scheduleRetryOrFinish(pending, effective)
            }
        }
    }

    private fun scheduleRetryOrFinish(pending: PendingExecution, error: Throwable) {
        val entry = pending.entry
        pending.failedAttempts++
        val policy = entry.retryPolicy
        if (policy == null || pending.failedAttempts >= policy.maxAttempts) {
            finish(pending, null, error)
            return
        }
        pending.lastError = error
        val delayMs = policy.delayForFailedAttempts(pending.failedAttempts - 1)
        val retryEvent = TaskRetryEvent(
            taskName = entry.name,
            failedAttempts = pending.failedAttempts,
            maxAttempts = policy.maxAttempts,
            error = error,
            nextRetryDelayMs = delayMs
        )
        fireHooks(entry.name, SchedulerErrorPhase.ON_RETRY, retryEvent, entry.hooks.onRetry, config.onRetry)

        // Retries wait in the delay queue instead of sleeping on this worker thread.
        val item = QueueItem.Retry(pending, System.currentTimeMillis() + delayMs)
        taskQueue.offer(item)
        // If shutdown raced with the offer, reclaim the item and settle now.
        if (state == SchedulerState.SHUTDOWN && taskQueue.remove(item)) {
            finish(pending, null, error)
        }
    }

    private fun fireTimeout(attempt: Attempt) {
        attempt.timedOut = true
        if (attempt.done.compareAndSet(false, true)) {
            attempt.thread.interrupt()
        }
    }

    private fun finish(pending: PendingExecution, result: Any?, error: Throwable?) {
        if (!pending.finished.compareAndSet(false, true)) return
        livePendings.remove(pending)
        val entry = pending.entry
        if (pending.started) {
            entry.endExecution()
            val event = TaskCompleteEvent(entry.name, pending.startedAt, System.currentTimeMillis(), result, error)
            entry.stats.markCompleted(event)
            fireHooks(entry.name, SchedulerErrorPhase.ON_TASK_COMPLETE, event, entry.hooks.onComplete, config.onTaskComplete)
            if (error != null && entry.hooks.onComplete == null && config.onTaskComplete == null) {
                logger.log(Level.WARNING, "Task '${entry.name}' failed", error)
            }
        }
        pending.future.complete(
            if (error == null) TaskRunResult.Success(result) else TaskRunResult.Failure(error)
        )
    }

    /** Settles an execution that never ran (skipped or rejected) with an explicit outcome. */
    private fun settle(pending: PendingExecution, outcome: TaskRunResult) {
        if (!pending.finished.compareAndSet(false, true)) return
        livePendings.remove(pending)
        pending.future.complete(outcome)
    }

    /** Settles retries still waiting in the queue when the scheduler shuts down. */
    private fun failPendingRetries() {
        for (item in taskQueue.toList()) {
            if (taskQueue.remove(item) && item is QueueItem.Retry) {
                val pending = item.pending
                finish(pending, null, pending.lastError ?: InterruptedException("Scheduler shut down"))
            }
        }
    }

    // ------------------------------------------------------------------------
    // Events
    // ------------------------------------------------------------------------

    private fun recordSkip(entry: TaskEntry, scheduledTime: Long?, type: TaskExecutionType) {
        val event = TaskSkippedEvent(
            taskName = entry.name,
            scheduledTime = scheduledTime,
            skippedAt = System.currentTimeMillis(),
            executionType = type,
            reason = TaskSkipReason.ALREADY_RUNNING
        )
        entry.stats.skipCount.incrementAndGet()
        fireHooks(entry.name, SchedulerErrorPhase.ON_TASK_SKIPPED, event, entry.hooks.onSkipped, config.onTaskSkipped)
    }

    private fun recordRejected(entry: TaskEntry, scheduledTime: Long?, type: TaskExecutionType) {
        val event = TaskRejectedEvent(
            taskName = entry.name,
            scheduledTime = scheduledTime,
            rejectedAt = System.currentTimeMillis(),
            executionType = type,
            reason = TaskRejectedReason.WORKER_QUEUE_FULL
        )
        entry.stats.rejectedCount.incrementAndGet()
        fireHooks(entry.name, SchedulerErrorPhase.ON_TASK_REJECTED, event, entry.hooks.onRejected, config.onTaskRejected)
    }

    private fun ensureAcceptsConfiguration() {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
    }

    private inline fun safeHook(taskName: String?, phase: SchedulerErrorPhase, block: () -> Unit) {
        try {
            block()
        } catch (t: Throwable) {
            reportSchedulerError(taskName, phase, t)
        }
    }

    /** Fires the task-level hook then the global hook, isolating failures of each. */
    private fun <T> fireHooks(
        taskName: String,
        phase: SchedulerErrorPhase,
        event: T,
        taskHook: ((T) -> Unit)?,
        globalHook: ((T) -> Unit)?
    ) {
        safeHook(taskName, phase) { taskHook?.invoke(event) }
        safeHook(taskName, phase) { globalHook?.invoke(event) }
    }

    private fun reportSchedulerError(taskName: String?, phase: SchedulerErrorPhase, error: Throwable) {
        val handler = config.onSchedulerError
        if (handler == null) {
            logger.log(Level.ERROR, "Scheduler error in phase $phase (task: $taskName)", error)
            return
        }
        try {
            handler(SchedulerErrorEvent(taskName, phase, error))
        } catch (secondary: Throwable) {
            secondary.addSuppressed(error)
            logger.log(Level.ERROR, "onSchedulerError hook threw", secondary)
        }
    }

    private fun TaskEntry.toTaskInfo(): TaskInfo = TaskInfo(
        name = name,
        scheduleDescription = describeSchedule(),
        enabled = enabled.get(),
        allowConcurrent = allowConcurrent,
        retryPolicy = retryPolicy,
        timeout = timeout,
        tags = tags,
        activeExecutions = stats.activeExecutions.get(),
        running = stats.activeExecutions.get() > 0,
        nextScheduledAt = stats.nextScheduledAt,
        lastStartedAt = stats.lastStartedAt,
        lastCompletedAt = stats.lastCompletedAt,
        lastDurationMs = stats.lastDurationMs,
        lastError = stats.lastError,
        runCount = stats.runCount.get(),
        successCount = stats.successCount.get(),
        failureCount = stats.failureCount.get(),
        skipCount = stats.skipCount.get(),
        rejectedCount = stats.rejectedCount.get()
    )

    private fun TaskEntry.describeSchedule(): String? {
        val base = schedule?.describe() ?: return null
        val delay = initialDelay ?: return base
        return "initial-delay ${delay.toMillisDescription()}, then $base"
    }

    // ------------------------------------------------------------------------
    // Internal state holders
    // ------------------------------------------------------------------------

    private class TaskEntry(
        val name: String,
        val schedule: Schedule?,
        val initialDelay: Duration?,
        val trigger: Trigger?,
        val allowConcurrent: Boolean,
        val retryPolicy: RetryPolicy?,
        val timeout: Duration?,
        val tags: Set<String>,
        val hooks: TaskHooks,
        val enabled: AtomicBoolean,
        val stats: TaskStats,
        val block: TaskContext.() -> Any?
    ) {
        val executing = AtomicBoolean(false)

        /** The single live scheduled fire for this entry, or null when none is armed. */
        val currentFire = AtomicReference<QueueItem.Fire?>(null)

        val rearmAfterCompletion: Boolean get() = schedule is Schedule.FixedDelay

        fun beginExecution(): Boolean {
            if (!allowConcurrent && !executing.compareAndSet(false, true)) return false
            stats.activeExecutions.incrementAndGet()
            return true
        }

        fun endExecution() {
            stats.activeExecutions.decrementAndGet()
            if (!allowConcurrent) executing.set(false)
        }
    }

    private class TaskStats {
        val activeExecutions = AtomicLong(0)
        val runCount = AtomicLong(0)
        val successCount = AtomicLong(0)
        val failureCount = AtomicLong(0)
        val skipCount = AtomicLong(0)
        val rejectedCount = AtomicLong(0)

        @Volatile
        var nextScheduledAt: Long? = null

        @Volatile
        var lastStartedAt: Long? = null

        @Volatile
        var lastCompletedAt: Long? = null

        @Volatile
        var lastDurationMs: Long? = null

        @Volatile
        var lastError: Throwable? = null

        fun markStarted(at: Long) {
            runCount.incrementAndGet()
            lastStartedAt = at
        }

        fun markCompleted(event: TaskCompleteEvent) {
            lastCompletedAt = event.endTime
            lastDurationMs = max(0, event.duration)
            if (event.isSuccess) {
                successCount.incrementAndGet()
                lastError = null
            } else {
                failureCount.incrementAndGet()
                lastError = event.error
            }
        }
    }

    /**
     * One execution spanning all of its retry attempts. Fields are volatile because
     * consecutive attempts may run on different worker threads.
     */
    private class PendingExecution(
        val entry: TaskEntry,
        val scheduledTime: Long?,
        val type: TaskExecutionType,
        val context: TaskContextImpl,
        val future: CompletableFuture<TaskRunResult>
    ) {
        @Volatile
        var started = false

        @Volatile
        var startedAt = 0L

        @Volatile
        var failedAttempts = 0

        @Volatile
        var lastError: Throwable? = null

        val finished = AtomicBoolean(false)
    }

    /** Timeout token for a single attempt; whoever wins [done] owns the outcome. */
    private class Attempt(val thread: Thread) {
        val done = AtomicBoolean(false)

        @Volatile
        var timedOut = false
    }

    private sealed class QueueItem(val time: Long) : Delayed {
        class Fire(val entry: TaskEntry, time: Long) : QueueItem(time)
        class Retry(val pending: PendingExecution, time: Long) : QueueItem(time)
        class Timeout(val attempt: Attempt, time: Long) : QueueItem(time)

        override fun getDelay(unit: TimeUnit): Long =
            unit.convert(time - System.currentTimeMillis(), TimeUnit.MILLISECONDS)

        override fun compareTo(other: Delayed): Int =
            time.compareTo((other as QueueItem).time)
    }
}
