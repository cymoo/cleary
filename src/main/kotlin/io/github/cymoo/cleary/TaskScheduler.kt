package io.github.cymoo.cleary

import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max

/**
 * A lightweight task scheduler supporting cron expressions, fixed-rate scheduling,
 * one-shot execution, retries, manual runs, and runtime task management.
 *
 * A [TaskScheduler] instance is single-use: after [shutdown] it cannot be started
 * again and no new tasks can be registered.
 */
class TaskScheduler private constructor(
    private val config: TaskSchedulerConfig
) {
    companion object {
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

    private val taskQueue = DelayQueue<ScheduledTask>()
    private val lifecycleState = AtomicReference(SchedulerState.NEW)
    private val schedulerThread = Thread(::runScheduler, "${config.threadNamePrefix}-scheduler")
        .apply { isDaemon = true }
    private val tasks = java.util.concurrent.ConcurrentHashMap<String, TaskEntry>()
    private val globalContext = java.util.concurrent.ConcurrentHashMap<String, Any>().apply {
        putAll(config.context)
    }
    private val shutdownLatch = CountDownLatch(1)

    /** Current lifecycle state of this single-use scheduler. */
    val state: SchedulerState get() = lifecycleState.get()

    /** True after [start] succeeds and before [shutdown] begins. */
    val isRunning: Boolean get() = state == SchedulerState.RUNNING

    /** True after shutdown has completed and the worker pool has terminated. */
    val isTerminated: Boolean get() = state == SchedulerState.SHUTDOWN && executor.isTerminated

    /** Registers a task. Omitting a schedule creates a manual-only task. */
    fun task(name: String, block: TaskBuilder.() -> Unit) {
        ensureAcceptsConfiguration()
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

        val entry = TaskEntry(
            name = name,
            schedule = schedule,
            trigger = schedule?.toTrigger(),
            allowConcurrent = allowConcurrent,
            retryPolicy = retryPolicy,
            enabled = AtomicBoolean(true),
            executing = AtomicBoolean(false),
            stats = TaskStats(),
            block = block
        )
        require(tasks.putIfAbsent(name, entry) == null) { "Task '$name' is already registered" }

        if (state == SchedulerState.RUNNING && entry.trigger != null && entry.enabled.get()) {
            enqueue(name, entry.trigger, entry)
        }
    }

    /** Starts the scheduler and enqueues all enabled scheduled tasks. */
    fun start() {
        when {
            lifecycleState.compareAndSet(SchedulerState.NEW, SchedulerState.RUNNING) -> {
                schedulerThread.start()
                tasks.values
                    .filter { it.trigger != null && it.enabled.get() }
                    .forEach { enqueue(it.name, it.trigger!!, it) }
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
                schedulerThread.join(5_000)
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            }
        }

        if (awaitTermination) {
            executor.shutdown()
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

    /** Blocks until [shutdown] is called or the waiting thread is interrupted. */
    fun await() {
        try {
            shutdownLatch.await()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }

    /** Submits a registered task for immediate manual execution. */
    fun run(name: String, contextValues: Map<String, Any> = emptyMap()): Future<TaskRunResult> {
        check(state == SchedulerState.RUNNING) { "TaskScheduler is not running" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        return submit(entry, scheduledTime = null, contextValues = contextValues, type = TaskExecutionType.MANUAL)
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

    /** Enables a task and schedules its next run if the scheduler is active. */
    fun enable(name: String) {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        if (entry.enabled.compareAndSet(false, true) && state == SchedulerState.RUNNING && entry.trigger != null) {
            enqueue(name, entry.trigger, entry)
        }
    }

    /** Disables a task and removes its pending scheduled triggers. */
    fun disable(name: String) {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
        val entry = tasks[name] ?: throw NoSuchElementException("Task '$name' not found")
        entry.enabled.set(false)
        taskQueue.removeIf { it.taskName == name }
        entry.stats.nextScheduledAt = null
    }

    /** Removes a task from the registry and clears pending scheduled triggers. */
    fun remove(name: String) {
        check(state != SchedulerState.SHUTDOWN) { "TaskScheduler has been shut down" }
        tasks.remove(name)
        taskQueue.removeIf { it.taskName == name }
    }

    /** Returns whether a task is currently registered. */
    fun exists(name: String): Boolean = tasks.containsKey(name)

    /** Lists registered task names in unspecified order. */
    fun listTaskNames(): List<String> = tasks.keys.toList()

    /** Returns a runtime snapshot for a task, or null when it is not registered. */
    fun getTaskInfo(name: String): TaskInfo? {
        val entry = tasks[name] ?: return null
        return entry.toTaskInfo()
    }

    private fun enqueue(taskName: String, trigger: Trigger, entry: TaskEntry): Long? {
        val nextTime = trigger.nextExecutionTime(System.currentTimeMillis() - 1)
        entry.stats.nextScheduledAt = nextTime
        if (nextTime != null) taskQueue.offer(ScheduledTask(taskName, nextTime))
        return nextTime
    }

    private fun runScheduler() {
        while (state == SchedulerState.RUNNING) {
            try {
                val scheduled = taskQueue.take()
                dispatch(scheduled)
            } catch (_: InterruptedException) {
                break
            } catch (t: Throwable) {
                reportSchedulerError(null, SchedulerErrorPhase.SCHEDULER_LOOP, t)
            }
        }
    }

    private fun dispatch(scheduled: ScheduledTask) {
        val entry = tasks[scheduled.taskName] ?: return
        if (!entry.enabled.get()) return

        val nextTime = entry.trigger?.nextExecutionTime(scheduled.scheduledTime)
        entry.stats.nextScheduledAt = nextTime
        if (nextTime != null) {
            taskQueue.offer(ScheduledTask(scheduled.taskName, nextTime))
        }

        submit(entry, scheduled.scheduledTime, emptyMap(), TaskExecutionType.SCHEDULED)
    }

    private fun submit(
        entry: TaskEntry,
        scheduledTime: Long?,
        contextValues: Map<String, Any>,
        type: TaskExecutionType
    ): Future<TaskRunResult> =
        try {
            executor.submit(Callable { executeOrSkip(entry, scheduledTime, contextValues, type) })
        } catch (_: RejectedExecutionException) {
            val event = TaskRejectedEvent(
                taskName = entry.name,
                scheduledTime = scheduledTime,
                rejectedAt = System.currentTimeMillis(),
                executionType = type,
                reason = TaskRejectedReason.WORKER_QUEUE_FULL
            )
            entry.stats.rejectedCount.incrementAndGet()
            safeHook(entry.name, SchedulerErrorPhase.ON_TASK_REJECTED) { config.onTaskRejected?.invoke(event) }
            CompletableFuture.completedFuture(TaskRunResult.Rejected(entry.name, event.reason))
        }

    private fun executeOrSkip(
        entry: TaskEntry,
        scheduledTime: Long?,
        contextValues: Map<String, Any>,
        type: TaskExecutionType
    ): TaskRunResult {
        if (!entry.beginExecution()) {
            val event = TaskSkippedEvent(
                taskName = entry.name,
                scheduledTime = scheduledTime,
                skippedAt = System.currentTimeMillis(),
                executionType = type,
                reason = TaskSkipReason.ALREADY_RUNNING
            )
            entry.stats.skipCount.incrementAndGet()
            safeHook(entry.name, SchedulerErrorPhase.ON_TASK_SKIPPED) { config.onTaskSkipped?.invoke(event) }
            return TaskRunResult.Skipped(entry.name, event.reason)
        }

        return try {
            executeTask(entry, contextValues, scheduledTime)
        } finally {
            entry.endExecution()
        }
    }

    private fun executeTask(
        entry: TaskEntry,
        extra: Map<String, Any>,
        scheduledTime: Long?
    ): TaskRunResult {
        val ctx = java.util.concurrent.ConcurrentHashMap(globalContext).apply { putAll(extra) }
        val actualStartTime = System.currentTimeMillis()
        val startEvent = TaskStartEvent(
            taskName = entry.name,
            scheduledTime = scheduledTime ?: actualStartTime,
            actualTime = actualStartTime,
            context = ctx
        )
        entry.stats.markStarted(actualStartTime)
        safeHook(entry.name, SchedulerErrorPhase.ON_TASK_START) { config.onTaskStart?.invoke(startEvent) }

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
                    val retryEvent = TaskRetryEvent(
                        taskName = entry.name,
                        failedAttempts = failedAttempts + 1,
                        maxAttempts = maxAttempts,
                        error = t,
                        nextRetryDelayMs = delayMs
                    )
                    safeHook(entry.name, SchedulerErrorPhase.ON_RETRY) { config.onRetry?.invoke(retryEvent) }
                    try {
                        Thread.sleep(delayMs)
                    } catch (ie: InterruptedException) {
                        Thread.currentThread().interrupt()
                        lastError = ie
                        break
                    }
                }
            }
        }

        val endTime = System.currentTimeMillis()
        val completeEvent = TaskCompleteEvent(entry.name, actualStartTime, endTime, result, lastError)
        entry.stats.markCompleted(completeEvent)
        safeHook(entry.name, SchedulerErrorPhase.ON_TASK_COMPLETE) { config.onTaskComplete?.invoke(completeEvent) }

        return if (lastError == null) TaskRunResult.Success(result) else TaskRunResult.Failure(lastError!!)
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

    private fun reportSchedulerError(taskName: String?, phase: SchedulerErrorPhase, error: Throwable) {
        val event = SchedulerErrorEvent(taskName, phase, error)
        try {
            config.onSchedulerError?.invoke(event)
        } catch (secondary: Throwable) {
            secondary.addSuppressed(error)
            secondary.printStackTrace()
        }
    }

    private fun TaskEntry.toTaskInfo(): TaskInfo {
        val stats = stats
        return TaskInfo(
            name = name,
            scheduleDescription = schedule?.describe(),
            enabled = enabled.get(),
            allowConcurrent = allowConcurrent,
            retryPolicy = retryPolicy,
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
    }

    private data class TaskEntry(
        val name: String,
        val schedule: Schedule?,
        val trigger: Trigger?,
        val allowConcurrent: Boolean,
        val retryPolicy: RetryPolicy?,
        val enabled: AtomicBoolean,
        val executing: AtomicBoolean,
        val stats: TaskStats,
        val block: TaskContext.() -> Any?
    ) {
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
            lastError = null
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

    private class ScheduledTask(
        val taskName: String,
        val scheduledTime: Long
    ) : Delayed {
        override fun getDelay(unit: TimeUnit): Long =
            unit.convert(scheduledTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS)

        override fun compareTo(other: Delayed): Int =
            scheduledTime.compareTo((other as ScheduledTask).scheduledTime)
    }
}
