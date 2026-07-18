package io.github.cymoo.cleary

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Per-execution context passed to a task body and lifecycle hooks.
 *
 * Values written here are visible only to the current execution (including its
 * retries); the scheduler's global context is layered underneath as read-only
 * defaults. Typed reads are provided by the reified extensions [getAs],
 * [getOrDefault], and [require].
 */
interface TaskContext {
    /** Name of the task currently executing. */
    val taskName: String

    /**
     * True when scheduler shutdown has been requested or the current worker thread
     * has been interrupted (e.g. by a task timeout). Long-running tasks should poll
     * this and return early to cooperate with shutdown and timeouts.
     */
    val isCancelled: Boolean

    /** Returns the raw value for [key], or null when absent. */
    operator fun get(key: String): Any?

    /** Writes a value visible only to this execution. */
    operator fun set(key: String, value: Any)

    /** Removes a value from this execution's view (including inherited global values). */
    fun remove(key: String)

    /** Snapshot of all currently visible values. */
    fun toMap(): Map<String, Any>
}

/** Returns the value for [key] cast to [T], or null when absent or of another type. */
inline fun <reified T : Any> TaskContext.getAs(key: String): T? = get(key) as? T

/** Returns the value for [key] cast to [T], or [default] when absent or of another type. */
inline fun <reified T : Any> TaskContext.getOrDefault(key: String, default: T): T =
    getAs<T>(key) ?: default

/** Returns the value for [key] cast to [T]; throws when absent or of another type. */
inline fun <reified T : Any> TaskContext.require(key: String): T {
    val value = get(key)
        ?: throw NoSuchElementException("Context key '$key' not found for task '$taskName'")
    return value as? T ?: throw ClassCastException(
        "Context key '$key' for task '$taskName' holds ${value::class.qualifiedName}, " +
            "not ${T::class.qualifiedName}"
    )
}

/**
 * Copy-on-write view over the global context: reads fall through to [global]
 * until the first write, which materializes a private copy for this execution.
 */
internal class TaskContextImpl(
    override val taskName: String,
    private val global: Map<String, Any>,
    private val cancelled: () -> Boolean
) : TaskContext {
    private val local = AtomicReference<ConcurrentHashMap<String, Any>?>(null)

    override val isCancelled: Boolean
        get() = cancelled() || Thread.currentThread().isInterrupted

    override fun get(key: String): Any? = (local.get() ?: global)[key]

    override fun set(key: String, value: Any) {
        materialize()[key] = value
    }

    override fun remove(key: String) {
        materialize().remove(key)
    }

    override fun toMap(): Map<String, Any> = HashMap(local.get() ?: global)

    internal fun seed(values: Map<String, Any>) {
        if (values.isNotEmpty()) materialize().putAll(values)
    }

    private fun materialize(): ConcurrentHashMap<String, Any> {
        local.get()?.let { return it }
        val created = ConcurrentHashMap(global)
        return if (local.compareAndSet(null, created)) created else local.get()!!
    }
}
