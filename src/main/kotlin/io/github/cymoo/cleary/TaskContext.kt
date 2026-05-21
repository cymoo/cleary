package io.github.cymoo.cleary

/** Per-execution context passed to a task body. */
interface TaskContext {
    /** Name of the task currently executing. */
    val taskName: String

    /** Returns a context value or throws when absent or of another type. */
    operator fun <T : Any> get(key: String): T

    /** Returns a context value or null when absent or of another type. */
    fun <T> getOrNull(key: String): T?

    /** Returns a context value or [default] when absent or of another type. */
    fun <T> getOrDefault(key: String, default: T): T

    /** Writes a value visible only to this execution. */
    operator fun set(key: String, value: Any)

    /** Removes a value from this execution context. */
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
