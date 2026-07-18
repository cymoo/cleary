package io.github.cymoo.cleary.dashboard

/**
 * Minimal JSON writer used by the dashboard's API responses, keeping the core
 * free of serialization dependencies. Values are written eagerly into a single
 * [StringBuilder]; only the shapes the dashboard emits are supported.
 */
internal class JsonWriter(private val sb: StringBuilder = StringBuilder()) {

    fun obj(build: JsonObjectScope.() -> Unit): String {
        JsonObjectScope(sb).writeObject(build)
        return sb.toString()
    }

    companion object {
        fun escape(value: String): String {
            val sb = StringBuilder(value.length + 8)
            for (ch in value) {
                when {
                    ch == '"' -> sb.append("\\\"")
                    ch == '\\' -> sb.append("\\\\")
                    ch == '\n' -> sb.append("\\n")
                    ch == '\r' -> sb.append("\\r")
                    ch == '\t' -> sb.append("\\t")
                    ch < ' ' -> sb.append("\\u%04x".format(ch.code))
                    else -> sb.append(ch)
                }
            }
            return sb.toString()
        }
    }
}

internal class JsonObjectScope(private val sb: StringBuilder) {
    private var first = true

    fun writeObject(build: JsonObjectScope.() -> Unit) {
        sb.append('{')
        build()
        sb.append('}')
    }

    private fun key(name: String) {
        if (!first) sb.append(',')
        first = false
        sb.append('"').append(JsonWriter.escape(name)).append("\":")
    }

    fun put(name: String, value: String?) {
        key(name)
        if (value == null) sb.append("null")
        else sb.append('"').append(JsonWriter.escape(value)).append('"')
    }

    fun put(name: String, value: Long?) {
        key(name)
        sb.append(value?.toString() ?: "null")
    }

    fun put(name: String, value: Int) {
        key(name)
        sb.append(value)
    }

    fun put(name: String, value: Boolean) {
        key(name)
        sb.append(value)
    }

    fun obj(name: String, build: JsonObjectScope.() -> Unit) {
        key(name)
        JsonObjectScope(sb).writeObject(build)
    }

    fun arrayOfObjects(name: String, count: Int, build: JsonObjectScope.(Int) -> Unit) {
        key(name)
        sb.append('[')
        for (i in 0 until count) {
            if (i > 0) sb.append(',')
            JsonObjectScope(sb).writeObject { build(i) }
        }
        sb.append(']')
    }

    fun arrayOfLongs(name: String, values: List<Long>) {
        key(name)
        sb.append('[')
        values.forEachIndexed { i, v ->
            if (i > 0) sb.append(',')
            sb.append(v)
        }
        sb.append(']')
    }

    fun arrayOfStrings(name: String, values: Collection<String>) {
        key(name)
        sb.append('[')
        values.forEachIndexed { i, v ->
            if (i > 0) sb.append(',')
            sb.append('"').append(JsonWriter.escape(v)).append('"')
        }
        sb.append(']')
    }
}

/** Extracts the string field [field] from a tiny JSON body like `{"expr": "..."}`. */
internal fun extractJsonStringField(body: String, field: String): String? {
    val match = Regex("\"${Regex.escape(field)}\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"").find(body) ?: return null
    val raw = match.groupValues[1]
    val sb = StringBuilder(raw.length)
    var i = 0
    while (i < raw.length) {
        val ch = raw[i]
        if (ch == '\\' && i + 1 < raw.length) {
            when (val next = raw[i + 1]) {
                'n' -> sb.append('\n')
                't' -> sb.append('\t')
                'r' -> sb.append('\r')
                'u' -> {
                    if (i + 5 < raw.length) {
                        sb.append(raw.substring(i + 2, i + 6).toInt(16).toChar())
                        i += 4
                    }
                }
                else -> sb.append(next)
            }
            i += 2
        } else {
            sb.append(ch)
            i++
        }
    }
    return sb.toString()
}
