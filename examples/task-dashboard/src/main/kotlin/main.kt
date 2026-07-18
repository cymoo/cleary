import io.github.cymoo.cleary.dashboard.Dashboard
import io.github.cymoo.cleary.taskScheduler
import io.github.cymoo.colleen.Colleen
import io.github.cymoo.colleen.middleware.RequestLogger
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Demo for Cleary's built-in web dashboard, mounted into a host Colleen
 * application: a handful of tasks exercising fixed-rate, fixed-delay, cron,
 * one-shot, retry, timeout, and manual-only scheduling.
 *
 * Run with `mvn compile exec:java`, then open http://localhost:8000/tasks/
 * (override the port with -Dport=NNNN or PORT).
 *
 * For a dashboard-only process, `Dashboard(scheduler).start(port)` serves the
 * same app standalone without a host application.
 */
fun main() {
    val scheduler = taskScheduler {
        concurrency = 6
        threadNamePrefix = "cleary-demo"
        registerShutdownHook = true
    }

    registerDemoTasks(scheduler)
    scheduler.start()

    // A regular Colleen application with its own routes …
    val app = Colleen()
    app.use(RequestLogger())
    app.get("/") { ctx ->
        ctx.html(
            """
            <!DOCTYPE html>
            <html>
            <head><title>Demo app</title></head>
            <body style="font-family: sans-serif; max-width: 640px; margin: 60px auto;">
              <h1>Host application</h1>
              <p>This is an ordinary Colleen app with the Cleary dashboard mounted as a sub-app.</p>
              <p>➡ <a href="/tasks/">Open the task dashboard</a></p>
            </body>
            </html>
            """.trimIndent()
        )
    }

    // … with the Cleary dashboard mounted as a sub-application.
    app.mount("/tasks", Dashboard(scheduler).app)

    app.onShutdown { scheduler.shutdown() }

    val port = dashboardPort()
    app.listen(port)
    println("Demo app running at http://localhost:$port — dashboard at http://localhost:$port/tasks/")
}

private fun registerDemoTasks(scheduler: io.github.cymoo.cleary.TaskScheduler) {
    val flakyAttempts = AtomicInteger(0)
    val rollupCursor = AtomicLong(40)
    val reportNumber = AtomicInteger(0)

    scheduler.task("heartbeat") {
        every(2.seconds)
        tags("core")
        run {
            Thread.sleep(45)
            mapOf("signal" to "nominal", "at" to Instant.now().toString())
        }
    }

    scheduler.task("metrics-rollup") {
        every(7.seconds)
        tags("core")
        run {
            Thread.sleep(180)
            mapOf("rolledUp" to rollupCursor.addAndGet(3), "window" to "7s")
        }
    }

    scheduler.task("queue-drain") {
        fixedDelay(6.seconds)   // measured from the previous run's completion
        tags("core")
        run {
            Thread.sleep(1_200)
            "drained"
        }
    }

    scheduler.task("flaky-sync") {
        every(11.seconds)
        tags("operations")
        retry(maxAttempts = 3, initialDelay = 450.milliseconds, backoffMultiplier = 2.0, maxDelay = 2.seconds)
        run {
            Thread.sleep(120)
            val attempt = flakyAttempts.incrementAndGet()
            if (attempt % 3 != 0) error("Remote inventory shard timed out on attempt marker $attempt")
            mapOf("remote" to "inventory", "checkpoint" to attempt)
        }
    }

    scheduler.task("slow-external-call") {
        every(20.seconds)
        timeout(2.seconds)
        tags("operations")
        run {
            // takes 1–3 s against a 2 s timeout: times out roughly half the time
            Thread.sleep(ThreadLocalRandom.current().nextLong(1_000, 3_000))
            "responded"
        }
    }

    scheduler.task("nightly-cleanup") {
        cron("0 0/5 * * * ?")
        tags("operations")
        run {
            Thread.sleep(90)
            mapOf("deletedRows" to 12, "partition" to "demo")
        }
    }

    scheduler.task("one-shot-report") {
        once(Instant.now().plusSeconds(15))
        run {
            Thread.sleep(150)
            mapOf("report" to "ops-snapshot-${reportNumber.incrementAndGet()}", "format" to "html")
        }
    }

    scheduler.task("manual-cache-flush") {
        run {
            Thread.sleep(110)
            mapOf("cache" to "edge-metadata", "status" to "flushed")
        }
    }
}

private fun dashboardPort(): Int {
    val raw = System.getProperty("port") ?: System.getenv("PORT") ?: "8000"
    return raw.toIntOrNull()?.takeIf { it in 1..65535 }
        ?: error("Invalid dashboard port: '$raw'")
}
