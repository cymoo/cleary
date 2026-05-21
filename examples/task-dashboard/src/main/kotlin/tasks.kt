import io.github.cymoo.cleary.TaskScheduler
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

private class DemoRuntime {
    val flakyAttempts = AtomicInteger(0)
    val rollupCursor = AtomicLong(40)
    val reportNumber = AtomicInteger(0)
}

fun registerDemoTasks(scheduler: TaskScheduler): List<DashboardTaskDescriptor> {
    val runtime = DemoRuntime()

    scheduler.task("heartbeat") {
        every(Duration.ofSeconds(2))
        run {
            Thread.sleep(45)
            mapOf("signal" to "nominal", "task" to taskName, "at" to Instant.now().toString())
        }
    }

    scheduler.task("metrics-rollup") {
        every(Duration.ofSeconds(7))
        concurrent(false)
        run {
            Thread.sleep(180)
            val total = runtime.rollupCursor.addAndGet(3)
            mapOf("rolledUp" to total, "window" to "7s")
        }
    }

    scheduler.task("flaky-sync") {
        every(Duration.ofSeconds(11))
        retry(
            maxAttempts = 3,
            initialDelay = Duration.ofMillis(450),
            backoffMultiplier = 2.0,
            maxDelay = Duration.ofSeconds(2)
        )
        run {
            Thread.sleep(120)
            val attempt = runtime.flakyAttempts.incrementAndGet()
            if (attempt % 3 != 0) {
                error("Remote inventory shard timed out on attempt marker $attempt")
            }
            mapOf("remote" to "inventory", "checkpoint" to attempt)
        }
    }

    scheduler.task("nightly-cleanup") {
        cron("0 0/5 * * * ?", ZoneId.systemDefault())
        run {
            Thread.sleep(90)
            mapOf("deletedRows" to 12, "partition" to "demo")
        }
    }

    scheduler.task("one-shot-report") {
        once(Instant.now().plusSeconds(12))
        run {
            Thread.sleep(150)
            val number = runtime.reportNumber.incrementAndGet()
            mapOf("report" to "ops-snapshot-$number", "format" to "html")
        }
    }

    scheduler.task("manual-cache-flush") {
        run {
            Thread.sleep(110)
            mapOf("cache" to "edge-metadata", "status" to "flushed")
        }
    }

    return listOf(
        DashboardTaskDescriptor(
            name = "heartbeat",
            description = "Publishes a fast liveness pulse so the dashboard always has fresh activity.",
            group = "control-plane"
        ),
        DashboardTaskDescriptor(
            name = "metrics-rollup",
            description = "Aggregates a pretend shard counter and demonstrates a longer fixed-rate cadence.",
            group = "analytics"
        ),
        DashboardTaskDescriptor(
            name = "flaky-sync",
            description = "Fails twice and succeeds on the third attempt, making retry backoff visible.",
            group = "integrations"
        ),
        DashboardTaskDescriptor(
            name = "nightly-cleanup",
            description = "Cron-driven cleanup lane; useful for inspecting Quartz schedule metadata.",
            group = "maintenance"
        ),
        DashboardTaskDescriptor(
            name = "one-shot-report",
            description = "Runs once shortly after startup or reset to demonstrate one-shot scheduling.",
            group = "reports"
        ),
        DashboardTaskDescriptor(
            name = "manual-cache-flush",
            description = "Manual-only task; it never runs on a schedule and is triggered from the dashboard.",
            group = "operator"
        )
    )
}
