import io.github.cymoo.colleen.Colleen
import io.github.cymoo.colleen.middleware.Cors
import io.github.cymoo.colleen.middleware.RequestLogger
import io.github.cymoo.colleen.middleware.ServeStatic

fun main() {
    val app = Colleen()
    val dashboard = TaskDashboard(
        options = TaskDashboardOptions.fromEnvironment(
            allowReset = true,
            concurrency = 6,
            threadNamePrefix = "cleary-dashboard"
        ),
        registerTasks = ::registerDemoTasks
    )
    val port = dashboardPort()

    app.config.json {
        pretty = true
    }

    app.provide(dashboard)
    app.use(RequestLogger())
    app.use(Cors())
    app.use(
        ServeStatic(
            root = "classpath:public",
            baseUrl = "/",
            index = "index.html",
            maxAge = 0,
            fallthrough = true
        )
    )
    app.onShutdown {
        dashboard.shutdown()
    }

    dashboard.mount(app)

    app.listen(port)
    println("Cleary task dashboard running at http://localhost:$port")
}

private fun dashboardPort(): Int {
    val raw = System.getProperty("port") ?: System.getenv("PORT") ?: "8000"
    return raw.toIntOrNull()?.takeIf { it in 1..65535 }
        ?: error("Invalid dashboard port: '$raw'")
}
