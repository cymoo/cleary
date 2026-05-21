# Cleary Task Dashboard Example

This example runs a production-shaped task management dashboard for Cleary using the
[Colleen](https://github.com/cymoo/colleen) web framework.

The reusable dashboard is decoupled from the concrete demo tasks. The example wires
three Kotlin files:

| File | Purpose |
|---|---|
| `main.kt` | Colleen app wiring, static assets, API mount, port config |
| `tasks.kt` | Built-in demo task registration |
| `TaskDashboard.kt` | Reusable dashboard service, metrics, history, and API handlers |

The homepage shows compact task state under group tabs, plus health, next run,
last signal, success rate, average duration, and bounded history capacity. Click
a task name for a detail page with the full description, counters, retry policy,
timestamps, and task-specific events.

## Requirements

- JDK 21 or later for Colleen.
- Maven.
- The Cleary root project installed into your local Maven repository.

## Run

From the repository root:

```bash
mvn -DskipTests install
mvn -f examples/task-dashboard/pom.xml compile exec:java
```

If port `8000` is already in use:

```bash
mvn -f examples/task-dashboard/pom.xml compile exec:java -Dport=8081
```

Configure retained execution history with either a system property or environment
variable. The default is `500` records.

```bash
mvn -f examples/task-dashboard/pom.xml compile exec:java -DtaskDashboard.historyLimit=1000
TASK_DASHBOARD_HISTORY_LIMIT=1000 mvn -f examples/task-dashboard/pom.xml compile exec:java
```

Then open:

```text
http://localhost:8000
```

Use the matching port if you started it with `-Dport=...`.

The example project depends on `io.github.cymoo:cleary:0.1.0`. Running `mvn install`
from the root first ensures the dashboard consumes your local Cleary code rather than
an older artifact from Maven Central.

## Demo Tasks

| Task | Group | Schedule | Purpose |
|---|---|---|---|
| `heartbeat` | core | every 2 seconds | Frequent liveness signal |
| `metrics-rollup` | core | every 7 seconds | Fixed-rate analytics rollup |
| `flaky-sync` | operations | every 11 seconds with retry | Retry/backoff visualization |
| `nightly-cleanup` | operations | Quartz cron | Cron metadata and scheduled maintenance |
| `one-shot-report` | default | once after startup/reset | One-shot scheduling |
| `manual-cache-flush` | default | manual only | Operator-triggered task |

## API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/api/overview` | Scheduler health and aggregate counters |
| `GET` | `/api/tasks` | Current task registry and per-task stats |
| `GET` | `/api/tasks/{name}` | Single task detail payload |
| `GET` | `/api/events?after=<seq>&limit=<n>` | Incremental event feed |
| `GET` | `/api/history?limit=<n>&task=<name>` | Bounded execution history, optionally task-filtered |
| `POST` | `/api/tasks/{name}/run` | Queue a manual execution |
| `POST` | `/api/tasks/{name}/enable` | Enable scheduled execution |
| `POST` | `/api/tasks/{name}/disable` | Disable scheduled execution |
| `DELETE` | `/api/tasks/{name}` | Remove a task until reset |
| `POST` | `/api/admin/reset` | Restore built-in demo tasks and clear state |

## Smoke Test

```bash
curl http://localhost:8000/api/overview
curl http://localhost:8000/api/tasks
curl http://localhost:8000/api/tasks/heartbeat
curl 'http://localhost:8000/api/history?limit=25'
curl 'http://localhost:8000/api/history?limit=25&task=heartbeat'
curl -X POST http://localhost:8000/api/tasks/heartbeat/run
curl -X POST http://localhost:8000/api/tasks/heartbeat/disable
curl -X POST http://localhost:8000/api/tasks/heartbeat/enable
curl -X DELETE http://localhost:8000/api/tasks/heartbeat
curl -X POST http://localhost:8000/api/admin/reset
```

## Notes

- State is intentionally in-memory. Restarting the process or pressing reset clears
  counters and restores the built-in task set.
- Reset is enabled for this example. When embedding `TaskDashboard` in production,
  leave `allowReset = false` unless rebuilding the scheduler is a deliberate operator action.
- Recent events are kept in a bounded ring buffer to avoid unbounded memory growth.
- The frontend uses vanilla HTML, CSS, and JavaScript served from the application
  classpath; there is no Node or frontend build step.
