const state = {
  tasks: [],
  currentTask: null,
  activeGroup: null,
  toastTimer: null,
  pending: new Set(),
};

const page = document.body.dataset.page;
const toast = document.querySelector("#toast");

async function request(path, options = {}) {
  const response = await fetch(path, {
    headers: { Accept: "application/json", ...(options.headers || {}) },
    ...options,
  });
  const text = await response.text();
  const data = text ? JSON.parse(text) : null;
  if (!response.ok) {
    throw new Error(data?.message || data?.error || `${response.status} ${response.statusText}`);
  }
  return data;
}

function showToast(message, isError = false) {
  toast.textContent = message;
  toast.classList.toggle("is-error", isError);
  toast.classList.add("is-visible");
  clearTimeout(state.toastTimer);
  state.toastTimer = setTimeout(() => toast.classList.remove("is-visible"), 2600);
}

function pct(value) {
  return `${Math.round((value || 0) * 100)}%`;
}

function duration(value) {
  if (value == null) return "-";
  if (value < 1000) return `${value} ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)} s`;
  return `${Math.round(value / 60_000)} min`;
}

function clock(value) {
  if (value == null) return "-";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function dateTime(value) {
  if (value == null) return "-";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function distance(value, empty = "never") {
  if (value == null) return empty;
  const delta = value - Date.now();
  const abs = Math.abs(delta);
  const suffix = delta >= 0 ? "from now" : "ago";
  if (abs < 1000) return delta >= 0 ? "due now" : "just now";
  if (abs < 60_000) return `${Math.round(abs / 1000)}s ${suffix}`;
  if (abs < 3_600_000) return `${Math.round(abs / 60_000)}m ${suffix}`;
  return `${Math.round(abs / 3_600_000)}h ${suffix}`;
}

function setText(selector, value) {
  const node = document.querySelector(selector);
  if (node) node.textContent = value;
}

function taskUrl(name) {
  return `/task.html?name=${encodeURIComponent(name)}`;
}

function taskApi(name) {
  return `/api/tasks/${encodeURIComponent(name)}`;
}

async function loadDashboard() {
  const [overview, tasks] = await Promise.all([
    request("/api/overview"),
    request("/api/tasks"),
  ]);
  state.tasks = tasks;
  renderOverview(overview);
  syncActiveGroup(tasks);
  renderTaskTabs(tasks);
  renderTaskRows(tasks);
}

function renderOverview(overview) {
  setText("#health", overview.health);
  setText("#scheduler-state", overview.schedulerRunning ? `online for ${duration(overview.uptimeMs)}` : "offline");
  setText("#task-count", overview.taskCount);
  setText("#task-enabled", `${overview.enabledCount} enabled / ${overview.disabledCount} disabled`);
  setText("#running-count", overview.runningCount);
  setText("#removed-count", `${overview.removedCount} removed`);
  setText("#success-rate", pct(overview.successRate));
  setText("#avg-duration", `${duration(overview.averageDurationMs)} avg`);
  setText("#history-retained", overview.historyRetained);
  setText("#history-capacity", `${overview.historyCapacity} capacity`);
  setText("#last-updated", `updated ${clock(overview.generatedAt)}`);
  const dot = document.querySelector("#health-dot");
  if (dot) dot.dataset.health = overview.health;
}

function renderTaskRows(tasks) {
  const tbody = document.querySelector("#task-rows");
  const empty = document.querySelector("#empty-tasks");
  tbody.replaceChildren();
  const groups = groupTasks(tasks);
  const active = state.activeGroup || groups[0]?.[0] || "default";
  const visibleTasks = groups.find(([group]) => group === active)?.[1] || [];
  empty.hidden = visibleTasks.length !== 0;

  for (const task of visibleTasks) {
    const row = document.createElement("tr");
    row.dataset.status = task.status;

    row.append(
      cell(taskIdentityCompact(task), "task-name-cell"),
      cell(statusPill(task.status), "status-cell"),
      cell(scheduleBrief(task)),
      cell(nextSignal(task), "nowrap"),
      cell(lastSignalCompact(task), "last-cell"),
      cell(homeActions(task), "actions-cell"),
    );

    tbody.append(row);
  }
}

function syncActiveGroup(tasks) {
  const groups = groupTasks(tasks).map(([group]) => group);
  if (!groups.length) {
    state.activeGroup = null;
    return;
  }
  if (!state.activeGroup || !groups.includes(state.activeGroup)) {
    state.activeGroup = groups[0];
  }
}

function renderTaskTabs(tasks) {
  const target = document.querySelector("#task-tabs");
  if (!target) return;
  target.replaceChildren();

  for (const [group, groupedTasks] of groupTasks(tasks)) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "tab";
    button.role = "tab";
    button.dataset.group = group;
    button.setAttribute("aria-selected", String(group === state.activeGroup));
    button.innerHTML = `<span>${group}</span><strong>${groupedTasks.length}</strong>`;
    button.addEventListener("click", () => {
      state.activeGroup = group;
      renderTaskTabs(state.tasks);
      renderTaskRows(state.tasks);
    });
    target.append(button);
  }
}

function groupTasks(tasks) {
  const groups = new Map();
  for (const task of tasks) {
    const group = normalizeGroup(task.group);
    if (!groups.has(group)) groups.set(group, []);
    groups.get(group).push(task);
  }
  return [...groups.entries()].sort(([a], [b]) => {
    if (a === "default") return 1;
    if (b === "default") return -1;
    return a.localeCompare(b);
  });
}

function normalizeGroup(group) {
  const value = (group || "").trim();
  return value || "default";
}

function taskIdentityCompact(task) {
  const wrap = document.createElement("div");
  wrap.className = "task-identity task-identity--compact";
  const title = document.createElement("a");
  title.className = "task-link";
  title.href = taskUrl(task.name);
  title.textContent = task.name;
  const meta = document.createElement("span");
  meta.textContent = `${task.allowConcurrent ? "concurrent" : "single-flight"}${task.retry ? " / retry" : ""}`;
  wrap.append(title, meta);
  return wrap;
}

function scheduleBrief(task) {
  if (task.manualOnly) return "manual";
  return task.scheduleDescription || "scheduled";
}

function nextSignal(task) {
  if (task.manualOnly || !task.enabled) return "-";
  return distance(task.nextScheduledAt, "not scheduled");
}

function lastSignalCompact(task) {
  const wrap = document.createElement("div");
  wrap.className = "last-signal last-signal--compact";
  const headline = document.createElement("strong");
  headline.textContent = task.lastError || task.lastResult || "No runs";
  if (task.lastError) headline.classList.add("is-error");
  const meta = document.createElement("span");
  meta.textContent = `${distance(task.lastCompletedAt)} / ${duration(task.lastDurationMs)}`;
  wrap.append(headline, meta);
  return wrap;
}

function statusPill(status) {
  const pill = document.createElement("span");
  pill.className = "status";
  pill.dataset.status = status;
  pill.textContent = status;
  return pill;
}

function homeActions(task) {
  const wrap = document.createElement("div");
  wrap.className = "actions actions--nowrap";
  wrap.append(
    actionButton("Run", task, "run", "button--primary"),
    actionButton(task.enabled ? "Pause" : "Enable", task, task.enabled ? "disable" : "enable"),
  );
  return wrap;
}

function detailActions(task) {
  const wrap = document.createElement("div");
  wrap.className = "actions detail-actions__buttons";
  wrap.append(
    actionButton("Run now", task, "run", "button--primary"),
    actionButton(task.enabled ? "Disable" : "Enable", task, task.enabled ? "disable" : "enable"),
    actionButton("Remove", task, "remove", "button--danger"),
  );
  return wrap;
}

function actionButton(label, task, action, extraClass = "") {
  const button = document.createElement("button");
  const key = `${task.name}:${action}`;
  button.type = "button";
  button.className = `button button--small ${extraClass}`;
  button.textContent = label;
  button.disabled = state.pending.has(key);
  button.addEventListener("click", (event) => {
    event.stopPropagation();
    performTaskAction(task.name, action, key).catch((error) => showToast(error.message, true));
  });
  return button;
}

function cell(content, className = "") {
  const td = document.createElement("td");
  if (className) td.className = className;
  if (content instanceof Node) {
    td.append(content);
  } else {
    td.textContent = content;
  }
  return td;
}

async function performTaskAction(taskName, action, key) {
  if (action === "remove" && !confirm(`Remove '${taskName}' until reset?`)) return;
  state.pending.add(key);
  rerenderPending();
  try {
    const method = action === "remove" ? "DELETE" : "POST";
    const suffix = action === "remove" ? "" : `/${action}`;
    const response = await request(`${taskApi(taskName)}${suffix}`, { method });
    showToast(response.message);
    if (page === "task" && action === "remove") {
      window.location.href = "/";
      return;
    }
    await refreshCurrentPage();
  } finally {
    state.pending.delete(key);
    rerenderPending();
  }
}

function rerenderPending() {
  if (page === "dashboard") renderTaskRows(state.tasks);
  if (page === "task" && state.currentTask) renderDetailActions(state.currentTask);
}

async function resetDashboard() {
  if (!confirm("Reset tasks, counters, and bounded history?")) return;
  const button = document.querySelector("#reset-button");
  button.disabled = true;
  try {
    const response = await request("/api/admin/reset", { method: "POST" });
    showToast(response.message);
    await loadDashboard();
  } catch (error) {
    showToast(error.message, true);
  } finally {
    button.disabled = false;
  }
}

async function loadTaskDetail() {
  const name = new URLSearchParams(window.location.search).get("name");
  if (!name) {
    throw new Error("Missing task name");
  }
  const [task, feed] = await Promise.all([
    request(taskApi(name)),
    request(`/api/history?limit=80&task=${encodeURIComponent(name)}`),
  ]);
  state.currentTask = task;
  renderTaskDetail(task);
  renderTaskEvents(feed);
}

function renderTaskDetail(task) {
  document.title = `${task.name} - Cleary Task`;
  setText("#task-group", task.group);
  setText("#task-title", task.name);
  setText("#task-description", task.description || "No description provided.");
  setText("#detail-runs", task.runCount);
  setText("#detail-success", task.successCount);
  setText("#detail-success-rate", pct(task.successRate));
  setText("#detail-failures", task.failureCount);
  setText("#detail-skipped", task.skipCount);
  setText("#detail-rejected", task.rejectedCount);
  setText("#detail-retries", task.retryCount);
  setText("#detail-schedule", task.scheduleDescription || "Manual only");
  setText("#detail-next", task.manualOnly || !task.enabled ? "-" : `${dateTime(task.nextScheduledAt)} (${distance(task.nextScheduledAt)})`);
  setText("#detail-last-start", dateTime(task.lastStartedAt));
  setText("#detail-last-complete", dateTime(task.lastCompletedAt));
  setText("#detail-average", duration(task.averageDurationMs));
  setText("#detail-last-duration", duration(task.lastDurationMs));
  setText("#detail-mode", task.manualOnly ? "Manual" : "Scheduled");
  setText("#detail-concurrency", task.allowConcurrent ? "Concurrent allowed" : "Single flight");
  setText("#detail-active", task.activeExecutions);
  setText("#detail-retry", retryText(task.retry));
  setText("#detail-result", task.lastResult || "-");
  setText("#detail-error", task.lastError || "-");
  const status = document.querySelector("#detail-status");
  status.replaceChildren(statusPill(task.status));
  renderDetailActions(task);
}

function renderDetailActions(task) {
  const target = document.querySelector("#detail-actions");
  target.replaceChildren(detailActions(task));
}

function retryText(retry) {
  if (!retry) return "No retry policy";
  return `${retry.maxAttempts} attempts / ${duration(retry.initialDelayMs)} initial / x${retry.backoffMultiplier} / cap ${duration(retry.maxDelayMs)}`;
}

function renderTaskEvents(feed) {
  const tbody = document.querySelector("#task-history-rows");
  const empty = document.querySelector("#empty-task-history");
  renderEvents(tbody, empty, feed.events, false);
}

async function loadHistory() {
  const limitInput = document.querySelector("#history-limit");
  const limit = Number(limitInput.value || 500);
  const feed = await request(`/api/history?limit=${encodeURIComponent(limit)}`);
  renderHistory(feed);
}

function renderHistory(feed) {
  const tbody = document.querySelector("#history-rows");
  const empty = document.querySelector("#empty-history");
  const meta = document.querySelector("#history-meta");
  const rows = renderEvents(tbody, empty, feed.events, true);
  meta.textContent = `${rows} retained rows / capacity ${feed.capacity} / latest #${feed.latestSeq}${feed.truncated ? " / older rows truncated" : ""}`;
}

function renderEvents(tbody, empty, events, includeTask) {
  const rows = [...events].reverse();
  tbody.replaceChildren();
  empty.hidden = rows.length !== 0;

  for (const event of rows) {
    const row = document.createElement("tr");
    row.dataset.status = event.status;
    const cells = [
      cell(`#${event.seq}`, "mono"),
      cell(clock(event.timestamp), "nowrap"),
    ];
    if (includeTask) cells.push(cell(event.taskName || "dashboard"));
    cells.push(
      cell(statusPill(event.status), "status-cell"),
      cell(event.type),
      cell(event.message, "message-cell"),
      cell(event.detail || "-", "detail-cell"),
    );
    row.append(...cells);
    tbody.append(row);
  }
  return rows.length;
}

async function refreshCurrentPage() {
  if (page === "dashboard") return loadDashboard();
  if (page === "task") return loadTaskDetail();
  if (page === "history") return loadHistory();
}

function bootDashboard() {
  document.querySelector("#reset-button").addEventListener("click", resetDashboard);
  loadDashboard().catch((error) => showToast(error.message, true));
  setInterval(() => loadDashboard().catch((error) => showToast(error.message, true)), 1500);
}

function bootTaskDetail() {
  document.querySelector("#refresh-task").addEventListener("click", () => {
    loadTaskDetail().catch((error) => showToast(error.message, true));
  });
  loadTaskDetail().catch((error) => showToast(error.message, true));
  setInterval(() => loadTaskDetail().catch((error) => showToast(error.message, true)), 2000);
}

function bootHistory() {
  document.querySelector("#refresh-history").addEventListener("click", () => {
    loadHistory().catch((error) => showToast(error.message, true));
  });
  document.querySelector("#history-limit").addEventListener("change", () => {
    loadHistory().catch((error) => showToast(error.message, true));
  });
  loadHistory().catch((error) => showToast(error.message, true));
  setInterval(() => loadHistory().catch((error) => showToast(error.message, true)), 2500);
}

if (page === "history") {
  bootHistory();
} else if (page === "task") {
  bootTaskDetail();
} else {
  bootDashboard();
}
