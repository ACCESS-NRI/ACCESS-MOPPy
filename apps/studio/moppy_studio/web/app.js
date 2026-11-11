"use strict";
const $ = (id) => document.getElementById(id);
const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
let config = {},
  plan = null,
  selected = null,
  yamlMode = false,
  busy = false,
  poll = false;
let token = sessionStorage.getItem("moppy-token") || "";
let logKind = "monitor_out",
  logVariable = null;
function notify(message, error = false) {
  const target =
    error && $("review-dialog").open ? $("review-error") : $("notice");
  target.hidden = !message;
  target.textContent = message;
  target.className = error ? "error" : "";
}
async function api(path, body) {
  const xsrf = document.cookie.split("; ").find((v) => v.startsWith("_xsrf="));
  const headers = {
    "Content-Type": "application/json",
    "X-MOPPY-Token": token,
  };
  if (xsrf) headers["X-XSRFToken"] = decodeURIComponent(xsrf.slice(6));
  const response = await fetch("api/" + path, {
    method: body === undefined ? "GET" : "POST",
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (response.status === 401) $("auth-panel").hidden = false;
  const result = await response.json();
  if (!response.ok)
    throw new Error(
      typeof result.detail === "string"
        ? result.detail
        : JSON.stringify(result.detail),
    );
  return result;
}
function action(id, fn) {
  $(id).addEventListener("click", async () => {
    if (busy) return;
    busy = true;
    const button = $(id),
      previous = button.textContent;
    button.disabled = true;
    button.textContent = "Working…";
    notify("");
    try {
      await fn();
    } catch (e) {
      notify(e.message, true);
    } finally {
      busy = false;
      button.textContent = previous;
      button.disabled = id === "submit" ? !$("confirm-review").checked : false;
    }
  });
}
function syncSummary() {
  const value = (k) => document.querySelector(`[data-key="${k}"]`).value;
  const vars = $("variables")
    .value.split(/[\s,]+/)
    .filter(Boolean);
  $("variable-count").textContent = vars.length + " selected";
  $("summary-name").textContent = $("name").value || "New CMORisation batch";
  $("summary-model").textContent = value("source_id") || "—";
  $("summary-experiment").textContent = value("experiment_id") || "—";
  $("summary-vars").textContent = vars.length;
  $("summary-resources").textContent =
    (value("cpus_per_node") || "—") + " CPU · " + (value("mem") || "—");
}
async function populate(data) {
  config = structuredClone(data.config);
  // Explicit defaults match MOPPy when older batch files omit these fields.
  config.cmip_version ??= "CMIP6";
  config.model_id ??= "ACCESS-ESM1-6";
  document.querySelectorAll("[data-key]").forEach((el) => {
    const v = config[el.dataset.key];
    if (el.type === "checkbox") el.checked = v === true;
    else el.value = v ?? "";
  });
  $("variables").value = (config.variables || []).join("\n");
  $("project").value =
    String(config.scheduler_options || "").match(/^#PBS -P (\S+)\s*$/)?.[1] ||
    "";
  $("yaml").value = data.yaml || "";
  $("parent-info").value = config.parent_info
    ? (await api("config/render", { config: config.parent_info })).yaml
    : "";
  plan = null;
  syncSummary();
}
async function fromBuilder() {
  const c = structuredClone(config);
  document.querySelectorAll("[data-key]").forEach((el) => {
    const k = el.dataset.key;
    if (el.type === "checkbox") c[k] = el.checked;
    else if (el.value.trim() === "") delete c[k];
    else c[k] = el.type === "number" ? Number(el.value) : el.value.trim();
  });
  c.variables = $("variables")
    .value.split(/[\s,]+/)
    .filter(Boolean);
  const project = $("project").value.trim();
  if (project) c.scheduler_options = "#PBS -P " + project;
  else if (!config.scheduler_options) delete c.scheduler_options;
  const parent = $("parent-info").value.trim();
  if (parent)
    c.parent_info = (await api("config/parse", { yaml: parent })).config;
  else delete c.parent_info;
  // Output folder and scripts are kept together; follows a changed output path.
  if (c.script_dir === config.output_folder) c.script_dir = c.output_folder;
  return c;
}
async function currentConfig() {
  if (yamlMode)
    return (await api("config/parse", { yaml: $("yaml").value })).config;
  return await fromBuilder();
}
function setEditor(mode) {
  yamlMode = mode;
  $("builder").hidden = mode;
  $("yaml-panel").hidden = !mode;
  $("tab-builder").classList.toggle("selected", !mode);
  $("tab-yaml").classList.toggle("selected", mode);
  $("tab-builder").setAttribute("aria-selected", String(!mode));
  $("tab-yaml").setAttribute("aria-selected", String(mode));
}
action("tab-yaml", async () => {
  if (yamlMode) return;
  config = await fromBuilder();
  $("yaml").value = (await api("config/render", { config })).yaml;
  setEditor(true);
});
action("tab-builder", async () => {
  if (!yamlMode) return;
  await populate(await api("config/parse", { yaml: $("yaml").value }));
  setEditor(false);
});
action("apply-yaml", async () => {
  await populate(await api("config/parse", { yaml: $("yaml").value }));
  setEditor(false);
  notify("YAML applied. Advanced settings are preserved.");
});
action("new-batch", async () => {
  await populate(await api("template"));
  $("name").value = "";
  setEditor(false);
  syncSummary();
  notify("New configuration loaded.");
});
action("load-path", async () => {
  await populate(await api("config/load", { path: $("config-path").value }));
  setEditor(false);
  notify("Batch config loaded. Review its paths, resources and worker setup.");
});
$("upload").addEventListener("change", async (e) => {
  try {
    const file = e.target.files[0];
    if (!file) return;
    if (file.size > 512000)
      throw new Error("Choose a YAML file smaller than 512 KB");
    await populate(await api("config/parse", { yaml: await file.text() }));
    setEditor(false);
    notify("Loaded " + file.name + ". Advanced settings are preserved.");
  } catch (e) {
    notify(e.message, true);
  } finally {
    e.target.value = "";
  }
});
action("download", async () => {
  const c = await currentConfig();
  const { yaml } = await api("config/render", { config: c });
  const url = URL.createObjectURL(
    new Blob([yaml], { type: "application/yaml" }),
  );
  const a = document.createElement("a");
  a.href = url;
  a.download = "batch_config.yml";
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
function showReview(r) {
  plan = r;
  $("confirm-review").checked = false;
  $("submit").disabled = true;
  $("review-error").hidden = true;
  $("review-content").innerHTML =
    `<h3>${esc(r.name)}</h3><p class="run-meta">${esc(r.directory)}</p>
    <div class="review-facts"><span class="tag">MOPPy ${esc(r.runtime.version)}</span><span class="tag">${esc(r.runtime.module)}</span><span class="tag">${r.config.variables.length} variable jobs</span><span class="tag">Monitor: 1 CPU · 4 GB · ${esc(r.runtime.monitor_walltime)}</span></div>
    ${r.warnings.map((w) => `<div class="warning">${esc(w)}</div>`).join("")}
    <details open><summary>Saved batch_config.yml</summary><pre>${esc(r.yaml)}</pre></details><details><summary>Native PBS monitor script</summary><pre>${esc(r.script)}</pre></details>`;
  $("review-dialog").showModal();
}
action("review", async () => {
  notify("Checking paths and loading the MOPPy runtime…");
  const r = await api("plans", {
    config: await currentConfig(),
    name: $("name").value,
    module: $("module").value,
  });
  notify("");
  showReview(r);
  await loadRuns();
});
$("close-review").onclick = $("back-review").onclick = () =>
  $("review-dialog").close();
$("confirm-review").onchange = () =>
  ($("submit").disabled = !$("confirm-review").checked);
action("submit", async () => {
  const r = await api(`runs/${plan.id}/submit`, { sha256: plan.sha256 });
  $("review-dialog").close();
  selected = r.id;
  location.hash = "runs";
  await loadRuns();
  await loadRun(r.id);
  notify(
    r.monitor_job
      ? "Submitted monitor " +
          r.monitor_job +
          ". MOPPy will launch the variable jobs."
      : r.error || r.state,
    !r.monitor_job,
  );
});
document
  .querySelectorAll("#builder input,#builder select,#builder textarea")
  .forEach((el) =>
    el.addEventListener("input", () => {
      plan = null;
      syncSummary();
    }),
  );
$("name").addEventListener("input", syncSummary);
async function loadRuns() {
  const records = await api("runs");
  $("run-count").textContent = records.length;
  $("runs-list").innerHTML = records.length
    ? records
        .map(
          (r) =>
            `<button class="run-card ${r.id === selected ? "selected" : ""}" data-run="${esc(r.id)}"><span class="badge ${esc(r.state)}">${esc(r.state)}</span><h3>${esc(r.name)}</h3><small>${esc(new Date(r.created).toLocaleString())}${r.read_only ? " · attached" : ""}</small></button>`,
        )
        .join("")
    : '<div class="empty"><div class="nav-icon" style="margin:auto">▤</div><h2>Your batches will appear here</h2><p>Create a new batch or attach an existing MOPPy output folder to follow its progress.</p><a href="#build">Create your first batch →</a></div>';
  document.querySelectorAll("[data-run]").forEach(
    (button) =>
      (button.onclick = async () => {
        try {
          selected = button.dataset.run;
          logKind = "monitor_out";
          logVariable = null;
          await loadRun(selected);
          await loadRuns();
        } catch (e) {
          notify(e.message, true);
        }
      }),
  );
}
async function loadLog() {
  if (!selected || !$("log-text")) return;
  const value = await api(
    `runs/${selected}/logs?kind=${encodeURIComponent(logKind)}${logVariable ? "&variable=" + encodeURIComponent(logVariable) : ""}`,
  );
  $("log-text").textContent = value.text;
  $("log-path").textContent = value.path;
}
async function loadRun(id) {
  const r = await api("runs/" + id);
  if (selected !== id) return;
  const s = r.summary;
  const detail = $("run-detail");
  detail.hidden = false;
  if (r.state === "review") {
    detail.innerHTML = `<section class="panel"><h2>${esc(r.name)}</h2><p>This configuration is saved and has not been submitted.</p><div class="inline"><button id="open-saved-review" class="primary">Review saved plan →</button></div></section>`;
    $("open-saved-review").onclick = () => showReview(r);
    return;
  }
  detail.innerHTML = `<section class="panel"><div class="page-heading"><div><h2>${esc(r.name)}</h2><p>${esc(r.directory)}</p></div><span class="badge ${esc(r.state)}">${esc(r.state)}</span></div>
    ${r.error ? `<div class="warning">${esc(r.error)}</div>` : ""}${r.tracking_error ? `<div class="warning">${esc(r.tracking_error)}</div>` : ""}
    ${r.state === "submission uncertain" || r.state === "submitting" ? '<div class="warning">Submission may have reached PBS. Do not submit this batch again.<div class="inline"><button id="reconcile" class="secondary">Find accepted job in PBS</button></div></div>' : ""}
    <div class="stats"><div class="stat"><small>Completed</small><strong>${s.completed}<small> / ${s.total}</small></strong></div><div class="stat"><small>Running</small><strong>${s.running}</strong></div><div class="stat"><small>Pending</small><strong>${s.pending}</strong></div><div class="stat"><small>Failed</small><strong>${s.failed}</strong></div></div>
    <div class="progress-track"><div style="width:${s.percent}%"></div></div><small>${s.percent}% of variables completed · refreshed ${esc(new Date(r.refreshed).toLocaleTimeString())}</small>
    <div class="run-meta">Monitor ${esc(r.monitor_job || "not recorded")} · PBS state: ${esc(r.scheduler?.job_state || "unavailable")}${r.runtime ? " · MOPPy " + esc(r.runtime.version) : ""} · Auto-refresh: 15 seconds</div>
    ${r.scheduler?.error ? `<div class="info">${esc(r.scheduler.error)}</div>` : ""}
    <div class="table-wrap"><table><thead><tr><th>Variable</th><th>Status</th><th>PBS job</th><th>Details</th><th>Logs</th></tr></thead><tbody>${r.tasks.map((t) => `<tr><td><code>${esc(t.variable)}</code></td><td><span class="badge ${esc(t.status)}">${esc(t.status)}</span></td><td>${esc(t.pbs_job_id || "—")}</td><td>${esc(t.error_message || t.end_time || t.start_time || "Waiting")}</td><td><button class="secondary variable-log" data-variable="${esc(t.variable)}">Inspect</button></td></tr>`).join("") || '<tr><td colspan="5">Waiting for task records.</td></tr>'}</tbody></table></div>
    <details><summary>Task metadata, output summaries & compliance</summary><pre>${esc(JSON.stringify(r.tasks, null, 2))}</pre></details></section>
    <section class="panel"><h2>Batch logs</h2><div class="log-tools"><select id="log-kind" aria-label="Log stream"><option value="monitor_out">Monitor · stdout</option><option value="monitor_err">Monitor · stderr</option>${logVariable ? '<option value="worker_out">Selected variable · stdout</option><option value="worker_err">Selected variable · stderr</option>' : ""}</select><small id="selected-variable">${esc(logVariable || "")}</small></div><small id="log-path" class="run-meta"></small><pre id="log-text">Loading log…</pre></section>`;
  $("log-kind").value = logKind;
  $("log-kind").onchange = async (e) => {
    logKind = e.target.value;
    try {
      await loadLog();
    } catch (e) {
      notify(e.message, true);
    }
  };
  document.querySelectorAll(".variable-log").forEach(
    (button) =>
      (button.onclick = async () => {
        logVariable = button.dataset.variable;
        logKind = "worker_err";
        await loadRun(id);
        $("log-text").scrollIntoView({ behavior: "smooth", block: "center" });
      }),
  );
  if ($("reconcile"))
    action("reconcile", async () => {
      await api("runs/" + id + "/reconcile", {});
      await loadRun(id);
    });
  await loadLog();
  const card = document.querySelector(`[data-run="${id}"] .badge`);
  if (card) {
    card.textContent = r.state;
    card.className = "badge " + r.state;
  }
}
action("attach", async () => {
  const r = await api("runs/attach", { path: $("attach-path").value });
  selected = r.id;
  await loadRuns();
  await loadRun(r.id);
  notify("Existing batch attached for monitoring.");
});
action("refresh", async () => {
  await loadRuns();
  if (selected) await loadRun(selected);
});
async function route() {
  const page = ["build", "runs", "guide"].includes(location.hash.slice(1))
    ? location.hash.slice(1)
    : "build";
  ["build", "runs", "guide"].forEach(
    (p) => ($("page-" + p).hidden = p !== page),
  );
  document
    .querySelectorAll("[data-page]")
    .forEach((a) => a.classList.toggle("active", a.dataset.page === page));
  if (page === "runs")
    try {
      await loadRuns();
      if (selected) await loadRun(selected);
    } catch (e) {
      notify(e.message, true);
    }
}
window.addEventListener("hashchange", route);
$("theme").onclick = () => {
  document.body.classList.toggle("dark");
  localStorage.setItem(
    "moppy-theme",
    document.body.classList.contains("dark") ? "dark" : "light",
  );
};
if (localStorage.getItem("moppy-theme") === "dark")
  document.body.classList.add("dark");
async function connect() {
  const health = await api("health");
  $("auth-panel").hidden = true;
  $("user").textContent = health.user;
  $("version").textContent = "v" + health.version;
  $("connection").textContent = health.on_gadi
    ? "Connected to Gadi"
    : "Local workspace";
  $("connection-dot").className = "online";
  if (!Object.keys(config).length) await populate(await api("template"));
  await loadRuns();
  await route();
  if (!health.on_gadi)
    notify(
      "Local preview. Configuration editing is available; runtime preflight and submission require Gadi.",
    );
}
action("connect", async () => {
  token = $("token").value.trim();
  sessionStorage.setItem("moppy-token", token);
  await connect();
});
setInterval(async () => {
  if (
    location.hash !== "#runs" ||
    !selected ||
    busy ||
    poll ||
    document.hidden ||
    $("review-dialog").open
  )
    return;
  poll = true;
  try {
    await loadRun(selected);
  } catch (e) {
    notify(e.message, true);
  } finally {
    poll = false;
  }
}, 15000);
connect().catch((e) => notify(e.message, true));
