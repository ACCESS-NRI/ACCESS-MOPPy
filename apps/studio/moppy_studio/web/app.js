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
    updateProbeButtons();
    if (id === 'request-add') $('request-add-feedback').textContent = '';
    notify("");
    try {
      await fn();
    } catch (e) {
      if (id === 'request-add') $('request-add-feedback').textContent = e.message;
      notify(e.message, true);
    } finally {
      busy = false;
      button.textContent = previous;
      button.disabled = id === "submit" ? !$("confirm-review").checked : false;
      if (id === "use-starter-variables") updateTarget();
      updateProbeButtons();
    }
  });
}
let variableChoices = [];
let variableEvidence = new Map(), requestInfo = null, assessmentInfo = null;
let requestPreview = null, requestSelection = new Set(), requestPreviewKey = '', requestRevision = 0;
const inputLabels = {
  not_scanned: 'Inputs not scanned', not_required: 'Generated / bundled inputs',
  found: 'Inputs found in sample', missing_files: 'No matching input files',
  missing_inputs: 'Fields absent from sample', partial: 'Incomplete scan',
  unavailable: 'Input assessment unavailable',
};
function probeConfig() {
  const c = structuredClone(config);
  for (const key of ['cmip_version', 'model_id', 'experiment_id', 'input_folder', 'start_year', 'end_year']) {
    const value = document.querySelector(`[data-key="${key}"]`).value.trim();
    if (value) c[key] = value; else delete c[key];
  }
  c.variables = variableChoices.map(v => v.name);
  return c;
}
function probeFingerprint() {
  const c = probeConfig();
  return JSON.stringify([c.cmip_version, c.model_id, c.experiment_id, c.input_folder,
    c.start_year, c.end_year, c.file_patterns, c.variables, $('module').value,
    $('request-version').value, $('request-priority').value, $('request-offline').checked]);
}
function updateProbeButtons() {
  const cmip7 = $('cmor-target').value === 'CMIP7';
  $('request-load').disabled = $('request-experiments-load').disabled = busy || !cmip7;
  $('request-panel').classList.toggle('inactive', !cmip7);
  $('request-target-note').hidden = cmip7;
  $('request-model-context').textContent = 'Mapping support will be checked for ' +
    (document.querySelector('[data-key="model_id"]').value || 'the model selected above') + '.';
  $('request-add').disabled = busy || !cmip7 || !requestPreview || !requestSelection.size;
  if (!busy) $('request-add').textContent = requestSelection.size
    ? `Add ${requestSelection.size} selected variables to batch →` : 'Add selected variables to batch →';
  for (const id of ['request-select-mapped', 'request-select-visible', 'request-clear-selection']) {
    $(id).disabled = busy || !requestPreview?.variables.length;
  }
  $('request-list').querySelectorAll('input').forEach(el => el.disabled = busy);
  $('scan-inputs').disabled = busy || !variableChoices.length;
  $('select-mapped').disabled = busy || ![...variableEvidence.values()].some(r => r.mapping === 'mapped');
  $('select-found').disabled = busy || ![...variableEvidence.values()].some(r => r.input_status === 'found');
  $('download-assessment').disabled = busy || !variableEvidence.size;
}
function clearEvidence(message = '') {
  variableEvidence = new Map(); requestInfo = assessmentInfo = null;
  $('assessment-status').textContent = message || 'Check the model mapping and sample NetCDF headers in the input folder. No job is submitted.';
  $('variable-assessment').value = '';
  updateProbeButtons();
}
function clearRequestPreview(message = '') {
  requestRevision++;
  requestPreview = null; requestSelection = new Set(); requestPreviewKey = '';
  $('request-results').hidden = true;
  $('request-list').replaceChildren();
  $('request-add-feedback').textContent = '';
  $('request-status').textContent = message || 'Enter an experiment and find its requested variables. Your batch variable list stays unchanged until you add a selection.';
  updateProbeButtons();
}
function requestFingerprint() {
  const c = probeConfig();
  return JSON.stringify([requestRevision, c.cmip_version, c.model_id, c.experiment_id,
    $('module').value, $('request-version').value, $('request-priority').value, $('request-offline').checked]);
}
function visibleRequestRows() {
  const query = $('request-search').value.trim().toLowerCase(), mapping = $('request-mapping').value;
  return (requestPreview?.variables || []).filter(r => r.name.toLowerCase().includes(query) && (!mapping || r.mapping === mapping));
}
function syncRequestSelection() {
  const known = new Map(variableChoices.map(v => [v.name, v.selected]));
  const existing = [...requestSelection].filter(name => known.has(name)).length;
  $('request-selection-count').textContent = `${requestSelection.size} selected in these results · ${requestSelection.size - existing} new to your batch · ${existing} already in its checklist`;
  $('request-list').querySelectorAll('[data-existing-request]').forEach(tag => {
    const name = tag.dataset.existingRequest;
    tag.hidden = !known.has(name);
    tag.textContent = known.get(name) ? 'Already selected in batch' : 'Already listed, unchecked';
  });
  updateProbeButtons();
}
function renderRequestPreview() {
  if (!requestPreview) return;
  const known = new Map(variableChoices.map(v => [v.name, v.selected]));
  const visible = visibleRequestRows();
  $('request-list').innerHTML = visible.map(r => `<label class="request-choice"><input type="checkbox" data-request-name="${esc(r.name)}" ${requestSelection.has(r.name) ? 'checked' : ''}><span><code>${esc(r.name)}</code><span class="evidence-tags"><span class="tag ${r.mapping === 'mapped' ? '' : 'evidence-warning'}">${r.mapping === 'mapped' ? 'Mapped by MOPPy' : 'No supported mapping'}</span><span class="tag">${esc(r.priority)}</span><span class="tag" data-existing-request="${esc(r.name)}" ${known.has(r.name) ? '' : 'hidden'}></span></span><small>${[r.reason, r.time_subsets?.length ? 'Time subsets: ' + r.time_subsets.join(', ') : ''].filter(Boolean).map(esc).join(' · ')}</small></span></label>`).join('') || '<p class="variable-empty">No request variables match these filters.</p>';
  $('request-visible').textContent = visible.length + ' of ' + requestPreview.variables.length + ' shown';
  syncRequestSelection();
}
function evidenceMatches(name, filter) {
  const r = variableEvidence.get(name);
  if (!filter) return true;
  if (filter === 'mapped' || filter === 'unsupported') return r?.mapping === filter;
  if (filter === 'found') return r?.input_status === 'found';
  if (filter === 'missing') return ['missing_files', 'missing_inputs'].includes(r?.input_status);
  return !r || (r.mapping === 'mapped' && ['not_scanned', 'partial', 'unavailable', 'not_required'].includes(r.input_status));
}
function evidenceHTML(name) {
  const r = variableEvidence.get(name);
  if (!r) return '';
  const mapped = r.mapping === 'mapped';
  const tags = `<div class="evidence-tags"><span class="tag ${mapped ? '' : 'evidence-warning'}">${mapped ? 'Mapped' : 'No supported mapping'}</span>${mapped ? `<span class="tag">${esc(inputLabels[r.input_status] || r.input_status)}</span>` : ''}${r.priority ? `<span class="tag">${esc(r.priority)}</span>` : ''}</div>`;
  const lines = [r.reason,
    r.alias ? 'MOPPy table alias: ' + r.alias : '',
    r.required_inputs?.length ? 'Required native fields: ' + r.required_inputs.join(', ') : '',
    r.optional_inputs?.length ? 'Optional native fields: ' + r.optional_inputs.join(', ') : '',
    r.time_subsets?.length ? 'Requested time subsets: ' + r.time_subsets.join(', ') + ' (not applied to batch years)' : '',
    r.missing_inputs?.length ? 'Not found in sampled headers: ' + r.missing_inputs.join(', ') : '',
    r.patterns?.length ? 'Patterns: ' + r.patterns.join('; ') : '',
    r.explicit_patterns ? 'Uses imported file_patterns; native override discovery does not filter filenames by year.' : '',
    r.sample_files?.length ? 'Headers sampled: ' + r.sample_files.slice(0, 6).join('; ') : '',
    r.sampled ? 'Only a sample of matching files was opened.' : '',
    r.limited ? 'Scan limit reached; evidence is incomplete.' : '',
    r.unknown_year ? 'Some filename years could not be parsed; those files were retained.' : '',
    ...(r.errors || []).map(e => e.file + ': ' + e.error),
  ].filter(Boolean);
  return tags + `<details class="evidence-details"><summary>Request & input evidence</summary>${lines.map(s => `<p>${esc(s)}</p>`).join('')}</details>`;
}
async function loadRequest(catalogOnly) {
  const c = probeConfig();
  if (c.cmip_version !== 'CMIP7') throw new Error('Choose CMIP7 to search the experiment request. Input assessment supports all three targets.');
  if (!catalogOnly && !c.experiment_id) throw new Error('Enter an Experiment ID in step 1, or load the experiment suggestions first.');
  clearRequestPreview('Loading the official CMIP7 request and installed mappings… Your batch variable list is unchanged.');
  const before = requestFingerprint();
  try {
    const result = await api('request', {request_version: $('request-version').value.trim(),
      priority: $('request-priority').value, model_id: c.model_id,
      experiment: catalogOnly ? '' : c.experiment_id, module: $('module').value,
      offline: $('request-offline').checked});
    if (before !== requestFingerprint()) {
      $('request-status').textContent = 'Settings changed during lookup. Find requested variables again.'; return;
    }
    $('request-experiments').innerHTML = result.experiments.map(e => `<option value="${esc(e)}"></option>`).join('');
    if (catalogOnly) {
      $('request-status').textContent = `${result.experiments.length} experiments available. Type or choose an Experiment ID in step 1, then click Find requested variables.`;
      $('request-experiment').focus(); return;
    }
    checkVariables(result.variables.map(r => r.name));
    requestPreview = result; requestPreviewKey = before;
    requestSelection = new Set(result.variables.filter(r => r.mapping === 'mapped').map(r => r.name));
    $('request-search').value = ''; $('request-mapping').value = '';
    const mapped = result.variables.filter(r => r.mapping === 'mapped').length;
    $('request-result-summary').textContent = `${result.experiment} · ${result.request_version} · through ${result.priority} · all opportunities: ${result.variables.length} requested, ${mapped} mapped, ${result.variables.length - mapped} without a supported mapping.`;
    $('request-status').textContent = 'Request found. Review the ticked results in step 2, then use Add selected variables to batch in step 3. Nothing has been added yet.';
    $('request-results').hidden = false;
    renderRequestPreview();
  } catch (e) {
    $('request-status').textContent = 'Request lookup failed. ' + e.message;
    throw e;
  }
}
action('request-experiments-load', () => loadRequest(true));
action('request-load', () => loadRequest(false));
action('request-add', async () => {
  if (!requestPreview || requestPreviewKey !== requestFingerprint()) throw new Error('Request settings changed. Find requested variables again before adding them.');
  if (!requestSelection.size) throw new Error('Tick at least one request variable to add.');
  const known = new Map(variableChoices.map(v => [v.name, v]));
  const additions = [...requestSelection].filter(name => !known.has(name));
  if (known.size + additions.length > 2000) throw new Error('This selection would exceed 2000 variables in the batch. Select fewer results; your batch is unchanged.');
  for (const r of requestPreview.variables) {
    if (!requestSelection.has(r.name)) continue;
    if (known.has(r.name)) known.get(r.name).selected = true;
    else variableChoices.push({name: r.name, selected: true, resources: structuredClone(config.variable_resources?.[r.name] || {})});
    variableEvidence.set(r.name, {...r, ...variableEvidence.get(r.name), priority: r.priority, time_subsets: r.time_subsets});
  }
  requestInfo = requestPreview;
  $('variable-search').value = ''; $('variable-table').value = ''; $('variable-assessment').value = '';
  const existing = requestSelection.size - additions.length;
  const message = `Added ${additions.length} new variable${additions.length === 1 ? '' : 's'} and selected ${existing} existing ${existing === 1 ? 'entry' : 'entries'}. Your batch now includes ${selectedVariables().length} selected variables.`;
  $('request-status').textContent = message;
  $('assessment-status').textContent = `Request selection added from ${requestInfo.request_version}. Mapping lookup: MOPPy ${requestInfo.version} · ${requestInfo.module}. Scan inputs to assess these variables.`;
  builderChanged(); renderVariables(); renderRequestPreview();
  $('request-added').hidden = false;
  $('request-added').textContent = message + ' Next: check your input folder, scan inputs and review the batch settings.';
  await openBuilderSection('variables-panel');
});
$('request-experiment').addEventListener('input', () => {
  const field = document.querySelector('[data-key="experiment_id"]');
  field.value = $('request-experiment').value;
  field.dispatchEvent(new Event('input', {bubbles: true}));
});
$('request-search').addEventListener('input', renderRequestPreview);
$('request-mapping').addEventListener('change', renderRequestPreview);
$('request-list').addEventListener('change', event => {
  const name = event.target.dataset.requestName;
  if (!name || busy) return;
  if (event.target.checked) requestSelection.add(name); else requestSelection.delete(name);
  syncRequestSelection();
});
for (const [id, select] of [
  ['request-select-mapped', () => new Set(requestPreview.variables.filter(r => r.mapping === 'mapped').map(r => r.name))],
  ['request-select-visible', () => new Set([...requestSelection, ...visibleRequestRows().map(r => r.name)])],
  ['request-clear-selection', () => new Set()],
]) {
  $(id).addEventListener('click', () => { if (busy || !requestPreview) return; requestSelection = select(); renderRequestPreview(); });
}
action('scan-inputs', async () => {
  const c = probeConfig(), before = probeFingerprint();
  $('assessment-status').textContent = 'Scanning mapping-specific input streams and sampling NetCDF headers…';
  try {
    const result = await api('assessment', {config: c, module: $('module').value});
    if (before !== probeFingerprint()) {
      $('assessment-status').textContent = 'Settings changed during the scan. Scan again for current evidence.'; return;
    }
    for (const r of result.variables) variableEvidence.set(r.name, {...variableEvidence.get(r.name), ...r});
    assessmentInfo = result;
    const found = result.variables.filter(r => r.input_status === 'found').length;
    const unsupported = result.variables.filter(r => r.mapping !== 'mapped').length;
    $('assessment-status').textContent = `Last scan: ${found} with inputs found in sampled headers · ${unsupported} without a supported mapping · ${result.scan.headers_read} headers read. MOPPy ${result.version} · ${result.module}. Full coverage and scientific checks remain.`;
    renderVariables();
  } catch (e) { $('assessment-status').textContent = 'Input assessment failed. ' + e.message; throw e; }
});
for (const [id, predicate] of [['select-mapped', r => r.mapping === 'mapped'], ['select-found', r => r.input_status === 'found']]) {
  $(id).addEventListener('click', () => {
    if (busy) return;
    for (const v of variableChoices) { const r = variableEvidence.get(v.name); v.selected = Boolean(r && predicate(r)); }
    builderChanged(); renderVariables();
  });
}
action('download-assessment', async () => {
  const report = {request: requestInfo, assessment: assessmentInfo,
    config: probeConfig(), selected_variables: selectedVariables(), variables: [...variableEvidence.values()],
    limitations: 'Header samples do not validate complete time coverage, units, grids, auxiliary resources or scientific correctness. Request time subsets are not applied to the batch.'};
  const url = URL.createObjectURL(new Blob([JSON.stringify(report, null, 2)], {type: 'application/json'}));
  const a = document.createElement('a'); a.href = url; a.download = 'moppy-input-assessment.json'; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
for (const id of ['request-version', 'request-priority', 'request-offline', 'module']) {
  $(id).addEventListener('input', () => {
    if (id === 'request-version') $('request-experiments').innerHTML = '';
    clearRequestPreview('Request settings changed. Find requested variables again to preview the updated request.');
    if (id === 'module') { clearEvidence('Runtime changed. Scan inputs for current evidence.'); renderVariables(); }
  });
}
$('builder').addEventListener('input', event => {
  if (['cmip_version', 'model_id', 'experiment_id'].includes(event.target.dataset.key)) {
    $('request-experiment').value = document.querySelector('[data-key="experiment_id"]').value;
    clearRequestPreview('Batch identity changed. Find requested variables again for this experiment and model.');
  }
  if (['cmip_version', 'model_id', 'experiment_id', 'input_folder', 'start_year', 'end_year'].includes(event.target.dataset.key)) {
    clearEvidence('Batch settings changed. Reload the request or scan inputs for current evidence.'); renderVariables();
  }
});
$('variable-assessment').addEventListener('change', renderVariables);
const resourceFields = [
  ['queue', 'Queue', 'text'], ['cpus_per_node', 'CPUs', 'number'],
  ['mem', 'Memory', 'text'], ['jobfs', 'Job filesystem', 'text'],
  ['walltime', 'Walltime', 'text'],
];
const isMapping = value => value !== null && typeof value === 'object' && !Array.isArray(value);
function checkVariables(variables) {
  if (!Array.isArray(variables) || variables.some(v => typeof v !== 'string' ||
      !/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(v) || !v.includes('.'))) {
    throw new Error('Use CMIP7 compound identifiers (atmos.tas.tavg-h2m-hxy-u.mon.glb), or table.variable names for CMIP6 (Amon.tas).');
  }
  if (variables.length > 2000) throw new Error('Choose at most 2000 variables.');
  if (new Set(variables).size !== variables.length) throw new Error('Remove duplicate variable names in the YAML.');
}
const cmip7Examples = ['atmos.tas.tavg-h2m-hxy-u.mon.glb', 'atmos.pr.tavg-u-hxy-u.mon.glb', 'ocean.tos.tavg-u-hxy-sea.mon.glb'];
const tableExamples = ['Amon.tas', 'Amon.pr', 'Omon.tos'];
function updateTarget() {
  const target = $('cmor-target').value;
  const examples = target === 'CMIP7' ? cmip7Examples : tableExamples;
  $('variable-add').placeholder = examples.join('\n');
  $('variable-format').textContent = target + ': ' + examples[0];
  $('target-help').textContent = target === 'CMIP7'
    ? 'CMIP7 uses compound names with realm, variable, branding, frequency and region. MOPPy also supports mapped table.variable aliases.'
    : target === 'CMIP6Plus'
      ? 'CMIP6Plus uses table.variable names with its own controlled vocabulary. MOPPy supports legacy tables (Amon.tas) and MIP tables (APmon.tas) when available in the runtime.'
      : target === 'CMIP6'
        ? 'CMIP6 uses table.variable names, such as Amon.tas, with CMIP6 controlled vocabularies and tables.'
        : 'Check the installed MOPPy runtime supports this imported target.';
  $('use-starter-variables').hidden = !['CMIP6', 'CMIP6Plus', 'CMIP7'].includes(target);
  $('use-starter-variables').textContent = 'Use ' + target + ' starter variables';
  const incompatible = ['CMIP6', 'CMIP6Plus'].includes(target) && selectedVariables().some(name => name.split('.').length !== 2);
  $('target-variables-note').hidden = !incompatible;
  $('target-variables-note').textContent = incompatible
    ? target + ' needs table.variable names. Replace or deselect the selected CMIP7 names, load a matching config, or use the starter list above.' : '';
  updateProbeButtons();
}
$('cmor-target').addEventListener('change', () => { builderChanged(); updateTarget(); });
action('use-starter-variables', async () => {
  if ($('variable-add').value.trim()) throw new Error('Add or clear the names you entered before replacing the variable list.');
  const target = $('cmor-target').value;
  const data = await api('template?cmip_version=' + encodeURIComponent(target));
  // A slow response must not replace variables after the user switches target.
  if ($('cmor-target').value !== target) return;
  const current = new Map(variableChoices.map(v => [v.name, v.resources]));
  variableChoices = data.config.variables.map(name => ({name, selected: true,
    resources: structuredClone(current.get(name) || config.variable_resources?.[name] || {})}));
  clearEvidence();
  $('variable-search').value = ''; $('variable-table').value = '';
  builderChanged(); renderVariables();
  notify(target + ' starter variables loaded. Your paths, resources and metadata are retained.');
});
function checkTargetNames(c) {
  if (['CMIP6', 'CMIP6Plus'].includes(c.cmip_version) && c.variables.some(name => name.split('.').length !== 2)) {
    throw new Error(c.cmip_version + ' requires table.variable names, e.g. Amon.tas; replace CMIP7 compound names or choose CMIP7.');
  }
}
function selectedVariables() {
  return variableChoices.filter(v => v.selected).map(v => v.name);
}
function visibleVariables() {
  const query = $('variable-search').value.trim().toLowerCase();
  const table = $('variable-table').value;
  return variableChoices.filter(v => v.name.toLowerCase().includes(query) &&
    (!table || v.name.split('.')[0] === table) && evidenceMatches(v.name, $('variable-assessment').value));
}
function syncSummary(snapshot) {
  const value = k => snapshot ? snapshot[k] : document.querySelector(`[data-key="${k}"]`).value;
  const count = snapshot ? (Array.isArray(snapshot.variables) ? snapshot.variables.length : 0) : selectedVariables().length;
  $('variable-count').textContent = selectedVariables().length + ' of ' + variableChoices.length + ' selected';
  $('summary-name').textContent = $('name').value || 'New CMORisation batch';
  $('summary-target').textContent = value('cmip_version') || 'CMIP6';
  $('summary-model').textContent = value('source_id') || '—';
  updateTarget();
  $('summary-experiment').textContent = value('experiment_id') || '—';
  $('summary-vars').textContent = count;
  $('summary-resources').textContent = (value('cpus_per_node') || '—') + ' CPU · ' + (value('mem') || '—');
  $('home-draft-count').textContent = count;
  $('home-draft-context').textContent = [value('cmip_version') || 'CMIP6', value('source_id'), value('experiment_id')].filter(Boolean).join(' · ');
}
function renderVariables() {
  const table = $('variable-table').value;
  const tables = [...new Set(variableChoices.map(v => v.name.split('.')[0]))].sort();
  $('variable-table').innerHTML = '<option value="">All tables / realms</option>' + tables.map(t => `<option value="${esc(t)}">${esc(t)}</option>`).join('');
  $('variable-table').value = tables.includes(table) ? table : '';
  const visible = new Set(visibleVariables());
  $('variable-list').innerHTML = variableChoices.map((v, index) => {
    if (!visible.has(v)) return '';
    return `<div class="variable-row ${v.selected ? 'included' : ''}" data-choice="${index}">
      <div class="variable-heading"><label><input type="checkbox" data-select-variable ${v.selected ? 'checked' : ''}><code>${esc(v.name)}</code></label>
      <button type="button" class="icon-button remove-variable" aria-label="Remove ${esc(v.name)}">×</button></div>${evidenceHTML(v.name)}
      <details><summary>Resources · <span class="resource-status">${Object.keys(v.resources).length ? 'custom overrides' : 'batch defaults'}</span></summary>
      <p>Leave a field empty to inherit the batch resource. Unchecked variables keep their settings while you edit.</p>
      <div class="form-grid three">${resourceFields.map(([key, label, type]) => `<label>${label}<input data-resource="${key}" type="${type}" ${type === 'number' ? 'min="1" step="1"' : ''} value="${esc(v.resources[key] ?? '')}" placeholder="Batch default" aria-label="${esc(v.name)} ${label}"></label>`).join('')}</div></details></div>`;
  }).join('') || `<p class="variable-empty">${variableChoices.length ? 'No variables match these filters.' : 'Add variables below or load a batch config to get started.'}</p>`;
  $('variable-visible').textContent = visible.size + ' shown';
  $('select-visible').disabled = $('clear-visible').disabled = visible.size === 0;
  syncSummary();
  if (requestPreview) syncRequestSelection();
}
function builderChanged() { plan = null; $('request-added').hidden = true; syncSummary(); if (requestPreview) syncRequestSelection(); }
$('variable-search').addEventListener('input', renderVariables);
$('variable-table').addEventListener('change', renderVariables);
for (const [id, selected] of [['select-visible', true], ['clear-visible', false]]) {
  $(id).addEventListener('click', () => {
    visibleVariables().forEach(v => v.selected = selected);
    builderChanged(); renderVariables();
  });
}
$('variable-list').addEventListener('input', event => {
  const row = event.target.closest('[data-choice]');
  if (!row) return;
  const choice = variableChoices[Number(row.dataset.choice)];
  if (event.target.matches('[data-select-variable]')) {
    choice.selected = event.target.checked;
    row.classList.toggle('included', choice.selected);
  } else if (event.target.dataset.resource) {
    const key = event.target.dataset.resource;
    const value = event.target.value.trim();
    // Retain invalid number input until it can be reported, rather than inheriting silently.
    if (event.target.validity.badInput) choice.resources[key] = null;
    else if (!value) delete choice.resources[key];
    else choice.resources[key] = event.target.type === 'number' ? Number(value) : value;
    row.querySelector('.resource-status').textContent = Object.keys(choice.resources).length ? 'custom overrides' : 'batch defaults';
  }
  builderChanged();
});
$('variable-list').addEventListener('click', event => {
  if (!event.target.closest('.remove-variable')) return;
  const index = Number(event.target.closest('[data-choice]').dataset.choice);
  const [removed] = variableChoices.splice(index, 1);
  variableEvidence.delete(removed.name);
  if (isMapping(config.variable_resources)) delete config.variable_resources[removed.name];
  builderChanged(); renderVariables();
});
action('add-variables', async () => {
  const names = [...new Set($('variable-add').value.split(/[\s,]+/).filter(Boolean))];
  if (!names.length) throw new Error('Enter at least one variable to add.');
  checkVariables(names);
  const known = new Map(variableChoices.map(v => [v.name, v]));
  if (new Set([...known.keys(), ...names]).size > 2000) throw new Error('Keep at most 2000 variables in the builder.');
  for (const name of names) {
    if (known.has(name)) known.get(name).selected = true;
    else variableChoices.push({name, selected: true, resources: structuredClone(config.variable_resources?.[name] || {})});
  }
  $('variable-add').value = ''; $('variable-search').value = ''; $('variable-table').value = '';
  builderChanged(); renderVariables();
  notify(names.length + ' variable' + (names.length === 1 ? '' : 's') + ' selected. Existing entries are kept once.');
});
async function populate(data, keepChoices = false) {
  const next = structuredClone(data.config);
  const names = next.variables ?? [];
  checkVariables(names);
  if ('variable_resources' in next && (!isMapping(next.variable_resources) ||
      Object.values(next.variable_resources).some(v => !isMapping(v)))) {
    throw new Error('variable_resources must map variable names to resource settings.');
  }
  for (const key of ['qc_plots', 'resume', 'enable_chunking', 'use_jobfs_staging']) {
    if (key in next && typeof next[key] !== 'boolean') throw new Error(key + ' must be true or false.');
  }
  // Complete async parsing before replacing any of the user's current draft.
  const parent = next.parent_info ? (await api('config/render', {config: next.parent_info})).yaml : '';
  const retained = keepChoices ? variableChoices.filter(v => !names.includes(v.name)) : [];
  config = next;
  clearEvidence();
  clearRequestPreview();
  $('request-added').hidden = true;
  // New configurations use the template's CMIP7 defaults; unversioned imports retain legacy semantics.
  config.cmip_version ??= 'CMIP6'; config.model_id ??= 'ACCESS-ESM1-6';
  variableChoices = [...names.map(name => ({name, selected: true})), ...retained.map(v => ({name: v.name, selected: false}))]
    .map(v => ({...v, resources: structuredClone(config.variable_resources?.[v.name] || {})}));
  document.querySelectorAll('[data-key]').forEach(el => {
    const value = config[el.dataset.key];
    if (el.type === 'checkbox') el.checked = value === true;
    else el.value = value ?? '';
  });
  $('request-experiment').value = config.experiment_id || '';
  $('project').value = String(config.scheduler_options || '').match(/^#PBS -P (\S+)\s*$/)?.[1] || '';
  $('yaml').value = data.yaml || '';
  $('parent-info').value = parent;
  if (!keepChoices) {
    $('variable-add').value = ''; $('variable-search').value = ''; $('variable-table').value = '';
  }
  plan = null; renderVariables();
}
async function fromBuilder() {
  if ($('variable-add').value.trim()) throw new Error('Click Add variables to include the names you entered, or clear the entry before continuing.');
  const c = structuredClone(config);
  document.querySelectorAll('[data-key]').forEach(el => {
    const key = el.dataset.key;
    if (el.type === 'number' && !el.checkValidity()) throw new Error(key + ' must be a positive whole number.');
    if (el.type === 'checkbox') {
      if (key in c || el.checked) c[key] = el.checked;
    } else if (el.value.trim() === '') delete c[key];
    else if (el.dataset.type === 'boolean') c[key] = el.value === 'true';
    else c[key] = el.type === 'number' ? Number(el.value) : el.value.trim();
  });
  c.variables = selectedVariables();
  const overrides = structuredClone(config.variable_resources || {});
  for (const choice of variableChoices) {
    if ('cpus_per_node' in choice.resources && (!Number.isInteger(choice.resources.cpus_per_node) || choice.resources.cpus_per_node < 1)) {
      throw new Error(choice.name + ': CPUs must be a positive whole number.');
    }
    if (Object.keys(choice.resources).length) overrides[choice.name] = structuredClone(choice.resources);
    else delete overrides[choice.name];
  }
  if (Object.keys(overrides).length) c.variable_resources = overrides;
  else delete c.variable_resources;
  const project = $('project').value.trim();
  if (project) c.scheduler_options = '#PBS -P ' + project;
  else if (/^#PBS -P \S+\s*$/.test(config.scheduler_options || '')) delete c.scheduler_options;
  const parent = $('parent-info').value.trim();
  if (parent) c.parent_info = (await api('config/parse', {yaml: parent})).config;
  else delete c.parent_info;
  if (c.script_dir === config.output_folder) c.script_dir = c.output_folder;
  return c;
}
async function currentConfig() {
  const c = yamlMode ? (await api('config/parse', {yaml: $('yaml').value})).config : await fromBuilder();
  checkVariables(c.variables);
  checkTargetNames(c);
  if (!c.variables.length) throw new Error('Select at least one variable before downloading or reviewing the batch.');
  return c;
}
let yamlRevision = 0, yamlTimer;
$('yaml').addEventListener('input', () => {
  plan = null;
  const revision = ++yamlRevision;
  clearTimeout(yamlTimer);
  for (const id of ['summary-target', 'summary-model', 'summary-experiment', 'summary-vars', 'summary-resources']) $(id).textContent = 'Editing YAML…';
  $('home-draft-count').textContent = '…';
  $('home-draft-context').textContent = 'Editing YAML…';
  yamlTimer = setTimeout(async () => {
    try {
      const parsed = await api('config/parse', {yaml: $('yaml').value});
      if (yamlMode && revision === yamlRevision) syncSummary(parsed.config);
    } catch (_) {
      if (yamlMode && revision === yamlRevision) {
        $('summary-vars').textContent = $('home-draft-context').textContent = 'Invalid YAML';
        $('home-draft-count').textContent = '—';
      }
    }
  }, 350);
});
function setEditor(mode) {
  yamlRevision++;
  yamlMode = mode;
  $("builder").hidden = mode;
  $("yaml-panel").hidden = !mode;
  $("tab-builder").classList.toggle("selected", !mode);
  $("tab-yaml").classList.toggle("selected", mode);
  $("tab-builder").setAttribute("aria-selected", String(!mode));
  $("tab-yaml").setAttribute("aria-selected", String(mode));
  syncSummary();
}
action("tab-yaml", async () => {
  if (yamlMode) return;
  const next = await fromBuilder();
  const rendered = await api("config/render", { config: next });
  config = next;
  $("yaml").value = rendered.yaml;
  setEditor(true);
});
action("tab-builder", async () => {
  if (!yamlMode) return;
  await populate(await api("config/parse", { yaml: $("yaml").value }), true);
  setEditor(false);
});
action("apply-yaml", async () => {
  await populate(await api("config/parse", { yaml: $("yaml").value }), true);
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
    <div class="review-facts"><span class="tag">Target: ${esc(r.config.cmip_version || 'CMIP6')}</span><span class="tag">MOPPy ${esc(r.runtime.version)}</span><span class="tag">${esc(r.runtime.module)}</span><span class="tag">${r.config.variables.length} variable jobs</span><span class="tag">Monitor: 1 CPU · 4 GB · ${esc(r.runtime.monitor_walltime)}</span></div>
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
$("name").addEventListener("input", () => syncSummary());
async function loadRuns() {
  let records;
  try { records = await api("runs"); }
  catch (e) {
    $('home-review-count').textContent = $('home-run-count').textContent = '—';
    $('home-recent').textContent = 'Batch history is unavailable. Use Refresh summary to try again.';
    throw e;
  }
  $("run-count").textContent = records.length;
  $('home-review-count').textContent = records.filter(r => r.state === 'review').length;
  $('home-run-count').textContent = records.length;
  $('home-recent').innerHTML = records.length
    ? records.slice(0, 4).map(r => `<button class="run-card" data-open-run="${esc(r.id)}"><span class="badge ${esc(r.state)}">${esc(r.state)}</span><h3>${esc(r.name)}</h3><small>${esc(new Date(r.created).toLocaleString())}${r.read_only ? ' · attached' : ''}</small></button>`).join('')
    : '<div class="home-empty"><h3>No saved batches yet</h3><p>Start with the options above. Batches appear here after you save a review or attach an existing run.</p></div>';
  $('home-recent').querySelectorAll('[data-open-run]').forEach(button => button.onclick = () => {
    selected = button.dataset.openRun; logKind = 'monitor_out'; logVariable = null; location.hash = 'runs';
  });
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
    detail.innerHTML = `<section class="panel"><h2>${esc(r.name)}</h2><p>This configuration is saved and has not been submitted.</p><div class="inline"><button id="open-saved-review" class="primary">Review saved plan →</button><button id="edit-saved-plan" class="secondary">Edit configuration</button></div></section>`;
    $("open-saved-review").onclick = () => showReview(r);
    action('edit-saved-plan', async () => {
      await populate(r); $('name').value = r.name; setEditor(false);
      location.hash = 'build';
      notify('Saved plan loaded for editing. Check & review creates a new plan with your changes.');
    });
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
action('home-refresh', loadRuns);
async function openBuilderSection(id) {
  location.hash = 'build';
  await route(false);
  if (yamlMode) {
    await populate(await api('config/parse', {yaml: $('yaml').value}), true);
    setEditor(false);
  }
  $(id).scrollIntoView({behavior: 'smooth', block: 'start'});
  $(id).focus({preventScroll: true});
}
document.querySelectorAll('[data-build-section]').forEach(link => link.addEventListener('click', event => {
  event.preventDefault();
  if (busy) return;
  openBuilderSection(link.dataset.buildSection).catch(e => notify(e.message, true));
}));
async function route(refresh = true) {
  const page = ["home", "build", "runs", "guide"].includes(location.hash.slice(1))
    ? location.hash.slice(1)
    : "home";
  ["home", "build", "runs", "guide"].forEach(
    (p) => ($("page-" + p).hidden = p !== page),
  );
  document
    .querySelectorAll("[data-page]")
    .forEach((a) => {
      a.classList.toggle("active", a.dataset.page === page);
      if (a.dataset.page === page) a.setAttribute('aria-current', 'page'); else a.removeAttribute('aria-current');
    });
  document.title = ({home: 'Overview', build: 'Batch builder', runs: 'Runs & progress', guide: 'Workflow guide'})[page] + ' · MOPPy Studio (Beta)';
  if (refresh && ['home', 'runs'].includes(page))
    try {
      await loadRuns();
      if (page === 'runs' && selected) await loadRun(selected);
    } catch (e) {
      notify(e.message, true);
    }
}
window.addEventListener("hashchange", () => route());
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
  await route(false);
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
route(false).catch((e) => notify(e.message, true));
connect().catch((e) => notify(e.message, true));
