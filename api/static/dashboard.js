// dashboard.js
const AUTO_REFRESH_MS = 10000;

let incidentsOverTimeChart;
let severityDistributionChart;
let topSegmentsChart;
let speedComparisonChart;

const state = {
  currentIncidents: [],
  history: [],
  summary: null,
  selectedSegment: null,
};

function byId(id) {
  return document.getElementById(id);
}

function num(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function parseTime(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function fmtTime(value) {
  const d = parseTime(value);
  return d ? d.toLocaleString() : "--";
}

function normalizeStatus(value) {
  const status = String(value || "open").trim().toLowerCase();
  return status === "close" || status === "closed" ? "closed" : "open";
}

function isMissingClosedTime(value) {
  const d = parseTime(value);
  return !d || d.getTime() <= 0 || d.getUTCFullYear() < 1971;
}

function fmtClosedTime(row) {
  if (normalizeStatus(row?.status) !== "closed") return "Open";
  return isMissingClosedTime(row?.closed_at) ? "--" : fmtTime(row.closed_at);
}

function fmtNumber(value, digits = 1) {
  return num(value).toFixed(digits);
}

function severityBadgeClass(severity) {
  const s = num(severity);
  if (s >= 7) return "badge badge-open";
  if (s >= 4) return "badge badge-mid";
  return "badge badge-closed";
}

function statusBadgeClass(status) {
  return normalizeStatus(status) === "closed" ? "badge badge-closed" : "badge badge-open";
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normalizeIncident(row) {
  return {
    raw: row,
    incident_id: String(row?.incident_id ?? ""),
    segment_id: String(row?.segment_id ?? "--"),
    severity: num(row?.severity, 0),
    status: normalizeStatus(row?.status),
    current_speed_mph: num(row?.current_speed_mph, 0),
    baseline_speed_mph: num(row?.baseline_speed_mph, 0),
    opened_at: row?.opened_at ?? null,
    closed_at:
      normalizeStatus(row?.status) === "closed" && !isMissingClosedTime(row?.closed_at)
        ? row.closed_at
        : null,
    updated_at: row?.updated_at ?? null,
    ingested_at: row?.ingested_at ?? null,
    observed_count: num(row?.observed_count, 0),
    start_ts: row?.start_ts ?? null,
    end_ts: row?.end_ts ?? null,
  };
}

function getHistoryIncidentKey(row) {
  if (row.incident_id && row.incident_id.trim()) {
    return row.incident_id.trim();
  }

  const segmentId = String(row.segment_id || "--").trim();
  const openedAt = String(row.opened_at || "").trim();
  return `${segmentId}__${openedAt}`;
}

function getHistoryRowSortTs(row) {
  const openedTs = parseTime(row.opened_at)?.getTime() || 0;
  const closedTs = parseTime(row.closed_at)?.getTime() || 0;
  return Math.max(openedTs, closedTs);
}

function shouldReplaceHistoryRow(existing, candidate) {
  if (!existing) return true;

  const existingClosed = normalizeStatus(existing.status) === "closed";
  const candidateClosed = normalizeStatus(candidate.status) === "closed";

  if (candidateClosed && !existingClosed) return true;
  if (existingClosed && !candidateClosed) return false;

  const existingTs = getHistoryRowSortTs(existing);
  const candidateTs = getHistoryRowSortTs(candidate);

  if (candidateTs > existingTs) return true;
  if (candidateTs < existingTs) return false;

  return num(candidate.severity) >= num(existing.severity);
}

function getDistinctHistoryRows() {
  const deduped = new Map();

  for (const row of state.history) {
    const key = getHistoryIncidentKey(row);
    const existing = deduped.get(key);

    if (shouldReplaceHistoryRow(existing, row)) {
      deduped.set(key, row);
    }
  }

  return [...deduped.values()];
}

async function fetchJson(url) {
  const res = await fetch(url, { cache: "no-store" });
  if (!res.ok) {
    throw new Error(`${url} -> ${res.status}`);
  }
  return res.json();
}

async function loadData() {
  const [currentResp, historyResp, summaryResp] = await Promise.all([
    fetchJson("/incidents/current"),
    fetchJson("/incidents/history"),
    fetchJson("/summary"),
  ]);

  const currentRows = safeArray(currentResp?.incidents ?? currentResp).map(normalizeIncident);
  const historyRows = safeArray(historyResp?.incidents ?? historyResp?.history ?? historyResp).map(
    normalizeIncident
  );

  state.currentIncidents = currentRows;
  state.history = historyRows;
  state.summary = summaryResp || null;

  const allSegments = Array.from(
    new Set([...currentRows.map((r) => r.segment_id), ...historyRows.map((r) => r.segment_id)])
  ).filter(Boolean);

  if (!state.selectedSegment || !allSegments.includes(state.selectedSegment)) {
    state.selectedSegment = allSegments[0] || null;
  }

  populateSegmentSelect(allSegments);
  renderAll();
}

function populateSegmentSelect(segments) {
  const select = byId("segmentSelect");
  const prev = state.selectedSegment;
  select.innerHTML = "";

  if (!segments.length) {
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No segments";
    select.appendChild(option);
    state.selectedSegment = null;
    return;
  }

  for (const segment of segments) {
    const option = document.createElement("option");
    option.value = segment;
    option.textContent = segment;
    if (segment === prev) option.selected = true;
    select.appendChild(option);
  }
}

function renderCards() {
  const summary = state.summary || {};
  const metrics = summary.card_metrics || {};
  const activeCount = num(metrics.active_incidents, 0);
  const avgSeverity = num(metrics.average_severity, 0);
  const topSegment = metrics.top_segment || "--";
  const topSegmentIncidentCount = num(metrics.top_segment_incident_count, 0);
  const recentOpened = num(metrics.opened_last_5_minutes, 0);

  byId("activeIncidents").textContent = String(activeCount);
  byId("avgSeverity").textContent = avgSeverity.toFixed(1);
  byId("topSegment").textContent = topSegment;
  byId("opened5m").textContent = String(recentOpened);

  byId("activeIncidentsSub").textContent =
    activeCount === 1 ? "1 open incident" : `${activeCount} open incidents`;
  byId("avgSeveritySub").textContent =
    activeCount ? "Across active incidents" : "No active incidents";
  byId("topSegmentSub").textContent =
    topSegmentIncidentCount
      ? `${topSegmentIncidentCount} distinct incidents in history`
      : "No history yet";
  byId("opened5mSub").textContent = "New incidents detected recently";
}

function buildIncidentsOverTimeSeries() {
  const bucketCounts = new Map();
  const distinctHistory = getDistinctHistoryRows();

  for (const row of distinctHistory) {
    const d = parseTime(row.opened_at);
    if (!d) continue;

    const bucketTs = new Date(
      d.getFullYear(),
      d.getMonth(),
      d.getDate(),
      d.getHours(),
      d.getMinutes(),
      0,
      0
    ).getTime();

    bucketCounts.set(bucketTs, (bucketCounts.get(bucketTs) || 0) + 1);
  }

  const sortedBuckets = [...bucketCounts.entries()].sort((a, b) => a[0] - b[0]);
  return {
    labels: sortedBuckets.map(([bucketTs]) =>
      new Date(bucketTs).toLocaleString([], {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      })
    ),
    values: sortedBuckets.map(([, count]) => count),
  };
}

function renderIncidentsOverTimeChart() {
  const { labels, values } = buildIncidentsOverTimeSeries();
  incidentsOverTimeChart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 40, right: 20, top: 20, bottom: 45 },
    xAxis: {
      type: "category",
      data: labels,
      axisLabel: { color: "#a7b1cc", rotate: 35 },
      axisLine: { lineStyle: { color: "rgba(255,255,255,0.15)" } },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: "#a7b1cc" },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.08)" } },
    },
    series: [{ name: "Incidents", type: "line", smooth: true, data: values, areaStyle: {} }],
  });
}

function renderSeverityDistributionChart() {
  const bins = [
    { name: "0-2", min: 0, max: 2 },
    { name: "2-4", min: 2, max: 4 },
    { name: "4-6", min: 4, max: 6 },
    { name: "6-8", min: 6, max: 8 },
    { name: "8-10", min: 8, max: Infinity },
  ];

  const active = state.currentIncidents.filter((r) => r.status === "open");
  const counts = bins.map(
    (bin) => active.filter((r) => r.severity >= bin.min && r.severity < bin.max).length
  );

  severityDistributionChart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 40, right: 20, top: 20, bottom: 35 },
    xAxis: {
      type: "category",
      data: bins.map((b) => b.name),
      axisLabel: { color: "#a7b1cc" },
      axisLine: { lineStyle: { color: "rgba(255,255,255,0.15)" } },
    },
    yAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: "#a7b1cc" },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.08)" } },
    },
    series: [{ name: "Count", type: "bar", data: counts }],
  });
}

function renderTopSegmentsChart() {
  const counts = new Map();
  const distinctHistory = getDistinctHistoryRows();

  for (const row of distinctHistory) {
    counts.set(row.segment_id, (counts.get(row.segment_id) || 0) + 1);
  }

  const top = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8);

  topSegmentsChart.setOption({
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    grid: { left: 90, right: 20, top: 20, bottom: 20 },
    xAxis: {
      type: "value",
      minInterval: 1,
      axisLabel: { color: "#a7b1cc" },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.08)" } },
    },
    yAxis: {
      type: "category",
      data: top.map(([segment]) => segment).reverse(),
      axisLabel: { color: "#a7b1cc" },
      axisLine: { lineStyle: { color: "rgba(255,255,255,0.15)" } },
    },
    series: [{ name: "Incidents", type: "bar", data: top.map(([, count]) => count).reverse() }],
  });
}

function renderSpeedComparisonChart() {
  const segment = state.selectedSegment;
  const matches = state.currentIncidents.filter((r) => r.segment_id === segment);
  byId("selectedSegmentLabel").textContent = segment ? `Segment ${segment}` : "Selected segment";

  if (!matches.length) {
    speedComparisonChart.setOption({
      title: {
        text: "No current incident data for selected segment",
        left: "center",
        top: "middle",
        textStyle: { color: "#a7b1cc", fontSize: 14, fontWeight: "normal" },
      },
      xAxis: { show: false },
      yAxis: { show: false },
      series: [],
    });
    return;
  }

  const row = matches[0];
  speedComparisonChart.setOption({
    tooltip: { trigger: "axis" },
    grid: { left: 40, right: 20, top: 20, bottom: 35 },
    xAxis: {
      type: "category",
      data: ["Current", "Baseline"],
      axisLabel: { color: "#a7b1cc" },
      axisLine: { lineStyle: { color: "rgba(255,255,255,0.15)" } },
    },
    yAxis: {
      type: "value",
      axisLabel: { color: "#a7b1cc", formatter: "{value} mph" },
      splitLine: { lineStyle: { color: "rgba(255,255,255,0.08)" } },
    },
    series: [
      {
        name: "Speed",
        type: "bar",
        data: [row.current_speed_mph, row.baseline_speed_mph],
      },
    ],
  });
}

function renderCurrentIncidentsTable() {
  const tbody = byId("currentIncidentsBody");
  const rows = state.currentIncidents.slice().sort((a, b) => b.severity - a.severity);
  byId("currentCountLabel").textContent = `${rows.length} rows`;

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty">No active incident rows available.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.segment_id)}</td>
          <td><span class="${severityBadgeClass(row.severity)}">${fmtNumber(row.severity, 1)}</span></td>
          <td><span class="${statusBadgeClass(row.status)}">${escapeHtml(row.status)}</span></td>
          <td>${fmtNumber(row.current_speed_mph, 1)} mph</td>
          <td>${fmtNumber(row.baseline_speed_mph, 1)} mph</td>
          <td>${escapeHtml(fmtTime(row.opened_at))}</td>
        </tr>
      `
    )
    .join("");
}

function renderHistoryTable() {
  const tbody = byId("historyBody");
  const distinctRows = getDistinctHistoryRows();
  const rows = distinctRows
    .slice()
    .sort((a, b) => getHistoryRowSortTs(b) - getHistoryRowSortTs(a))
    .slice(0, 50);

  byId("historyCountLabel").textContent = `${distinctRows.length} incidents`;

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="5" class="empty">No incident history available.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map(
      (row) => `
        <tr>
          <td>${escapeHtml(row.segment_id)}</td>
          <td><span class="${severityBadgeClass(row.severity)}">${fmtNumber(row.severity, 1)}</span></td>
          <td><span class="${statusBadgeClass(row.status)}">${escapeHtml(row.status)}</span></td>
          <td>${escapeHtml(fmtTime(row.opened_at))}</td>
          <td>${escapeHtml(fmtClosedTime(row))}</td>
        </tr>
      `
    )
    .join("");
}

function renderAll() {
  renderCards();
  renderIncidentsOverTimeChart();
  renderSeverityDistributionChart();
  renderTopSegmentsChart();
  renderSpeedComparisonChart();
  renderCurrentIncidentsTable();
  renderHistoryTable();
  byId("lastUpdated").textContent = `Last updated: ${new Date().toLocaleTimeString()}`;
}

function initCharts() {
  incidentsOverTimeChart = echarts.init(byId("incidentsOverTimeChart"));
  severityDistributionChart = echarts.init(byId("severityDistributionChart"));
  topSegmentsChart = echarts.init(byId("topSegmentsChart"));
  speedComparisonChart = echarts.init(byId("speedComparisonChart"));

  window.addEventListener("resize", () => {
    incidentsOverTimeChart.resize();
    severityDistributionChart.resize();
    topSegmentsChart.resize();
    speedComparisonChart.resize();
  });
}

async function refresh() {
  try {
    await loadData();
  } catch (err) {
    console.error(err);
    byId("lastUpdated").textContent = "Last updated: failed";
  }
}

byId("refreshBtn").addEventListener("click", refresh);
byId("segmentSelect").addEventListener("change", (e) => {
  state.selectedSegment = e.target.value || null;
  renderSpeedComparisonChart();
});

initCharts();
refresh();
setInterval(refresh, AUTO_REFRESH_MS);