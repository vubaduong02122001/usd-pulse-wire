const MARKET_REFRESH_INTERVAL_MS = 1000;
const AUX_REFRESH_INTERVAL_MS = 10 * 60 * 1000;
const CHART_REFRESH_INTERVAL_MS = 30 * 1000;
const FEED_RENDER_LIMIT = 36;
const CHART_TIMEFRAMES = ["1D", "5D", "1M", "3M", "1Y"];
const CHART_TYPES = ["candles", "line"];
const CHART_MAS = [20, 50];
const DEFAULT_CHART_ASSETS = ["DXY", "EURUSD", "US10Y", "BTCUSD"];
const HISTORY_LIMIT = 220;
const PAGE_PARAMS = new URLSearchParams(window.location.search);
const SNAPSHOT_MODE = PAGE_PARAMS.has("snapshot");
const BOOT_ASSET = PAGE_PARAMS.get("asset");

const dom = {
  connectionText: document.getElementById("connectionText"),
  connectionBadge: document.getElementById("connectionBadge"),
  lastSync: document.getElementById("lastSync"),
  tickerTrack: document.getElementById("tickerTrack"),
  searchInput: document.getElementById("searchInput"),
  refreshButton: document.getElementById("refreshButton"),
  marketGrid: document.getElementById("marketGrid"),
  marketGroups: document.getElementById("marketGroups"),
  marketMeta: document.getElementById("marketMeta"),
  marketStamp: document.getElementById("marketStamp"),
  headlineCount: document.getElementById("headlineCount"),
  highImpactCount: document.getElementById("highImpactCount"),
  sourceCount: document.getElementById("sourceCount"),
  lastBurst: document.getElementById("lastBurst"),
  feedSummary: document.getElementById("feedSummary"),
  summaryLines: document.getElementById("summaryLines"),
  signalTable: document.getElementById("signalTable"),
  feedList: document.getElementById("feedList"),
  hotList: document.getElementById("hotList"),
  sourceList: document.getElementById("sourceList"),
  calendarStamp: document.getElementById("calendarStamp"),
  calendarSchedule: document.getElementById("calendarSchedule"),
  indicatorList: document.getElementById("indicatorList"),
  speechList: document.getElementById("speechList"),
  chartGrid: document.getElementById("chartGrid"),
  toastStack: document.getElementById("toastStack"),
  impactFilters: Array.from(document.querySelectorAll("[data-impact]")),
};

const state = {
  items: [],
  status: null,
  market: null,
  calendar: null,
  speeches: null,
  search: "",
  impact: "all",
  connectionState: "connecting",
  stream: null,
  reconnectTimer: null,
  marketBusy: false,
  macroBusy: false,
  refreshBusy: false,
  activeChartSlot: 0,
  chartSlots: Array.from({ length: 4 }, (_, index) => ({
    index,
    asset: null,
    timeframe: "5D",
    type: "candles",
    ma20: true,
    ma50: false,
    loading: false,
    data: null,
    chart: null,
    series: null,
    overlays: [],
    updatedAt: null,
  })),
};

const SIGNAL_DIRECTION_RULES = {
  "Fed policy": { DXY: 1.4, EURUSD: -1.4, USDJPY: 1.2, GOLD: -0.9, US10Y: 1.3, SPX: -0.8, BTCUSD: -0.5, ETHUSD: -0.5 },
  Inflation: { DXY: 1.1, EURUSD: -1.0, USDJPY: 0.8, GOLD: -0.4, US10Y: 1.2, SPX: -0.9, BTCUSD: -0.3, ETHUSD: -0.3 },
  Labor: { DXY: 0.7, EURUSD: -0.7, USDJPY: 0.6, US10Y: 0.7, SPX: -0.2 },
  Growth: { DXY: 0.2, EURUSD: -0.2, WTI: 0.9, SPX: 0.9, BTCUSD: 0.6, ETHUSD: 0.6, GOLD: -0.2 },
  "Treasury / fiscal": { DXY: 0.7, EURUSD: -0.6, USDJPY: 0.4, GOLD: 0.3, US10Y: 0.8, SPX: -0.4, WTI: -0.1 },
  "FX / rates": { DXY: 1.1, EURUSD: -1.1, USDJPY: 0.9, GOLD: -0.4, US10Y: 0.9 },
  "Risk sentiment": { DXY: 0.7, EURUSD: -0.3, USDJPY: -0.7, GOLD: 1.0, WTI: -0.5, SPX: -1.0, BTCUSD: -1.0, ETHUSD: -1.0 },
};

const USD_POSITIVE_TOKENS = [
  "hot inflation",
  "sticky inflation",
  "higher for longer",
  "tariff",
  "sanction",
  "hawkish",
  "strong",
  "resilient",
  "surge",
  "rise",
  "rises",
  "rising",
  "accelerate",
  "upside",
];

const USD_NEGATIVE_TOKENS = [
  "cooling",
  "cools",
  "soft",
  "weak",
  "cuts",
  "cut",
  "dovish",
  "lower",
  "slower",
  "miss",
  "drop",
  "drops",
  "decline",
  "declines",
  "de-escalation",
  "deescalation",
];

const RISK_OFF_TOKENS = [
  "war",
  "attack",
  "iran",
  "hormuz",
  "uncertainty",
  "sanction",
  "tariff",
  "conflict",
  "volatility",
  "risk off",
];

const RISK_ON_TOKENS = [
  "truce",
  "deal",
  "easing",
  "ceasefire",
  "de-escalation",
  "cooling inflation",
  "soft landing",
];

const GROWTH_POSITIVE_TOKENS = [
  "beat",
  "beats",
  "strong",
  "growth",
  "expansion",
  "rebound",
  "surprise increase",
];

const GROWTH_NEGATIVE_TOKENS = [
  "contraction",
  "slowdown",
  "slump",
  "recession",
  "fall",
  "falls",
  "miss",
  "weaker",
];

function escapeHtml(value = "") {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function formatAgo(value) {
  if (!value) return "--";
  const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function formatDateTime(value) {
  if (!value) return "--";
  return new Intl.DateTimeFormat("en-US", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

function formatDateLine(value) {
  if (!value) return `<span>--/--</span><span>--:--</span>`;
  const date = new Date(value);
  const top = new Intl.DateTimeFormat("en-US", { month: "2-digit", day: "2-digit" }).format(date);
  const bottom = new Intl.DateTimeFormat("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
  return `<span>${top}</span><span>${bottom}</span>`;
}

function formatCompactNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return Number(value).toLocaleString("en-US", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function quoteDigits(quote) {
  if (!quote) return 2;
  if (quote.group === "FX") return quote.label === "USDJPY" ? 3 : 4;
  if (quote.group === "Rates" || quote.group === "Dollar") return 3;
  return 2;
}

function pctString(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${Number(value).toFixed(digits)}%`;
}

function absString(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  const sign = value > 0 ? "+" : "";
  return `${sign}${Number(value).toFixed(digits)}`;
}

function impactRank(level) {
  return { high: 3, medium: 2, watch: 1 }[level] ?? 0;
}

function getFilteredItems() {
  const needle = state.search.trim().toLowerCase();
  return state.items
    .filter((item) => {
      if (state.impact !== "all" && item.impact_level !== state.impact) return false;
      if (!needle) return true;
      const haystack = [
        item.title,
        item.summary,
        item.source,
        ...(item.categories || []),
        ...(item.matched_signals || []),
        ...(item.matched_terms || []),
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(needle);
    })
    .sort((left, right) => {
      const impactDelta = impactRank(right.impact_level) - impactRank(left.impact_level);
      if (impactDelta !== 0) return impactDelta;
      return new Date(right.published_at).getTime() - new Date(left.published_at).getTime();
    });
}

function getMarketQuote(asset) {
  return state.market?.quotes?.find((quote) => quote.label === asset) ?? null;
}

async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    cache: "no-store",
    headers: { Accept: "application/json" },
    ...options,
  });
  if (!response.ok) {
    throw new Error(`${response.status} ${response.statusText}`);
  }
  return response.json();
}

function pushToast(title, body) {
  if (!dom.toastStack) return;
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.innerHTML = `<strong>${escapeHtml(title)}</strong><span>${escapeHtml(body)}</span>`;
  dom.toastStack.prepend(toast);
  window.setTimeout(() => toast.remove(), 5000);
}

function clampSummary(text, limit = 180) {
  if (!text) return "";
  if (text.length <= limit) return text;
  return `${text.slice(0, limit - 1).trim()}...`;
}

function upsertNewsItem(item) {
  const existingIndex = state.items.findIndex((entry) => entry.id === item.id);
  if (existingIndex >= 0) {
    state.items[existingIndex] = item;
  } else {
    state.items.unshift(item);
    state.items.sort(
      (left, right) => new Date(right.published_at).getTime() - new Date(left.published_at).getTime(),
    );
    state.items = state.items.slice(0, HISTORY_LIMIT);
  }
}

function textBlob(item) {
  return `${item.title || ""} ${item.summary || ""} ${(item.matched_terms || []).join(" ")} ${(item.matched_signals || []).join(" ")}`.toLowerCase();
}

function scoreFromTokens(text, tokens, amount) {
  return tokens.reduce((total, token) => total + (text.includes(token) ? amount : 0), 0);
}

function inferBiasMode(item, signals = item.matched_signals || []) {
  const text = textBlob(item);
  const risk = scoreFromTokens(text, RISK_OFF_TOKENS, -1) + scoreFromTokens(text, RISK_ON_TOKENS, 1);
  const usd = scoreFromTokens(text, USD_POSITIVE_TOKENS, 1) + scoreFromTokens(text, USD_NEGATIVE_TOKENS, -1);
  const growth = scoreFromTokens(text, GROWTH_POSITIVE_TOKENS, 1) + scoreFromTokens(text, GROWTH_NEGATIVE_TOKENS, -1);
  if (risk <= -1) return "RISK OFF";
  if (risk >= 1) return "RISK ON";
  if (signals.includes("Fed policy") || signals.includes("FX / rates") || signals.includes("Inflation")) {
    if (usd >= 0) return "USD+ / YIELDS+";
    return "USD- / YIELDS-";
  }
  if (growth >= 1) return "GROWTH+";
  if (growth <= -1) return "GROWTH-";
  if (signals.includes("Risk sentiment")) return "CROSS-ASSET";
  return "USD WATCH";
}

function calculateAssetBias(item) {
  const scores = {
    DXY: 0,
    EURUSD: 0,
    USDJPY: 0,
    GOLD: 0,
    WTI: 0,
    US10Y: 0,
    SPX: 0,
    BTCUSD: 0,
    ETHUSD: 0,
  };

  for (const signal of item.matched_signals || []) {
    const rule = SIGNAL_DIRECTION_RULES[signal];
    if (!rule) continue;
    Object.entries(rule).forEach(([asset, value]) => {
      scores[asset] += value;
    });
  }

  const text = textBlob(item);
  const usdShift = scoreFromTokens(text, USD_POSITIVE_TOKENS, 0.45) + scoreFromTokens(text, USD_NEGATIVE_TOKENS, -0.45);
  const riskShift = scoreFromTokens(text, RISK_OFF_TOKENS, -0.55) + scoreFromTokens(text, RISK_ON_TOKENS, 0.55);
  const growthShift = scoreFromTokens(text, GROWTH_POSITIVE_TOKENS, 0.4) + scoreFromTokens(text, GROWTH_NEGATIVE_TOKENS, -0.4);
  const oilShift = text.includes("oil") || text.includes("energy") || text.includes("crude") ? 0.6 : 0;

  scores.DXY += usdShift - riskShift * 0.12;
  scores.EURUSD -= usdShift;
  scores.USDJPY += usdShift + riskShift * 0.65;
  scores.GOLD += riskShift * -0.95 - usdShift * 0.5;
  scores.WTI += growthShift * 0.75 + oilShift - riskShift * 0.55;
  scores.US10Y += usdShift * 0.65 - riskShift * 0.1;
  scores.SPX += growthShift * 0.8 + riskShift;
  scores.BTCUSD += growthShift * 0.5 + riskShift * 1.05 - usdShift * 0.25;
  scores.ETHUSD += growthShift * 0.55 + riskShift * 1.1 - usdShift * 0.25;

  return Object.entries(scores)
    .map(([asset, value]) => {
      let direction = "flat";
      if (value >= 0.55) direction = "up";
      if (value <= -0.55) direction = "down";
      return { asset, value, direction };
    })
    .filter((entry) => entry.direction !== "flat")
    .sort((left, right) => Math.abs(right.value) - Math.abs(left.value));
}

function reactionToneLabel(item) {
  return inferBiasMode(item);
}

function renderReactionStrip(item, includeActual = false, maxItems = 4) {
  const reactions = calculateAssetBias(item).slice(0, maxItems);
  if (!reactions.length) {
    return `<span class="reaction-tone">NO CLEAR BIAS</span>`;
  }

  const pills = reactions
    .map((reaction) => {
      const quote = includeActual ? getMarketQuote(reaction.asset) : null;
      const actual = quote ? pctString(quote.percent_change, 2) : "calc";
      const arrow = reaction.direction === "up" ? "UP" : "DN";
      return `
        <button class="reaction-pill ${reaction.direction}" type="button" data-open-asset="${reaction.asset}">
          <span>${reaction.asset}</span>
          <strong>${arrow}</strong>
          <small>${escapeHtml(actual)}</small>
        </button>
      `;
    })
    .join("");

  return `
    <span class="reaction-tone">${escapeHtml(reactionToneLabel(item))}</span>
    <div class="reaction-strip">${pills}</div>
  `;
}

function historyGroups(points = []) {
  const grouped = new Map();
  points.forEach((point) => {
    const year = new Date(point.at).getUTCFullYear();
    if (!grouped.has(year)) grouped.set(year, []);
    grouped.get(year).push(point);
  });
  return Array.from(grouped.entries()).sort((left, right) => right[0] - left[0]);
}

function renderTickerTape() {
  const marketItems = (state.market?.quotes || []).slice(0, 10).map((quote) => {
    const klass = quote.direction || "flat";
    return `
      <span class="ticker-item ${klass}">
        <strong>${quote.label}</strong>
        <span>${formatCompactNumber(quote.last, quoteDigits(quote))}</span>
        <span class="quote-change ${klass}">${pctString(quote.percent_change, 2)}</span>
      </span>
    `;
  });

  const headlineItems = getFilteredItems()
    .slice(0, 6)
    .map((item) => `<span class="ticker-item"><strong>${item.impact_level.toUpperCase()}</strong><span>${escapeHtml(clampSummary(item.title, 74))}</span></span>`);

  const content = [...marketItems, ...headlineItems].join("");
  dom.tickerTrack.innerHTML = content || `<span class="ticker-placeholder">Waiting for terminal tape...</span>`;
}

function renderConnectionState() {
  const label =
    state.connectionState === "live"
      ? "LIVE"
      : state.connectionState === "reconnecting"
        ? "RECONNECT"
        : "CONNECTING";
  dom.connectionText.textContent =
    state.connectionState === "live"
      ? "LIVE FEED LOCKED"
      : state.connectionState === "reconnecting"
        ? "REALTIME FEED RECONNECTING"
        : "CONNECTING REALTIME FEED";
  dom.connectionBadge.textContent = label;
  dom.connectionBadge.className = `connection-chip ${state.connectionState}`;
  dom.lastSync.textContent = formatAgo(state.status?.last_sync_at || state.market?.updated_at);
}

function renderMarketGrid() {
  const quotes = state.market?.quotes || [];
  if (!quotes.length) {
    const sourceNote = state.market?.source || "Waiting for price tape.";
    dom.marketGrid.innerHTML = `<div class="empty-state">${escapeHtml(sourceNote)}</div>`;
    return;
  }

  dom.marketGrid.innerHTML = quotes
    .map((quote) => {
      const changeClass = quote.direction || "flat";
      return `
        <button class="quote-card" type="button" data-open-asset="${quote.label}">
          <div class="quote-top">
            <div>
              <div class="quote-label">${quote.label}</div>
              <div class="quote-group">${escapeHtml(quote.group)} / ${escapeHtml(quote.currency)}</div>
            </div>
            <div class="quote-change ${changeClass}">${pctString(quote.percent_change, 2)}</div>
          </div>
          <div class="quote-price">${formatCompactNumber(quote.last, quoteDigits(quote))}</div>
          <div class="quote-bottom">
            <span class="quote-range">${absString(quote.absolute_change, quoteDigits(quote))}</span>
            <span class="quote-meta">${formatAgo(quote.updated_at)}</span>
          </div>
        </button>
      `;
    })
    .join("");
}

function renderMarketGroups() {
  const groups = new Map();
  for (const quote of state.market?.quotes || []) {
    const current = groups.get(quote.group);
    if (!current || Math.abs(quote.percent_change || 0) > Math.abs(current.percent_change || 0)) {
      groups.set(quote.group, quote);
    }
  }

  const rows = Array.from(groups.entries())
    .sort((left, right) => left[0].localeCompare(right[0]))
    .map(
      ([group, quote]) => `
        <div class="signal-row market">
          <span class="signal-tag">${escapeHtml(group)}</span>
          <button class="slot-button" type="button" data-open-asset="${quote.label}">${quote.label}</button>
          <span class="quote-change ${quote.direction || "flat"}">${pctString(quote.percent_change, 2)}</span>
        </div>
      `,
    )
    .join("");

  dom.marketGroups.innerHTML = rows || `<div class="empty-state">No market groups yet.</div>`;
}

function renderMarketMeta() {
  dom.marketStamp.textContent = state.market?.updated_at ? formatAgo(state.market.updated_at) : "LOADING";
  const lines = [
    ["Source", state.market?.source || "--"],
    ["Quotes", String(state.market?.quotes?.length || 0)],
    ["Cache", "1s UI / provider paced"],
    ["Last quote", state.market?.updated_at ? formatDateTime(state.market.updated_at) : "--"],
  ];

  dom.marketMeta.innerHTML = lines
    .map(
      ([label, value]) => `
        <div class="summary-line">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `,
    )
    .join("");
}

function renderStats() {
  const filtered = getFilteredItems();
  const sources = state.status?.sources || [];
  dom.headlineCount.textContent = String(filtered.length);
  dom.highImpactCount.textContent = String(filtered.filter((item) => item.impact_level === "high").length);
  dom.sourceCount.textContent = String(state.status?.connected_sources ?? sources.filter((source) => !source.last_error).length);
  dom.lastBurst.textContent = formatAgo(state.status?.last_new_item_at || filtered[0]?.published_at);
  dom.feedSummary.textContent = `${filtered.length} / ${state.items.length} ON SCREEN`;

  const lines = [
    ["Impact filter", state.impact.toUpperCase()],
    ["Search term", state.search ? state.search : "NONE"],
    ["Subscribers", String(state.status?.subscriber_count ?? 0)],
    ["Refresh burst", String(state.status?.last_refresh_count ?? 0)],
  ];

  dom.summaryLines.innerHTML = lines
    .map(
      ([label, value]) => `
        <div class="summary-line">
          <span>${escapeHtml(label)}</span>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `,
    )
    .join("");
}

function renderSignalTable() {
  const counts = new Map();
  for (const item of getFilteredItems()) {
    for (const signal of item.matched_signals || []) {
      counts.set(signal, (counts.get(signal) || 0) + 1);
    }
  }

  const rows = Array.from(counts.entries())
    .sort((left, right) => right[1] - left[1])
    .slice(0, 8)
    .map(
      ([signal, count]) => `
        <div class="signal-row">
          <span>${escapeHtml(signal)}</span>
          <span class="signal-count">${count}</span>
        </div>
      `,
    )
    .join("");

  dom.signalTable.innerHTML = rows || `<div class="empty-state">No active themes yet.</div>`;
}

function renderHotList() {
  const rows = getFilteredItems()
    .slice(0, 6)
    .map(
      (item) => `
        <div class="hot-row">
          <span>${escapeHtml(clampSummary(item.title, 58))}</span>
          <span class="hot-score">${item.impact_score}</span>
        </div>
      `,
    )
    .join("");
  dom.hotList.innerHTML = rows || `<div class="empty-state">No catalysts yet.</div>`;
}

function renderSources() {
  const sources = state.status?.sources || [];
  const rows = sources
    .map((source) => {
      const statusClass = source.last_error ? "error" : source.last_success_at ? "live" : "pending";
      const statusLabel = source.last_error ? "ERR" : source.last_success_at ? "LIVE" : "WAIT";
      return `
        <div class="source-item">
          <div class="source-top">
            <span class="source-name">${escapeHtml(source.name)}</span>
            <span class="status-chip ${statusClass}">${statusLabel}</span>
          </div>
          <div class="source-meta">${escapeHtml(source.kind)} / ${source.last_item_count} rows / ${formatAgo(source.last_success_at)}</div>
        </div>
      `;
    })
    .join("");
  dom.sourceList.innerHTML = rows || `<div class="empty-state">No source health yet.</div>`;
}

function renderFeed() {
  const items = getFilteredItems().slice(0, FEED_RENDER_LIMIT);
  if (!items.length) {
    dom.feedList.innerHTML = `<div class="empty-state">No rows match the current filter.</div>`;
    return;
  }

  dom.feedList.innerHTML = items
    .map((item) => {
      const terms = (item.matched_terms || []).slice(0, 3);
      const signals = (item.matched_signals || []).slice(0, 3);
      return `
        <article class="news-row ${item.impact_level}">
          <div class="row-time">${formatDateLine(item.published_at)}</div>
          <div class="row-impact ${item.impact_level}">${item.impact_level.toUpperCase()}</div>
          <div class="row-source">
            <div>${escapeHtml(item.source)}</div>
            <div class="row-meta">score ${item.impact_score} | trust ${Math.round((item.trust_score || 0) * 100)}%</div>
          </div>
          <div class="row-main">
            <a class="headline-link" href="${item.url}" target="_blank" rel="noreferrer">${escapeHtml(item.title)}</a>
            <div class="row-summary">${escapeHtml(clampSummary(item.summary, 170))}</div>
            <div class="row-signals">
              ${signals.map((signal) => `<span class="chip signal">${escapeHtml(signal)}</span>`).join("")}
              ${terms.map((term) => `<span class="chip term">${escapeHtml(term)}</span>`).join("")}
            </div>
            <div class="row-meta">${formatAgo(item.published_at)} | ${escapeHtml((item.categories || []).slice(0, 2).join(" / "))}</div>
          </div>
          <div class="row-reaction">
            ${renderReactionStrip(item, true, 3)}
          </div>
        </article>
      `;
    })
    .join("");
}

function renderHistoryRows(points = []) {
  const groups = historyGroups(points);
  return groups
    .map(
      ([year, items], index) => `
        <details class="history-group" ${index === 0 ? "open" : ""}>
          <summary class="history-year">${year}<span>${items.length} pts</span></summary>
          <div class="history-table">
            ${items
              .map(
                (point) => `
                  <div class="history-row">
                    <span>${escapeHtml(point.label || formatDateTime(point.at))}</span>
                    <strong>${escapeHtml(point.display_value || "--")}</strong>
                    <span>${formatDateTime(point.at)}</span>
                  </div>
                `,
              )
              .join("")}
          </div>
        </details>
      `,
    )
    .join("");
}

function renderCalendarSchedule() {
  const items = state.calendar?.schedule || [];
  dom.calendarSchedule.innerHTML =
    items
      .map(
        (item, index) => `
          <details class="calendar-card" ${index < 2 ? "open" : ""}>
            <summary>
              <div class="calendar-summary">
                <div class="calendar-topline">
                  <div class="calendar-title">
                    <strong>${escapeHtml(item.title)}</strong>
                    <div class="calendar-meta">${escapeHtml(item.category)} / ${escapeHtml(item.frequency)}</div>
                  </div>
                  <span class="calendar-badge ${item.importance}">${item.importance.toUpperCase()}</span>
                </div>
                <div class="chip-strip">
                  ${(item.signals || []).map((signal) => `<span class="chip signal">${escapeHtml(signal)}</span>`).join("")}
                </div>
              </div>
            </summary>
            <div class="calendar-body">
              <div class="calendar-grid">
                <div class="mini-stat">
                  <div class="calendar-meta">NEXT</div>
                  <div class="calendar-value">${item.scheduled_at ? formatDateTime(item.scheduled_at) : "--"}</div>
                </div>
                <div class="mini-stat">
                  <div class="calendar-meta">LAST</div>
                  <div class="calendar-value">${item.last_release_at ? formatDateTime(item.last_release_at) : "--"}</div>
                </div>
              </div>
              <div class="terminal-note">${escapeHtml(item.summary || "")}</div>
              ${renderReactionStrip({ title: item.title, summary: item.summary, matched_signals: item.signals || [], matched_terms: [] }, false, 4)}
              ${renderHistoryRows(item.history || [])}
              <a class="source-link" href="${item.source_url}" target="_blank" rel="noreferrer">${escapeHtml(item.source)}</a>
            </div>
          </details>
        `,
      )
      .join("") || `<div class="empty-state">No USD schedule available.</div>`;
}

function renderIndicators() {
  const items = state.calendar?.indicators || [];
  dom.indicatorList.innerHTML =
    items
      .map(
        (item, index) => `
          <details class="indicator-card" ${index < 2 ? "open" : ""}>
            <summary>
              <div class="indicator-summary">
                <div class="indicator-topline">
                  <div class="indicator-title">
                    <strong>${escapeHtml(item.title)}</strong>
                    <div class="indicator-meta">${escapeHtml(item.category)} / ${escapeHtml(item.frequency)}</div>
                  </div>
                  <div class="indicator-value">${escapeHtml(item.current_display)}</div>
                </div>
                <div class="chip-strip">
                  ${(item.signals || []).map((signal) => `<span class="chip signal">${escapeHtml(signal)}</span>`).join("")}
                </div>
              </div>
            </summary>
            <div class="indicator-body">
              <div class="indicator-grid">
                <div class="mini-stat">
                  <div class="indicator-meta">CURRENT</div>
                  <div class="indicator-value">${escapeHtml(item.current_display)}</div>
                </div>
                <div class="mini-stat">
                  <div class="indicator-meta">PREVIOUS</div>
                  <div class="indicator-value">${escapeHtml(item.previous_display || "--")}</div>
                </div>
              </div>
              <div class="terminal-note">${escapeHtml(item.note || "")}</div>
              ${renderReactionStrip({ title: item.title, summary: item.note || "", matched_signals: item.signals || [], matched_terms: [] }, false, 4)}
              ${renderHistoryRows(item.history || [])}
              <a class="source-link" href="${item.source_url}" target="_blank" rel="noreferrer">${escapeHtml(item.source)}</a>
            </div>
          </details>
        `,
      )
      .join("") || `<div class="empty-state">Indicator history unavailable.</div>`;
}

function renderCalendar() {
  dom.calendarStamp.textContent = state.calendar?.updated_at ? formatAgo(state.calendar.updated_at) : "LOADING";
  renderCalendarSchedule();
  renderIndicators();
}

function renderSpeeches() {
  const items = state.speeches?.items || [];
  dom.speechList.innerHTML =
    items
      .map(
        (item, index) => `
          <details class="speech-card" ${index < 2 ? "open" : ""}>
            <summary>
              <div class="speech-summary-wrap">
                <div class="speech-topline">
                  <div class="speech-title">
                    <strong>${escapeHtml(item.title)}</strong>
                    <div class="speech-meta">${formatAgo(item.published_at)} / ${escapeHtml(item.source)}</div>
                  </div>
                  <div class="speech-score">${item.impact_score}</div>
                </div>
                <div class="chip-strip">
                  <span class="speech-kind">${escapeHtml(item.item_kind.toUpperCase())}</span>
                  <span class="calendar-badge ${item.impact_level}">${item.impact_level.toUpperCase()}</span>
                  ${(item.matched_signals || []).map((signal) => `<span class="chip signal">${escapeHtml(signal)}</span>`).join("")}
                </div>
              </div>
            </summary>
            <div class="speech-body">
              <div class="speech-summary">${escapeHtml(item.summary || "")}</div>
              ${renderReactionStrip(item, false, 4)}
              <a class="source-link" href="${item.url}" target="_blank" rel="noreferrer">Open official item</a>
            </div>
          </details>
        `,
      )
      .join("") || `<div class="empty-state">No official Trump tape items right now.</div>`;
}

function renderAllViews() {
  renderConnectionState();
  renderTickerTape();
  renderMarketGrid();
  renderMarketGroups();
  renderMarketMeta();
  renderStats();
  renderSignalTable();
  renderHotList();
  renderSources();
  renderFeed();
}

// chart functions
function clearChartInstance(slot) {
  if (slot.chart) {
    slot.chart.remove();
    slot.chart = null;
  }
  slot.series = null;
  slot.overlays = [];
}

function movingAverage(points, length) {
  const output = [];
  let sum = 0;
  for (let index = 0; index < points.length; index += 1) {
    sum += points[index].close;
    if (index >= length) {
      sum -= points[index - length].close;
    }
    if (index >= length - 1) {
      output.push({
        time: points[index].time,
        value: Number((sum / length).toFixed(4)),
      });
    }
  }
  return output;
}

function renderChartGrid() {
  dom.chartGrid.innerHTML = state.chartSlots
    .map((slot, index) => {
      const isActive = index === state.activeChartSlot;
      const hasAsset = Boolean(slot.asset);
      const hasBars = Boolean(slot.data?.bars?.length);
      return `
        <section class="chart-slot ${isActive ? "active" : ""}">
          <div class="chart-slot-head">
            <div class="chart-slot-meta">
              <span class="slot-index">SLOT ${index + 1}</span>
              <strong>${escapeHtml(slot.asset || "EMPTY WORKSPACE")}</strong>
            </div>
            <div class="slot-actions">
              <button class="slot-button ${isActive ? "active" : ""}" type="button" data-set-slot="${index}">${isActive ? "ACTIVE" : "USE"}</button>
              ${hasAsset ? `<button class="slot-close" type="button" data-close-slot="${index}">CLOSE</button>` : ""}
            </div>
          </div>
          <div class="slot-toolbar">
            <div class="slot-group">
              ${CHART_TIMEFRAMES
                .map(
                  (timeframe) => `
                    <button class="slot-button ${slot.timeframe === timeframe ? "active" : ""}" type="button" data-chart-timeframe="${timeframe}" data-slot-index="${index}">
                      ${timeframe}
                    </button>
                  `,
                )
                .join("")}
            </div>
            <div class="slot-group">
              ${CHART_TYPES
                .map(
                  (type) => `
                    <button class="slot-button ${slot.type === type ? "active" : ""}" type="button" data-chart-type="${type}" data-slot-index="${index}">
                      ${type === "candles" ? "CANDLE" : "LINE"}
                    </button>
                  `,
                )
                .join("")}
            </div>
            <div class="slot-group">
              ${CHART_MAS
                .map(
                  (ma) => `
                    <label class="slot-toggle">
                      <input type="checkbox" data-ma-toggle="${ma}" data-slot-index="${index}" ${slot[`ma${ma}`] ? "checked" : ""} />
                      MA${ma}
                    </label>
                  `,
                )
                .join("")}
            </div>
            ${hasAsset ? `<button class="slot-button" type="button" data-refresh-slot="${index}">RELOAD</button>` : ""}
          </div>
          <div class="chart-surface">
            ${
              hasAsset && hasBars
                ? `<div class="chart-canvas" id="chartCanvas-${index}"></div>`
                : `<div class="chart-placeholder">${
                    hasAsset
                      ? "Chart feed temporarily unavailable.<br />Try reload later or change timeframe."
                      : "Click an asset from the tape to pin it here.<br />Up to 4 charts can stay open together."
                  }</div>`
            }
          </div>
          <div class="chart-status">
            ${
              slot.loading
                ? "Loading chart..."
                : hasAsset
                  ? `${slot.type.toUpperCase()} / ${slot.timeframe} / ${hasBars ? (slot.updatedAt ? formatAgo(slot.updatedAt) : "waiting") : "unavailable"}`
                  : "No asset loaded"
            }
          </div>
        </section>
      `;
    })
    .join("");

  window.requestAnimationFrame(rebuildCharts);
}

function rebuildCharts() {
  if (!window.LightweightCharts) return;

  for (const slot of state.chartSlots) {
    clearChartInstance(slot);
  }

  state.chartSlots.forEach((slot, index) => {
    const canvas = document.getElementById(`chartCanvas-${index}`);
    if (!canvas || !slot.data?.bars?.length) return;

    const chart = window.LightweightCharts.createChart(canvas, {
      autoSize: true,
      height: 262,
      layout: {
        background: { color: "#091018" },
        textColor: "#b8c9d9",
        fontFamily: "IBM Plex Mono, monospace",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.05)" },
        horzLines: { color: "rgba(255,255,255,0.05)" },
      },
      rightPriceScale: {
        borderColor: "#22364d",
      },
      timeScale: {
        borderColor: "#22364d",
        timeVisible: slot.timeframe !== "1Y",
      },
      crosshair: {
        vertLine: { color: "#ffb52f", labelBackgroundColor: "#ff9800" },
        horzLine: { color: "#03b9ff", labelBackgroundColor: "#03b9ff" },
      },
    });

    const bars = slot.data.bars.map((bar) => ({
      time: Math.floor(new Date(bar.time).getTime() / 1000),
      open: Number(bar.open),
      high: Number(bar.high),
      low: Number(bar.low),
      close: Number(bar.close),
    }));

    if (slot.type === "line") {
      slot.series = chart.addLineSeries({
        color: "#03b9ff",
        lineWidth: 2,
        lastValueVisible: true,
        priceLineVisible: true,
      });
      slot.series.setData(bars.map((bar) => ({ time: bar.time, value: bar.close })));
    } else {
      slot.series = chart.addCandlestickSeries({
        upColor: "#36d79a",
        downColor: "#ff6860",
        borderVisible: false,
        wickUpColor: "#36d79a",
        wickDownColor: "#ff6860",
      });
      slot.series.setData(bars);
    }

    if (slot.ma20) {
      const ma20 = chart.addLineSeries({ color: "#ffb52f", lineWidth: 2, priceLineVisible: false });
      ma20.setData(movingAverage(bars, 20));
      slot.overlays.push(ma20);
    }
    if (slot.ma50) {
      const ma50 = chart.addLineSeries({ color: "#9a73ff", lineWidth: 2, priceLineVisible: false });
      ma50.setData(movingAverage(bars, 50));
      slot.overlays.push(ma50);
    }

    chart.timeScale().fitContent();
    slot.chart = chart;
  });
}

function nextChartSlot() {
  const emptyIndex = state.chartSlots.findIndex((slot) => !slot.asset);
  return emptyIndex >= 0 ? emptyIndex : state.activeChartSlot;
}

async function loadChartSlot(index, force = false) {
  const slot = state.chartSlots[index];
  if (!slot.asset) return;

  slot.loading = true;
  renderChartGrid();
  try {
    const payload = await fetchJSON(
      `/api/asset-chart?asset=${encodeURIComponent(slot.asset)}&timeframe=${encodeURIComponent(slot.timeframe)}&force=${force ? "true" : "false"}`,
    );
    slot.data = payload;
    slot.updatedAt = payload.updated_at;
  } catch (error) {
    pushToast("Chart Load Failed", `${slot.asset}: ${error.message}`);
  } finally {
    slot.loading = false;
    renderChartGrid();
  }
}

function openChartForAsset(asset) {
  const index = nextChartSlot();
  const slot = state.chartSlots[index];
  slot.asset = asset;
  slot.data = null;
  slot.updatedAt = null;
  slot.type = slot.type || "candles";
  slot.timeframe = slot.timeframe || "5D";
  state.activeChartSlot = index;
  renderChartGrid();
  return loadChartSlot(index, true);
}

function closeChartSlot(index) {
  const slot = state.chartSlots[index];
  clearChartInstance(slot);
  state.chartSlots[index] = {
    index,
    asset: null,
    timeframe: "5D",
    type: "candles",
    ma20: true,
    ma50: false,
    loading: false,
    data: null,
    chart: null,
    series: null,
    overlays: [],
    updatedAt: null,
  };
  renderChartGrid();
}

async function refreshOpenCharts() {
  const openSlots = state.chartSlots
    .map((slot, index) => ({ slot, index }))
    .filter(({ slot }) => slot.asset && !slot.loading);
  await Promise.all(openSlots.map(({ index }) => loadChartSlot(index, false)));
}

async function preloadDefaultCharts() {
  for (const asset of DEFAULT_CHART_ASSETS) {
    await openChartForAsset(asset);
  }
}

// loading and events
async function loadInitial() {
  const payload = await fetchJSON("/api/news?limit=160");
  state.items = payload.items || [];
  state.status = payload.status || null;
  state.market = payload.market || null;
  renderAllViews();
}

async function refreshMarket(force = true) {
  if (state.marketBusy) return;
  state.marketBusy = true;
  try {
    state.market = await fetchJSON(`/api/market-snapshot?force=${force ? "true" : "false"}`);
    renderAllViews();
  } catch (error) {
    console.error(error);
  } finally {
    state.marketBusy = false;
  }
}

async function refreshMacroPanels(force = false) {
  if (state.macroBusy) return;
  state.macroBusy = true;
  try {
    const [calendar, speeches] = await Promise.all([
      fetchJSON(`/api/calendar?force=${force ? "true" : "false"}`),
      fetchJSON(`/api/trump-tape?force=${force ? "true" : "false"}`),
    ]);
    state.calendar = calendar;
    state.speeches = speeches;
    renderCalendar();
    renderSpeeches();
  } catch (error) {
    console.error(error);
    pushToast("Macro Panel Error", error.message);
  } finally {
    state.macroBusy = false;
  }
}

async function triggerManualRefresh() {
  if (state.refreshBusy) return;
  state.refreshBusy = true;
  dom.refreshButton.disabled = true;
  try {
    const payload = await fetchJSON("/api/refresh", { method: "POST" });
    (payload.inserted || []).forEach((item) => upsertNewsItem(item));
    state.status = payload.status || state.status;
    await Promise.all([refreshMarket(true), refreshMacroPanels(true)]);
    renderAllViews();
    pushToast("Force Refresh", `${payload.inserted?.length || 0} new rows inserted.`);
  } catch (error) {
    pushToast("Refresh Failed", error.message);
  } finally {
    state.refreshBusy = false;
    dom.refreshButton.disabled = false;
  }
}

function handleIncomingNews(item) {
  upsertNewsItem(item);
  state.status = {
    ...(state.status || {}),
    last_new_item_at: item.published_at,
    last_refresh_count: (state.status?.last_refresh_count || 0) + 1,
    tracked_items: state.items.length,
  };
  renderAllViews();
  if (item.impact_level === "high") {
    pushToast("High Impact", clampSummary(item.title, 90));
  }
}

function connectStream() {
  if (state.stream) {
    state.stream.close();
    state.stream = null;
  }
  if (state.reconnectTimer) {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }

  state.connectionState = "connecting";
  renderConnectionState();

  const stream = new EventSource("/stream");
  state.stream = stream;

  stream.addEventListener("hello", () => {
    state.connectionState = "live";
    renderConnectionState();
  });

  stream.addEventListener("news", (event) => {
    try {
      handleIncomingNews(JSON.parse(event.data));
      state.connectionState = "live";
      renderConnectionState();
    } catch (error) {
      console.error(error);
    }
  });

  stream.addEventListener("ping", (event) => {
    try {
      state.status = JSON.parse(event.data);
      renderAllViews();
    } catch (error) {
      console.error(error);
    }
  });

  stream.onerror = () => {
    state.connectionState = "reconnecting";
    renderConnectionState();
    stream.close();
    state.stream = null;
    if (!state.reconnectTimer) {
      state.reconnectTimer = window.setTimeout(() => {
        state.reconnectTimer = null;
        connectStream();
      }, 3000);
    }
  };
}

function wireEvents() {
  dom.searchInput?.addEventListener("input", (event) => {
    state.search = event.target.value.trim();
    renderAllViews();
  });

  dom.refreshButton?.addEventListener("click", () => {
    triggerManualRefresh();
  });

  dom.impactFilters.forEach((button) => {
    button.addEventListener("click", () => {
      state.impact = button.dataset.impact || "all";
      dom.impactFilters.forEach((node) => node.classList.toggle("active", node === button));
      renderAllViews();
    });
  });

  document.addEventListener("click", (event) => {
    const openAsset = event.target.closest("[data-open-asset]");
    if (openAsset) {
      event.preventDefault();
      event.stopPropagation();
      openChartForAsset(openAsset.dataset.openAsset);
      return;
    }

    const setSlot = event.target.closest("[data-set-slot]");
    if (setSlot) {
      state.activeChartSlot = Number(setSlot.dataset.setSlot);
      renderChartGrid();
      return;
    }

    const timeframe = event.target.closest("[data-chart-timeframe]");
    if (timeframe) {
      const index = Number(timeframe.dataset.slotIndex);
      state.chartSlots[index].timeframe = timeframe.dataset.chartTimeframe;
      loadChartSlot(index, true);
      return;
    }

    const chartType = event.target.closest("[data-chart-type]");
    if (chartType) {
      const index = Number(chartType.dataset.slotIndex);
      state.chartSlots[index].type = chartType.dataset.chartType;
      renderChartGrid();
      return;
    }

    const refreshSlot = event.target.closest("[data-refresh-slot]");
    if (refreshSlot) {
      loadChartSlot(Number(refreshSlot.dataset.refreshSlot), true);
      return;
    }

    const closeSlot = event.target.closest("[data-close-slot]");
    if (closeSlot) {
      closeChartSlot(Number(closeSlot.dataset.closeSlot));
    }
  });

  document.addEventListener("change", (event) => {
    const toggle = event.target.closest("[data-ma-toggle]");
    if (!toggle) return;
    const index = Number(toggle.dataset.slotIndex);
    const ma = toggle.dataset.maToggle;
    state.chartSlots[index][`ma${ma}`] = toggle.checked;
    renderChartGrid();
  });

  window.addEventListener("resize", () => {
    window.clearTimeout(window.__chartResizeTimer);
    window.__chartResizeTimer = window.setTimeout(() => rebuildCharts(), 120);
  });
}

async function bootstrap() {
  renderChartGrid();
  wireEvents();
  try {
    await Promise.all([loadInitial(), refreshMacroPanels(false)]);
    if (BOOT_ASSET) {
      await openChartForAsset(BOOT_ASSET);
    } else {
      await preloadDefaultCharts();
    }
  } catch (error) {
    pushToast("Initial Load Error", error.message);
  }
  if (SNAPSHOT_MODE) {
    state.connectionState = "connecting";
    renderConnectionState();
    return;
  }
  connectStream();
  window.setInterval(() => refreshMarket(true), MARKET_REFRESH_INTERVAL_MS);
  window.setInterval(() => refreshMacroPanels(false), AUX_REFRESH_INTERVAL_MS);
  window.setInterval(() => refreshOpenCharts(), CHART_REFRESH_INTERVAL_MS);
}

bootstrap();
