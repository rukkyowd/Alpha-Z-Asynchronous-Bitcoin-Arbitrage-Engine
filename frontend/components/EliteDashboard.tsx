"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  BarChart3,
  Bell,
  BookOpen,
  CheckCircle2,
  Clock,
  Crosshair,
  Info,
  LayoutDashboard,
  Pause,
  Play,
  RefreshCcw,
  Settings2,
  SlidersHorizontal,
  Target,
  TrendingDown,
  TrendingUp,
  Volume2,
  VolumeX,
  Wifi,
  WifiOff,
  X,
  Zap,
} from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import { CandlestickSeries, ColorType, createChart } from "lightweight-charts";
import PriceChart from "../components/PriceChart";
import EquityCurve from "../components/EquityCurve";
import WinHeatmap from "../components/WinHeatmap";
import AiInsights from "../components/AiInsights";
import BrierScoreGauge from "../components/BrierScoreGauge";
import DrawdownRiskGauge from "../components/DrawdownRiskGauge";
import CalibrationBand from "../components/CalibrationBand";
import OrderFlowStrip, { logsToFlowEvents } from "../components/OrderFlowStrip";
import GrowthChart from "../components/GrowthChart";
import {
  type DashboardLiveData,
  type DrawdownGuardSnapshot,
  type EnginePosition,
  type EngineTelemetrySnapshot,
  type EngineWsPayload,
  type MarketCandleSnapshot,
  type PriceBar,
  type SystemLockItem,
  type TechnicalContextSnapshot,
  DEFAULT_DASHBOARD_LIVE_DATA,
} from "../components/engine-types";
import { getHttpUrl, getWsUrl } from "../config";

type WsStatus = "CONNECTING" | "LIVE" | "OFFLINE" | "ERROR";
type TimeMode = "UTC" | "LOCAL";
type ToastLevel = "success" | "warning" | "error" | "info";

type EngineHealth = {
  ws_latency_ms: number;
  feed_stale_ms: number;
  memory_mb: number;
  memory_peak_mb: number;
  ai_response_ms: number;
  ai_response_ema_ms: number;
  ai_status: string;
  ai_in_flight: boolean;
  ai_failures: number;
  api_inflight: number;
  api_capacity: number;
  api_saturation_pct: number;
  kill_switch: boolean;
};

type EngineControl = {
  kill_switch: boolean;
  paper_trading: boolean;
  max_trade_pct: number;
  max_daily_loss_pct: number;
};

type ToastItem = {
  id: number;
  level: ToastLevel;
  title: string;
  message: string;
  ts: number;
};

type AudioEventKey = "trade_entered" | "stop_loss" | "take_profit" | "circuit_breaker";

type AlertSettings = {
  volume: number;
  sound_trade_entered: boolean;
  sound_stop_loss: boolean;
  sound_take_profit: boolean;
  sound_circuit_breaker: boolean;
};

function safeString(val: any, fallback = ""): string {
  if (val === null || val === undefined || Number.isNaN(val)) return fallback;
  return String(val);
}

function formatSeconds(secs: number): string {
  if (!secs || secs <= 0) return "";
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.floor(secs % 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

const PAGE_SIZE = 20;
const WS_RECONNECT_SECONDS = 3;
const TOAST_DURATION_MS = 4200;
const OFFLINE_FALLBACK_POLL_MS = 4_000;
const ALERT_SETTINGS_STORAGE_KEY = "elite-alert-settings-v1";
const ENGINE_CONTROL_API_KEY = process.env.NEXT_PUBLIC_ENGINE_CONTROL_API_KEY?.trim() ?? "";

const DEFAULT_ALERT_SETTINGS: AlertSettings = {
  volume: 0.25,
  sound_trade_entered: true,
  sound_stop_loss: true,
  sound_take_profit: true,
  sound_circuit_breaker: true,
};

const DEFAULT_ENGINE_CONTROL: EngineControl = {
  kill_switch: false,
  paper_trading: true,
  max_trade_pct: 0.03,
  max_daily_loss_pct: 0.2,
};

const DEFAULT_ENGINE_HEALTH: EngineHealth = {
  ws_latency_ms: 0,
  feed_stale_ms: 0,
  memory_mb: 0,
  memory_peak_mb: 0,
  ai_response_ms: 0,
  ai_response_ema_ms: 0,
  ai_status: "ACTIVE",
  ai_in_flight: false,
  ai_failures: 0,
  api_inflight: 0,
  api_capacity: 5,
  api_saturation_pct: 0,
  kill_switch: false,
};

function parseUtcTimestamp(raw: string): Date | null {
  if (!raw) {
    return null;
  }
  const parsed = new Date(raw.replace(" ", "T") + "Z");
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed;
}

function formatTimestamp(raw: string, mode: TimeMode): string {
  const parsed = parseUtcTimestamp(raw);
  if (!parsed) {
    return raw || "-";
  }
  if (mode === "LOCAL") {
    return parsed.toLocaleString(undefined, {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
  }
  return parsed.toISOString().replace("T", " ").slice(0, 19) + " UTC";
}

function safeNumber(value: unknown, fallback = 0): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function normalizeSignedInt(value: number): number {
  const rounded = Math.round(value);
  return Object.is(rounded, -0) ? 0 : rounded;
}

function formatSignedInt(value: number): string {
  const normalized = normalizeSignedInt(value);
  if (normalized > 0) {
    return `+${normalized}`;
  }
  return `${normalized}`;
}

function formatDuration(seconds: number): string {
  const total = Math.max(0, Math.floor(seconds));
  const mm = Math.floor(total / 60);
  const ss = total % 60;
  return `${String(mm).padStart(2, "0")}:${String(ss).padStart(2, "0")}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toEpochSeconds(raw: unknown): number {
  if (typeof raw === "number" && Number.isFinite(raw)) {
    return raw > 10_000_000_000 ? Math.floor(raw / 1000) : Math.floor(raw);
  }
  if (typeof raw === "string" && raw.trim()) {
    const asNumber = Number(raw);
    if (Number.isFinite(asNumber)) {
      return toEpochSeconds(asNumber);
    }
    const parsed = parseUtcTimestamp(raw);
    if (parsed) {
      return Math.floor(parsed.getTime() / 1000);
    }
  }
  return 0;
}

function normalizePriceBar(raw: unknown): PriceBar | null {
  if (!isRecord(raw)) {
    return null;
  }
  const time = toEpochSeconds(raw.time ?? raw.timestamp);
  const open = safeNumber(raw.open);
  const high = safeNumber(raw.high);
  const low = safeNumber(raw.low);
  const close = safeNumber(raw.close);
  const volume = safeNumber(raw.volume);
  if (!time || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) {
    return null;
  }
  return { time, open, high, low, close, volume };
}

function normalizeLiveCandle(raw: Partial<MarketCandleSnapshot> | null | undefined): PriceBar | null {
  return normalizePriceBar(raw ?? null);
}

function dedupePriceBars(bars: PriceBar[]): PriceBar[] {
  const deduped = new Map<number, PriceBar>();
  bars
    .slice()
    .sort((a, b) => a.time - b.time)
    .forEach((bar) => {
      deduped.set(bar.time, bar);
    });
  return Array.from(deduped.values()).sort((a, b) => a.time - b.time);
}

function normalizeSystemLocks(rawLocks: unknown, latestRejectedReason: unknown): SystemLockItem[] {
  const items: SystemLockItem[] = [];
  const pushItem = (label: string, remainingSecs = 0, scope = "global") => {
    const trimmed = label.trim();
    if (!trimmed) {
      return;
    }
    items.push({
      key: `${scope}:${trimmed}`,
      label: trimmed,
      scope,
      remaining_secs: Math.max(0, Math.floor(remainingSecs)),
    });
  };

  if (Array.isArray(rawLocks)) {
    rawLocks.forEach((lock, index) => {
      if (typeof lock === "string") {
        pushItem(lock);
        return;
      }
      if (isRecord(lock)) {
        const label = String(lock.label ?? lock.type ?? lock.reason ?? "").trim();
        const scope = String(lock.slug ?? lock.scope ?? "global");
        const remainingSecs = safeNumber(lock.remaining_secs, 0);
        pushItem(label || `Lock ${index + 1}`, remainingSecs, scope);
      }
    });
  }

  if (typeof latestRejectedReason === "string" && latestRejectedReason.trim()) {
    pushItem(latestRejectedReason);
  }

  const deduped = new Map<string, SystemLockItem>();
  items.forEach((item) => {
    deduped.set(item.key, item);
  });
  return Array.from(deduped.values()).slice(-6);
}

function buildBackendFetchCandidates(path: string): string[] {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const candidates = new Set<string>();
  const apiUrl = getHttpUrl();
  const wsUrl = getWsUrl();

  if (apiUrl) {
    candidates.add(`${apiUrl}${normalizedPath}`);
  }

  if (wsUrl) {
    try {
      const parsed = new URL(wsUrl);
      parsed.protocol = parsed.protocol === "wss:" ? "https:" : "http:";
      parsed.pathname = "";
      parsed.search = "";
      parsed.hash = "";
      candidates.add(`${parsed.toString().replace(/\/+$/, "")}${normalizedPath}`);
    } catch {
      // Ignore malformed WS URL candidate.
    }
  }

  if (typeof window !== "undefined" && window.location?.origin) {
    candidates.add(`${window.location.origin.replace(/\/+$/, "")}${normalizedPath}`);
  }

  return Array.from(candidates);
}

async function fetchBackend(path: string, init?: RequestInit): Promise<Response> {
  const candidates = buildBackendFetchCandidates(path);
  let lastError: unknown = null;

  for (const url of candidates) {
    try {
      return await fetch(url, init);
    } catch (error) {
      lastError = error;
    }
  }

  throw lastError instanceof Error ? lastError : new Error(`Failed to fetch ${path}`);
}

function normalizePositions(raw: EngineWsPayload["positions"]): Record<string, EnginePosition> {
  if (Array.isArray(raw)) {
    return raw.reduce<Record<string, EnginePosition>>((acc, position) => {
      if (position?.slug) {
        acc[position.slug] = position;
      }
      return acc;
    }, {});
  }
  if (isRecord(raw)) {
    return Object.entries(raw).reduce<Record<string, EnginePosition>>((acc, [slug, value]) => {
      if (isRecord(value)) {
        acc[slug] = { ...(value as unknown as EnginePosition), slug: String((value as EnginePosition).slug || slug) };
      }
      return acc;
    }, {});
  }
  return {};
}

function extractContext(
  market: DashboardLiveData["market"],
  telemetry: EngineTelemetrySnapshot,
): Partial<TechnicalContextSnapshot> {
  const telemetryContext = isRecord(telemetry.context) ? telemetry.context : null;
  const marketContext = isRecord(market.latest_context) ? market.latest_context : null;
  return {
    ...(marketContext || {}),
    ...(telemetryContext || {}),
  };
}

function normalizeDrawdownGuard(
  drawdownGuard: Partial<DrawdownGuardSnapshot> | undefined,
  previous: DashboardLiveData,
  runtime: EngineWsPayload["runtime"],
): DrawdownGuardSnapshot {
  const previousGuard = previous.drawdown_guard || DEFAULT_DASHBOARD_LIVE_DATA.drawdown_guard;
  const next = {
    ...previousGuard,
    ...(drawdownGuard || {}),
  };
  if (!next.regime) {
    next.regime = runtime?.kill_switch_enabled ? "PAUSED" : "NORMAL";
  }
  if (!next.text) {
    next.text = next.regime === "PAUSED" ? "Trading paused" : "";
  }
  if (!Number.isFinite(Number(next.current_balance)) && Number.isFinite(Number(runtime?.simulated_balance))) {
    next.current_balance = safeNumber(runtime?.simulated_balance);
  }
  return next;
}

function normalizeWsPayload(raw: unknown, previous: DashboardLiveData): DashboardLiveData {
  if (!isRecord(raw)) {
    return previous;
  }

  const legacyRootKeys = ["price", "history", "strategy", "active_trades", "candle"];
  const isLegacyPayload = legacyRootKeys.some((key) => key in raw);
  if (isLegacyPayload) {
    const merged = {
      ...DEFAULT_DASHBOARD_LIVE_DATA,
      ...previous,
      ...raw,
    } as DashboardLiveData;
    return {
      ...merged,
      history: Array.isArray(raw.history)
        ? raw.history.map(normalizePriceBar).filter((bar): bar is PriceBar => Boolean(bar))
        : previous.history,
      candle: normalizeLiveCandle((raw.candle as Partial<MarketCandleSnapshot> | null | undefined) ?? previous.candle),
      active_trades: isRecord(raw.active_trades)
        ? normalizePositions(raw.active_trades as EngineWsPayload["positions"])
        : previous.active_trades,
    };
  }

  const payload = raw as EngineWsPayload;
  const market = {
    ...previous.market,
    ...(payload.market || {}),
  };
  const telemetry = {
    ...previous.telemetry,
    ...(payload.telemetry || {}),
    edge_snapshot: {
      ...previous.telemetry.edge_snapshot,
      ...(payload.telemetry?.edge_snapshot || {}),
    },
    signal_alignment: {
      ...previous.telemetry.signal_alignment,
      ...(payload.telemetry?.signal_alignment || {}),
    },
    execution_timing: {
      ...previous.telemetry.execution_timing,
      ...(payload.telemetry?.execution_timing || {}),
    },
    cvd_snapshot: {
      ...previous.telemetry.cvd_snapshot,
      ...(payload.telemetry?.cvd_snapshot || {}),
    },
    adaptive_thresholds: {
      ...previous.telemetry.adaptive_thresholds,
      ...(payload.telemetry?.adaptive_thresholds || {}),
    },
    last_ai_interaction: {
      ...previous.telemetry.last_ai_interaction,
      ...(payload.telemetry?.last_ai_interaction || {}),
    },
  };
  const runtime = {
    ...previous.runtime,
    ...(payload.runtime || {}),
  };
  const drawdown_guard = normalizeDrawdownGuard(payload.drawdown_guard, previous, runtime);
  const positions = normalizePositions(payload.positions);
  const context = extractContext(market, telemetry);

  const historySource = Array.isArray(payload.market?.candle_history) ? payload.market?.candle_history : previous.market.candle_history;
  const history = dedupePriceBars((historySource || [])
    .map(normalizePriceBar)
    .filter((bar): bar is PriceBar => Boolean(bar))
  );

  const candle = normalizeLiveCandle((payload.market?.live_candle as Partial<MarketCandleSnapshot> | null | undefined) ?? market.live_candle) ?? previous.candle;
  const price = safeNumber(payload.market?.live_price, previous.price);
  const vwap = safeNumber((context as Partial<TechnicalContextSnapshot>).vwap, previous.vwap);
  const regime = String((context as Partial<TechnicalContextSnapshot>).market_regime || previous.regime || "UNKNOWN");
  const atr = safeNumber((context as Partial<TechnicalContextSnapshot>).adaptive_atr_floor, previous.atr);
  const positionSecondsRemaining = Object.values(positions)[0]?.seconds_remaining;
  const seconds_remaining = safeNumber(payload.meta?.seconds_remaining, positionSecondsRemaining ?? previous.seconds_remaining);
  const strike_price = safeNumber(payload.meta?.strike_price, previous.meta?.strike_price as number | undefined);
  const market_end_iso = typeof payload.meta?.market_end_iso === "string" ? payload.meta.market_end_iso : (previous.meta?.market_end_iso as string | undefined);
  const logs = Array.isArray(runtime.logs)
    ? runtime.logs.map((item) => String(item))
    : previous.logs;
  const aiInFlight = Array.isArray(runtime.ai_call_in_flight)
    ? runtime.ai_call_in_flight.length > 0
    : Boolean(runtime.ai_call_in_flight);

  const strategy = {
    edge_tracker: {
      ...DEFAULT_DASHBOARD_LIVE_DATA.strategy.edge_tracker,
      ...previous.strategy.edge_tracker,
      ...telemetry.edge_snapshot,
    },
    signal_alignment: {
      ...DEFAULT_DASHBOARD_LIVE_DATA.strategy.signal_alignment,
      ...previous.strategy.signal_alignment,
      ...telemetry.signal_alignment,
    },
    execution_latency: {
      ...DEFAULT_DASHBOARD_LIVE_DATA.strategy.execution_latency,
      ...previous.strategy.execution_latency,
      ...telemetry.execution_timing,
    },
    cvd_gauge: {
      ...DEFAULT_DASHBOARD_LIVE_DATA.strategy.cvd_gauge,
      ...previous.strategy.cvd_gauge,
      ...telemetry.cvd_snapshot,
    },
    adaptive_thresholds: {
      ...DEFAULT_DASHBOARD_LIVE_DATA.strategy.adaptive_thresholds,
      ...previous.strategy.adaptive_thresholds,
      ...telemetry.adaptive_thresholds,
    },
    system_locks: {
      ...DEFAULT_DASHBOARD_LIVE_DATA.strategy.system_locks,
      ...previous.strategy.system_locks,
      locks: normalizeSystemLocks(runtime.locks ?? previous.strategy.system_locks.locks, runtime.latest_rejected_reason),
      ai_circuit_open: safeNumber(runtime.ai_circuit_open_until) > Date.now() / 1000,
      ai_circuit_remaining_secs: Math.max(0, Math.floor(safeNumber(runtime.ai_circuit_open_until) - Date.now() / 1000)),
      ai_failures: safeNumber(runtime.ai_consecutive_failures),
      ai_in_flight: aiInFlight,
    },
    drawdown_guard,
  };

  return {
    market: {
      ...market,
      live_price: price,
      live_candle: payload.market?.live_candle ? { ...payload.market.live_candle } : market.live_candle,
      candle_history: historySource || [],
      latest_context: Object.keys(context).length > 0 ? context : market.latest_context,
    },
    positions,
    telemetry,
    runtime,
    drawdown_guard,
    price,
    vwap,
    history: history.length > 0 ? history : previous.history,
    active_trades: positions,
    candle,
    is_paper: Boolean(runtime.paper_trading ?? previous.is_paper),
    logs,
    regime,
    atr,
    seconds_remaining,
    ai_log: {
      prompt: String(telemetry.last_ai_interaction?.prompt || previous.ai_log.prompt),
      response: String(telemetry.last_ai_interaction?.response || previous.ai_log.response),
      timestamp: String(telemetry.last_ai_interaction?.timestamp || previous.ai_log.timestamp),
    },
    strategy,
    quant: {
      bayesian_probability: safeNumber((context as Partial<TechnicalContextSnapshot>).bayesian_probability, previous.quant.bayesian_probability),
      garman_klass_volatility: safeNumber((context as Partial<TechnicalContextSnapshot>).garman_klass_volatility, previous.quant.garman_klass_volatility),
      cvd_candle_delta: safeNumber((context as Partial<TechnicalContextSnapshot>).cvd_candle_delta, previous.quant.cvd_candle_delta),
      expected_move_sigma: safeNumber((context as Partial<TechnicalContextSnapshot>).expected_move_sigma, previous.quant.expected_move_sigma),
    },
    meta: {
      ...payload.meta,
      ...previous.meta,
      strike_price,
      market_end_iso,
      seconds_remaining,
    },
  };
}

function getOutcomeFromPnl(pnl: number): "WIN" | "LOSS" | "TIE" {
  if (pnl > 0) {
    return "WIN";
  }
  if (pnl < 0) {
    return "LOSS";
  }
  return "TIE";
}

export default function EliteDashboard() {
  const [activeTab, setActiveTab] = useState<"terminal" | "journal" | "analytics">("terminal");
  const [portfolio, setPortfolio] = useState<any>(null);
  const [liveData, setLiveData] = useState<DashboardLiveData>(DEFAULT_DASHBOARD_LIVE_DATA);
  const [wsStatus, setWsStatus] = useState<WsStatus>("CONNECTING");
  const [reconnectIn, setReconnectIn] = useState(0);
  const [reconnectNonce, setReconnectNonce] = useState(0);
  const [replayTrade, setReplayTrade] = useState<any>(null);
  const [priceChange, setPriceChange] = useState(0);
  const [metricsLoading, setMetricsLoading] = useState(true);
  const [liveLoading, setLiveLoading] = useState(true);

  const [engineHealth, setEngineHealth] = useState<EngineHealth>(DEFAULT_ENGINE_HEALTH);
  const [engineControl, setEngineControl] = useState<EngineControl>(DEFAULT_ENGINE_CONTROL);
  const [controlDraft, setControlDraft] = useState<EngineControl>(DEFAULT_ENGINE_CONTROL);
  const [showControls, setShowControls] = useState(false);
  const [controlDirty, setControlDirty] = useState(false);
  const [savingControl, setSavingControl] = useState(false);
  const [showAlertSettings, setShowAlertSettings] = useState(false);
  const [showNotificationCenter, setShowNotificationCenter] = useState(false);

  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const [notificationHistory, setNotificationHistory] = useState<ToastItem[]>([]);
  const [alertSettings, setAlertSettings] = useState<AlertSettings>(() => {
    if (typeof window === "undefined") {
      return DEFAULT_ALERT_SETTINGS;
    }
    try {
      const raw = window.localStorage.getItem(ALERT_SETTINGS_STORAGE_KEY);
      if (!raw) {
        return DEFAULT_ALERT_SETTINGS;
      }
      const parsed = JSON.parse(raw);
      return { ...DEFAULT_ALERT_SETTINGS, ...(parsed || {}) };
    } catch {
      return DEFAULT_ALERT_SETTINGS;
    }
  });

  const [timeMode, setTimeMode] = useState<TimeMode>(() => {
    if (typeof window === "undefined") {
      return "UTC";
    }
    return window.localStorage.getItem("elite-time-mode") === "LOCAL" ? "LOCAL" : "UTC";
  });

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const reconnectCountdownRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const lastPriceRef = useRef(0);
  const lastLogRef = useRef<string>("");
  const toastIdRef = useRef(1);
  const healthSupportedRef = useRef(true);
  const controlSupportedRef = useRef(true);
  const showControlsRef = useRef(showControls);
  const controlDirtyRef = useRef(controlDirty);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("elite-time-mode", timeMode);
    }
  }, [timeMode]);

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem(ALERT_SETTINGS_STORAGE_KEY, JSON.stringify(alertSettings));
    }
  }, [alertSettings]);

  useEffect(() => {
    showControlsRef.current = showControls;
  }, [showControls]);

  useEffect(() => {
    controlDirtyRef.current = controlDirty;
  }, [controlDirty]);

  const addToast = useCallback((level: ToastLevel, title: string, message: string) => {
    const id = toastIdRef.current++;
    const event: ToastItem = { id, level, title, message, ts: Date.now() };
    setToasts((prev) => [...prev, event]);
    setNotificationHistory((prev) => [event, ...prev].slice(0, 10));
    window.setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, TOAST_DURATION_MS);
  }, []);

  const playNotification = useCallback((event: AudioEventKey) => {
    const keyMap: Record<AudioEventKey, keyof AlertSettings> = {
      trade_entered: "sound_trade_entered",
      stop_loss: "sound_stop_loss",
      take_profit: "sound_take_profit",
      circuit_breaker: "sound_circuit_breaker",
    };
    if (!alertSettings[keyMap[event]] || alertSettings.volume <= 0) {
      return;
    }
    const audio = new Audio("https://assets.mixkit.co/active_storage/sfx/2358/2358-preview.mp3");
    audio.volume = clamp(alertSettings.volume, 0, 1);
    audio.play().catch(() => {
      // Ignore autoplay restrictions.
    });
  }, [alertSettings]);

  const clearReconnectTimers = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    if (reconnectCountdownRef.current) {
      clearInterval(reconnectCountdownRef.current);
      reconnectCountdownRef.current = null;
    }
  }, []);

  const scheduleReconnect = useCallback(
    (seconds = WS_RECONNECT_SECONDS) => {
      clearReconnectTimers();
      setWsStatus("OFFLINE");
      setReconnectIn(seconds);

      reconnectCountdownRef.current = setInterval(() => {
        setReconnectIn((prev) => {
          if (prev <= 1) {
            if (reconnectCountdownRef.current) {
              clearInterval(reconnectCountdownRef.current);
              reconnectCountdownRef.current = null;
            }
            return 0;
          }
          return prev - 1;
        });
      }, 1000);

      reconnectTimeoutRef.current = setTimeout(() => {
        setReconnectNonce((n) => n + 1);
      }, seconds * 1000);
    },
    [clearReconnectTimers],
  );

  const forceReconnect = useCallback(() => {
    clearReconnectTimers();
    setReconnectIn(0);
    setReconnectNonce((n) => n + 1);
  }, [clearReconnectTimers]);

  useEffect(() => {
    let disposed = false;
    setWsStatus("CONNECTING");

    const ws = new WebSocket(getWsUrl());
    wsRef.current = ws;

    ws.onopen = () => {
      if (disposed) {
        return;
      }
      clearReconnectTimers();
      setReconnectIn(0);
      setWsStatus("LIVE");
    };

    ws.onclose = () => {
      if (disposed) {
        return;
      }
      scheduleReconnect();
    };

    ws.onerror = () => {
      if (disposed) {
        return;
      }
      setWsStatus("ERROR");
      ws.close();
    };

    ws.onmessage = (event) => {
      if (disposed) {
        return;
      }
      try {
        const rawPayload = JSON.parse(event.data) as EngineWsPayload;
        const nextPayload = normalizeWsPayload(rawPayload, liveData);

        if (nextPayload.price && lastPriceRef.current) {
          const change = ((nextPayload.price - lastPriceRef.current) / lastPriceRef.current) * 100;
          setPriceChange(change);
        }
        lastPriceRef.current = safeNumber(nextPayload.price);

        if (isRecord(rawPayload.portfolio) && !rawPayload.portfolio?.error) {
          setPortfolio(rawPayload.portfolio);
          setMetricsLoading(false);
        }

        if (rawPayload.engine_health && typeof rawPayload.engine_health === "object") {
          healthSupportedRef.current = true;
          setEngineHealth({ ...DEFAULT_ENGINE_HEALTH, ...rawPayload.engine_health });
        }

        if (rawPayload.engine_control && typeof rawPayload.engine_control === "object") {
          controlSupportedRef.current = true;
          const nextControl: EngineControl = {
            kill_switch: Boolean(rawPayload.engine_control.kill_switch),
            paper_trading: Boolean(rawPayload.engine_control.paper_trading),
            max_trade_pct: safeNumber(rawPayload.engine_control.max_trade_pct, 0.03),
            max_daily_loss_pct: safeNumber(rawPayload.engine_control.max_daily_loss_pct, 0.2),
          };
          setEngineControl(nextControl);
          if (!showControlsRef.current || !controlDirtyRef.current) {
            setControlDraft(nextControl);
          }
        }

        setLiveData((prev) => normalizeWsPayload(rawPayload, prev));
        setLiveLoading(false);
      } catch (error) {
        console.error("WebSocket payload parse error:", error);
      }
    };

    return () => {
      disposed = true;
      clearReconnectTimers();
      ws.close();
    };
  }, [reconnectNonce, clearReconnectTimers, scheduleReconnect]);

  const fetchMetrics = useCallback(async () => {
    try {
      const res = await fetchBackend("/api/metrics");
      if (!res.ok) {
        throw new Error(`Metrics request failed: ${res.status}`);
      }
      const data = await res.json();
      if (!data.error) {
        setPortfolio(data);
      }
    } catch (error) {
      console.error("Metrics sync error:", error);
    } finally {
      setMetricsLoading(false);
    }
  }, []);

  const fetchEngineHealth = useCallback(async () => {
    if (!healthSupportedRef.current) {
      return;
    }
    try {
      const res = await fetchBackend("/api/engine/health");
      if (res.status === 404) {
        healthSupportedRef.current = false;
        addToast("warning", "Health Endpoint Missing", "Backend does not expose /api/engine/health yet.");
        return;
      }
      if (!res.ok) {
        return;
      }
      const data = await res.json();
      setEngineHealth({ ...DEFAULT_ENGINE_HEALTH, ...data });
    } catch (error) {
      console.error("Engine health fetch error:", error);
    }
  }, [addToast]);

  const fetchEngineControl = useCallback(
    async (syncDraft = false) => {
      if (!controlSupportedRef.current) {
        return;
      }
      try {
        const res = await fetchBackend("/api/engine/control");
        if (res.status === 404) {
          controlSupportedRef.current = false;
          addToast("warning", "Control Endpoint Missing", "Backend does not expose /api/engine/control yet.");
          return;
        }
        if (!res.ok) {
          return;
        }
        const data = await res.json();
        const nextControl: EngineControl = {
          kill_switch: Boolean(data?.kill_switch),
          paper_trading: Boolean(data?.paper_trading),
          max_trade_pct: safeNumber(data?.max_trade_pct, 0.03),
          max_daily_loss_pct: safeNumber(data?.max_daily_loss_pct, 0.2),
        };
        setEngineControl(nextControl);
        if (syncDraft || !showControls || !controlDirty) {
          setControlDraft(nextControl);
        }
      } catch (error) {
        console.error("Engine control fetch error:", error);
      }
    },
    [showControls, controlDirty, addToast],
  );

  const fetchHistory = useCallback(async () => {
    try {
      const res = await fetchBackend("/api/history");
      if (!res.ok) {
        throw new Error(`History request failed: ${res.status}`);
      }
      const data = await res.json();
      const historyBars = dedupePriceBars(
        Array.isArray(data?.history)
          ? (data.history as unknown[]).map(normalizePriceBar).filter((bar): bar is PriceBar => Boolean(bar))
          : [],
      );

      const lastBar = historyBars.length > 0 ? historyBars[historyBars.length - 1] : null;
      const nextPrice = safeNumber(data?.price, lastBar?.close ?? 0);

      if (historyBars.length > 0) {
        lastPriceRef.current = nextPrice || lastBar?.close || lastPriceRef.current;
      }

      setLiveData((prev) => ({
        ...prev,
        price: nextPrice || prev.price,
        history: historyBars.length > 0 ? historyBars : prev.history,
        candle: lastBar ?? prev.candle,
        market: {
          ...prev.market,
          live_price: nextPrice || prev.market.live_price,
          candle_history: historyBars.length > 0 ? historyBars : prev.market.candle_history,
          live_candle: lastBar ?? prev.market.live_candle,
        },
      }));

      if (historyBars.length > 0 || nextPrice > 0) {
        setLiveLoading(false);
      }
    } catch (error) {
      console.error("History sync error:", error);
    }
  }, []);

  useEffect(() => {
    fetchMetrics();
    fetchEngineHealth();
    fetchEngineControl(true);
    fetchHistory();
  }, [fetchMetrics, fetchEngineHealth, fetchEngineControl, fetchHistory]);

  useEffect(() => {
    if (wsStatus === "LIVE") {
      return;
    }
    const fallbackInterval = setInterval(() => {
      fetchMetrics();
      fetchEngineHealth();
      fetchEngineControl(false);
      fetchHistory();
    }, OFFLINE_FALLBACK_POLL_MS);
    return () => {
      clearInterval(fallbackInterval);
    };
  }, [wsStatus, fetchMetrics, fetchEngineHealth, fetchEngineControl, fetchHistory]);

  useEffect(() => {
    const logs = Array.isArray(liveData.logs) ? liveData.logs : [];
    if (logs.length === 0) {
      return;
    }
    const latestLog = String(logs[logs.length - 1]);
    if (!latestLog || latestLog === lastLogRef.current) {
      return;
    }
    lastLogRef.current = latestLog;

    if (latestLog.includes("[BET] BET PLACED")) {
      addToast("success", "Trade Placed", "A new position was opened.");
      playNotification("trade_entered");
    } else if (latestLog.includes("[SL HIT]") || latestLog.includes("STOP_LOSS_CONFIRMED")) {
      addToast("error", "Stop Loss Hit", "A position was exited by stop loss.");
      playNotification("stop_loss");
    } else if (latestLog.includes("[TP HIT]")) {
      addToast("info", "Take Profit Hit", "A position reached take-profit conditions.");
      playNotification("take_profit");
    } else if (latestLog.includes("[CIRCUIT]")) {
      addToast("error", "AI Circuit Breaker", "AI validation is temporarily locked out.");
      playNotification("circuit_breaker");
    } else if (latestLog.includes("[KILL SWITCH]")) {
      addToast("warning", "Kill Switch Active", "New trade entries are paused.");
    }

    const shouldRefreshUi =
      latestLog.includes("[BET] BET PLACED") ||
      latestLog.includes("[SL HIT]") ||
      latestLog.includes("STOP_LOSS_CONFIRMED") ||
      latestLog.includes("[TP HIT]") ||
      latestLog.includes("[EARLY EXIT]") ||
      latestLog.includes("[CIRCUIT]") ||
      latestLog.includes("[KILL SWITCH]") ||
      latestLog.includes("[RESOLVE]") ||
      latestLog.includes("[STATS]");

    if (shouldRefreshUi && wsStatus !== "LIVE") {
      fetchMetrics();
      fetchEngineHealth();
      if (latestLog.includes("[KILL SWITCH]")) {
        fetchEngineControl(false);
      }
    }
  }, [liveData.logs, addToast, playNotification, fetchMetrics, fetchEngineHealth, fetchEngineControl, wsStatus]);

  const saveControls = useCallback(async () => {
    if (!controlSupportedRef.current) {
      addToast("error", "Controls Unsupported", "Current backend does not support runtime control updates.");
      return;
    }
    setSavingControl(true);
    try {
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      if (ENGINE_CONTROL_API_KEY) {
        headers["x-api-key"] = ENGINE_CONTROL_API_KEY;
      }
      const res = await fetchBackend("/api/engine/control", {
        method: "POST",
        headers,
        body: JSON.stringify(controlDraft),
      });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(data?.detail || "Failed to update controls");
      }
      const next = { ...DEFAULT_ENGINE_CONTROL, ...data.control };
      setEngineControl(next);
      setControlDraft(next);
      setControlDirty(false);
      setShowControls(false);
      addToast("success", "Controls Updated", "Engine controls were saved.");
    } catch (error) {
      addToast("error", "Control Update Failed", error instanceof Error ? error.message : "Unknown error");
    } finally {
      setSavingControl(false);
    }
  }, [controlDraft, addToast]);

  const metrics = portfolio?.metrics;
  const portfolioValue = safeNumber(metrics?.current_balance, safeNumber(metrics?.current_pnl));
  const currentPnl = safeNumber(metrics?.current_pnl);
  const isPnlPositive = currentPnl >= 0;
  const isPriceUp = priceChange > 0;
  const activePositions = Object.keys(liveData.active_trades || {}).length;
  const bayesianWinProbPct = clamp(safeNumber(liveData.quant.bayesian_probability) * 100, 0, 100);
  const intradayVolatilityPct = Math.max(0, safeNumber(liveData.quant.garman_klass_volatility) * 100);

  return (
    <div className="min-h-screen bg-[var(--az-bg-base)] text-[var(--az-text-primary)] font-sans selection:bg-blue-500/25">
      <nav className="fixed bottom-0 left-0 right-0 top-auto z-50 flex h-16 w-full items-center justify-around border-t border-[var(--az-border)] bg-[var(--az-bg-surface)]/95 px-2 backdrop-blur-2xl shadow-2xl lg:left-0 lg:right-auto lg:top-0 lg:h-full lg:w-20 lg:flex-col lg:items-center lg:justify-start lg:gap-8 lg:border-r lg:border-t-0 lg:px-0 lg:py-8">
        <motion.div
          className="hidden h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-blue-600 to-blue-500 text-sm font-black text-white shadow-lg shadow-blue-500/30 lg:flex"
          whileHover={{ scale: 1.05, rotate: 5 }}
          whileTap={{ scale: 0.95 }}
        >
          αZ
        </motion.div>
        <NavIcon icon={<LayoutDashboard size={20} />} active={activeTab === "terminal"} onClick={() => setActiveTab("terminal")} label="Terminal" />
        <NavIcon icon={<BookOpen size={20} />} active={activeTab === "journal"} onClick={() => setActiveTab("journal")} label="Journal" />
        <NavIcon icon={<BarChart3 size={20} />} active={activeTab === "analytics"} onClick={() => setActiveTab("analytics")} label="Analytics" />
      </nav>

      <main className="px-4 py-5 pb-24 sm:px-6 sm:py-6 sm:pb-24 lg:px-10 lg:py-8 lg:pb-10 lg:pl-32">
        <header className="mb-6 flex flex-col gap-4 sm:mb-8 lg:mb-8 lg:flex-row lg:items-start lg:justify-between">
          <motion.div initial={{ opacity: 0, y: -20 }} animate={{ opacity: 1, y: 0 }} className="space-y-4">
            <div>
              <div className="mb-2 flex items-center gap-3">
                <h1 className="section-label">Portfolio Value</h1>
                {liveData.is_paper && (
                  <div className="badge badge-paper">
                    <span className="h-1.5 w-1.5 animate-breathe rounded-full bg-amber-400" />
                    Paper Trading
                  </div>
                )}
              </div>

              {metricsLoading ? (
                <SkeletonBox className="h-14 w-80" />
              ) : (
                <div className="flex flex-col">
                  <motion.div
                    className={`text-4xl font-black tabular-nums tracking-tight sm:text-5xl ${isPnlPositive ? "text-[var(--az-text-primary)]" : "text-[var(--az-loss)]"}`}
                    key={portfolioValue}
                    initial={{ scale: 1.03, opacity: 0.85 }}
                    animate={{ scale: 1, opacity: 1 }}
                    transition={{ type: "spring", stiffness: 280, damping: 22 }}
                  >
                    ${portfolioValue.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                  </motion.div>
                  <div className="mt-2 flex items-center gap-2 text-sm">
                    <span className="font-medium tracking-wide text-zinc-500">Total PnL</span>
                    <span className={`font-mono font-bold ${isPnlPositive ? "text-az-profit" : "text-az-loss"}`}>
                      {isPnlPositive ? "+" : "-"}${Math.abs(currentPnl).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                    </span>
                  </div>
                </div>
              )}
            </div>


          </motion.div>

          <div className="relative flex w-full flex-col items-start gap-3 sm:items-end lg:w-auto">
            <div className="flex flex-wrap items-center gap-2">
              <button
                onClick={() => setTimeMode((prev) => (prev === "UTC" ? "LOCAL" : "UTC"))}
                className="rounded-lg border border-zinc-700/70 bg-zinc-900/70 px-2.5 py-1.5 text-[10px] font-bold tracking-wide text-zinc-300 transition hover:border-zinc-500 hover:text-white sm:px-3 sm:text-[11px]"
              >
                {timeMode === "UTC" ? "UTC" : "LOCAL"}
              </button>
              <button
                onClick={() => setShowNotificationCenter((prev) => !prev)}
                className="relative rounded-lg border border-zinc-700/70 bg-zinc-900/70 p-2 text-zinc-300 transition hover:border-zinc-500 hover:text-white"
                title="Notification History"
              >
                <Bell size={14} />
                {notificationHistory.length > 0 && (
                  <span className="absolute -right-1 -top-1 flex h-4 min-w-4 items-center justify-center rounded-full bg-blue-500 px-1 text-[9px] font-black text-white">
                    {notificationHistory.length}
                  </span>
                )}
              </button>
              <button
                onClick={() => setShowAlertSettings(true)}
                className="flex items-center gap-1.5 rounded-lg border border-zinc-700/70 bg-zinc-900/70 px-2.5 py-1.5 text-[10px] font-bold tracking-wide text-zinc-300 transition hover:border-zinc-500 hover:text-white sm:px-3 sm:text-[11px]"
              >
                <SlidersHorizontal size={14} />
                Alerts
              </button>
              <button
                onClick={() => {
                  setControlDraft(engineControl);
                  setControlDirty(false);
                  setShowControls(true);
                }}
                className="flex items-center gap-1.5 rounded-lg border border-zinc-700/70 bg-zinc-900/70 px-2.5 py-1.5 text-[10px] font-bold tracking-wide text-zinc-300 transition hover:border-zinc-500 hover:text-white sm:px-3 sm:text-[11px]"
              >
                <Settings2 size={14} />
                Controls
              </button>
            </div>
            {showNotificationCenter && (
              <NotificationCenter
                items={notificationHistory}
                timeMode={timeMode}
                onClear={() => setNotificationHistory([])}
                onClose={() => setShowNotificationCenter(false)}
              />
            )}
            <StatusBadge status={wsStatus} reconnectIn={reconnectIn} onReconnect={forceReconnect} />
            <RegimeBadge regime={liveData.regime} atr={safeNumber(liveData.atr)} />
          </div>
        </header>

        <div className="mb-6 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-6">
          {metricsLoading ? (
            <>
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
            </>
          ) : (
            <>
              <EnhancedStatCard
                icon={<TrendingUp size={18} className="text-emerald-400" />}
                label="Win Rate"
                value={`${safeNumber(metrics?.win_rate).toFixed(1)}%`}
                trend={safeNumber(metrics?.win_rate) >= 60 ? "up" : "neutral"}
                subtitle={`${safeNumber(metrics?.total_wins)}W / ${safeNumber(metrics?.total_losses)}L`}
              />
              <EnhancedStatCard
                icon={<TrendingDown size={18} className="text-rose-400" />}
                label="Max Drawdown"
                value={`$${safeNumber(metrics?.max_drawdown).toFixed(2)}`}
                trend="down"
                subtitle="Peak to trough"
              />
              <EnhancedStatCard
                icon={<Target size={18} className="text-blue-400" />}
                label="Sharpe Ratio"
                value={`${safeNumber(metrics?.sharpe).toFixed(2)}`}
                trend={safeNumber(metrics?.sharpe) > 1.5 ? "up" : "neutral"}
                subtitle="Risk-adjusted returns"
              />
              <EnhancedStatCard
                icon={<Zap size={18} className="text-amber-400" />}
                label="Active Positions"
                value={activePositions.toString()}
                trend="neutral"
                subtitle="Open trades"
              />
              <EnhancedStatCard
                icon={<Target size={18} className="text-fuchsia-400" />}
                label="Bayesian Win Prob"
                value={`${bayesianWinProbPct.toFixed(1)}%`}
                trend={bayesianWinProbPct >= 55 ? "up" : bayesianWinProbPct <= 45 ? "down" : "neutral"}
                subtitle={`Move sigma ${safeNumber(liveData.quant.expected_move_sigma).toFixed(2)}`}
              />
              <EnhancedStatCard
                icon={<BarChart3 size={18} className="text-cyan-400" />}
                label="Intraday Volatility"
                value={`${intradayVolatilityPct.toFixed(3)}%`}
                trend={intradayVolatilityPct >= 0.5 ? "down" : "neutral"}
                subtitle={`CVD ${formatSignedInt(safeNumber(liveData.quant.cvd_candle_delta))}`}
              />
            </>
          )}
        </div>

        <AnimatePresence mode="wait">
          {activeTab === "terminal" && (
            <TerminalView
              liveData={liveData}
              portfolio={portfolio}
              setReplayTrade={setReplayTrade}
              liveLoading={liveLoading}
              timeMode={timeMode}
            />
          )}
          {activeTab === "journal" && <JournalView portfolio={portfolio} setReplayTrade={setReplayTrade} timeMode={timeMode} />}
          {activeTab === "analytics" && <AnalyticsView portfolio={portfolio} engineHealth={engineHealth} liveData={liveData} />}
        </AnimatePresence>
      </main>

      <AnimatePresence>
        {replayTrade && <TradeReplayModal trade={replayTrade} onClose={() => setReplayTrade(null)} timeMode={timeMode} />}
      </AnimatePresence>

      <AnimatePresence>
        {showAlertSettings && (
          <AlertSettingsDrawer
            settings={alertSettings}
            setSettings={setAlertSettings}
            onClose={() => setShowAlertSettings(false)}
          />
        )}
      </AnimatePresence>

      <AnimatePresence>
        {showControls && (
          <ControlModal
            draft={controlDraft}
            setDraft={(next: EngineControl) => {
              setControlDraft(next);
              setControlDirty(true);
            }}
            onClose={() => setShowControls(false)}
            onSave={saveControls}
            saving={savingControl}
          />
        )}
      </AnimatePresence>

      <ToastStack toasts={toasts} onDismiss={(id) => setToasts((prev) => prev.filter((t) => t.id !== id))} />
    </div>
  );
}

function NavIcon({ icon, active, onClick, label }: any) {
  return (
    <motion.button
      onClick={onClick}
      className={`group relative flex h-12 w-20 flex-col items-center justify-center gap-0.5 rounded-xl transition-all lg:h-14 lg:w-14 ${
        active ? "bg-[var(--az-accent)] text-white shadow-lg shadow-blue-500/30" : "bg-[var(--az-bg-elevated)]/50 text-[var(--az-text-muted)] hover:bg-[var(--az-bg-elevated)] hover:text-[var(--az-text-secondary)]"
      }`}
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
    >
      {icon}
      <span className="text-[9px] font-semibold uppercase tracking-wide lg:hidden">{label}</span>
      <div className="pointer-events-none absolute left-full ml-4 hidden whitespace-nowrap rounded-lg border border-[var(--az-border)] bg-[var(--az-bg-surface)] px-3 py-1.5 text-xs font-medium opacity-0 transition-opacity group-hover:opacity-100 lg:block">
        {label}
      </div>
    </motion.button>
  );
}

function StatusBadge({ status, reconnectIn, onReconnect }: { status: WsStatus; reconnectIn: number; onReconnect: () => void }) {
  const isLive = status === "LIVE";
  return (
    <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto">
      <div
        className={`flex items-center gap-2 rounded-md border px-3 py-1.5 text-xs font-bold uppercase tracking-wider 
        ${isLive ? "border-az-border bg-az-surface text-az-profit" : "border-az-loss/30 bg-az-loss-muted text-az-loss"}`}
      >
        <span className="relative flex h-2 w-2">
          {isLive && <span className="absolute inline-flex h-full w-full animate-live-ping rounded-full bg-az-profit opacity-75"></span>}
          <span className={`relative inline-flex h-2 w-2 rounded-full ${isLive ? "bg-az-profit" : "bg-az-loss"}`}></span>
        </span>
        {status}
      </div>

      {!isLive && (
        <button
          onClick={onReconnect}
          className="flex items-center gap-1.5 rounded-md border border-az-border bg-az-surface-2 px-3 py-1.5 text-xs font-bold text-az-text transition-colors hover:bg-az-border"
        >
          <RefreshCcw size={12} />
          Reconnect{reconnectIn > 0 ? ` (${reconnectIn}s)` : ""}
        </button>
      )}
    </div>
  );
}

function RegimeBadge({ regime, atr }: { regime: string; atr: number }) {
  const colors = {
    TRENDING: "border-blue-500/30 bg-blue-500/10 text-blue-400",
    RANGING: "border-amber-500/30 bg-amber-500/10 text-amber-400",
    VOLATILE: "border-red-500/30 bg-red-500/10 text-red-400",
    UNKNOWN: "border-zinc-700/30 bg-zinc-700/10 text-zinc-500",
  } as const;

  const normalized = String(regime || "").trim().toUpperCase();
  const key = (normalized in colors ? normalized : "UNKNOWN") as keyof typeof colors;
  const showAtr = Number.isFinite(atr) && atr > 0;

  return (
    <div className={`rounded-full border px-4 py-2 text-xs font-bold ${colors[key]}`}>
      {key}
      {showAtr && <span className="ml-1 opacity-70">• ATR ${atr.toFixed(0)}</span>}
    </div>
  );
}

function EnhancedStatCard({ icon, label, value, trend, subtitle }: any) {
  const trendColors = { 
    up: "text-az-profit", 
    down: "text-az-loss", 
    neutral: "text-az-text-muted" 
  } as const;
  
  return (
    <div className="flex flex-col rounded-md border border-az-border bg-az-surface p-4">
      <div className="mb-3 flex items-center gap-2">
        <div className="text-az-text-muted">{icon}</div>
        <div className="text-[10px] font-semibold uppercase tracking-wider text-az-text-muted">{label}</div>
      </div>
      <div className={`font-mono text-xl tabular-nums tracking-tight ${trendColors[trend as keyof typeof trendColors]}`}>
        {value}
      </div>
      {subtitle && (
        <div className="mt-1 font-mono text-[10px] tabular-nums text-az-text-muted">
          {subtitle}
        </div>
      )}
    </div>
  );
}

function SkeletonBox({ className = "" }: { className?: string }) {
  return <div className={`skeleton-shimmer ${className}`} />;
}

function SkeletonCard() {
  return (
    <div className="glass-card p-5">
      <SkeletonBox className="mb-4 h-8 w-8" />
      <SkeletonBox className="mb-2 h-8 w-20" />
      <SkeletonBox className="h-3 w-32" />
    </div>
  );
}

function Panel({ title, children, className = "", contentClassName = "p-4" }: { title: string; children: React.ReactNode; className?: string; contentClassName?: string }) {
  return (
    <div className={`flex flex-col rounded-md border border-az-border bg-az-surface ${className}`}>
      <div className="flex shrink-0 items-center border-b border-az-border px-4 py-3">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-az-text-muted">{title}</h3>
      </div>
      <div className={`flex-1 ${contentClassName}`}>{children}</div>
    </div>
  );
}

function ArbitrageGapPanel({ edge }: { edge: any }) {
  const upMath = safeNumber(edge?.up_math_prob);
  const downMath = safeNumber(edge?.down_math_prob);
  const upPoly = safeNumber(edge?.up_poly_prob);
  const downPoly = safeNumber(edge?.down_poly_prob);
  const upGap = safeNumber(edge?.up_edge);
  const downGap = safeNumber(edge?.down_edge);
  const bestDir = String(edge?.direction || "UNKNOWN");
  const bestEdge = safeNumber(edge?.best_edge);

  const row = (label: string, mathProb: number, polyProb: number, gap: number) => (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wide">
        <span className="font-bold text-az-text-muted">{label}</span>
        <span className={`font-mono font-bold ${gap >= 0 ? "text-az-profit" : "text-az-loss"}`}>
          Gap {gap >= 0 ? "+" : ""}
          {gap.toFixed(2)}%
        </span>
      </div>
      <div className="space-y-1">
        <div className="h-1.5 overflow-hidden rounded-full bg-az-surface-2">
          <div className="h-full rounded-full bg-az-accent" style={{ width: `${clamp(mathProb, 0, 100)}%` }} />
        </div>
        <div className="h-1.5 overflow-hidden rounded-full bg-az-surface-2">
          <div className="h-full rounded-full bg-fuchsia-500" style={{ width: `${clamp(polyProb, 0, 100)}%` }} />
        </div>
      </div>
      <div className="flex items-center justify-between font-mono text-[10px] text-az-text-muted">
        <span>Math {mathProb.toFixed(1)}%</span>
        <span>Poly {polyProb.toFixed(1)}%</span>
      </div>
    </div>
  );

  return (
    <div className="space-y-4 pt-1">
      <div className="flex items-center justify-between rounded-md border border-az-border bg-az-surface p-2">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-az-text-muted">Best Edge</span>
        <span className={`text-xs font-black tabular-nums ${bestEdge >= 0 ? "text-az-profit" : "text-az-loss"}`}>
          {bestDir} {bestEdge >= 0 ? "+" : ""}
          {bestEdge.toFixed(2)}%
        </span>
      </div>
      {row("UP", upMath, upPoly, upGap)}
      {row("DOWN", downMath, downPoly, downGap)}
      <div className="flex items-center gap-3 text-[10px] text-az-text-muted">
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-az-accent" /> Math Prob</span>
        <span className="inline-flex items-center gap-1"><span className="h-2 w-2 rounded-full bg-fuchsia-500" /> Poly Prob</span>
      </div>
    </div>
  );
}

function SignalAlignmentMatrix({ signal }: { signal: any }) {
  const score = safeNumber(signal?.score);
  const maxScore = Math.max(1, safeNumber(signal?.max_score, 4));
  const direction = String(signal?.direction || "UNKNOWN");
  const cells = [
    { key: "vwap", label: "VWAP", ok: Boolean(signal?.vwap) },
    { key: "rsi", label: "RSI", ok: Boolean(signal?.rsi) },
    { key: "volume", label: "VOL", ok: Boolean(signal?.volume) },
    { key: "cvd", label: "CVD", ok: Boolean(signal?.cvd) },
  ];

  return (
    <div className="space-y-4 pt-1">
      <div className="flex items-center justify-between rounded-md border border-az-border bg-az-surface p-2">
        <span className="text-[10px] font-semibold uppercase tracking-wider text-az-text-muted">Direction</span>
        <span className={`text-xs font-black tabular-nums ${direction === "UP" ? "text-az-profit" : direction === "DOWN" ? "text-az-loss" : "text-az-text-muted"}`}>
          {direction} • {score}/{maxScore}
        </span>
      </div>
      <div className="grid grid-cols-2 gap-2">
        {cells.map((cell) => (
          <div key={cell.key} className={`rounded-md border p-2 ${cell.ok ? "border-az-profit/30 bg-az-profit-muted" : "border-az-loss/30 bg-az-loss-muted"}`}>
            <div className="flex items-center justify-between text-xs">
              <span className="font-semibold text-az-text-muted">{cell.label}</span>
              <span className={`font-black ${cell.ok ? "text-az-profit" : "text-az-loss"}`}>{cell.ok ? "PASS" : "FAIL"}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function LatencyWaterfallPanel({ latency }: { latency: any }) {
  const signalMs = safeNumber(latency?.signal_generation_ms);
  const aiMs = safeNumber(latency?.ai_inference_ms);
  const clobMs = safeNumber(latency?.clob_request_ms);
  const confirmMs = safeNumber(latency?.confirmation_ms);
  const totalMs = Math.max(1, safeNumber(latency?.total_ms, signalMs + aiMs + clobMs + confirmMs));
  const segments = [
    { label: "Signal", ms: signalMs, cls: "bg-az-accent" },
    { label: "AI", ms: aiMs, cls: "bg-az-warning" },
    { label: "CLOB", ms: clobMs, cls: "bg-fuchsia-500" },
    { label: "Confirm", ms: confirmMs, cls: "bg-az-profit" },
  ];

  return (
    <div className="space-y-4 pt-1">
      <div className="h-3 overflow-hidden rounded-full border border-az-border bg-az-surface-2">
        <div className="flex h-full w-full">
          {segments.map((seg) => (
            <div key={seg.label} className={seg.cls} style={{ width: `${clamp((seg.ms / totalMs) * 100, 0, 100)}%` }} />
          ))}
        </div>
      </div>
      <div className="space-y-1.5">
        {segments.map((seg) => (
          <div key={seg.label} className="flex items-center justify-between text-xs">
            <span className="text-az-text-muted">{seg.label}</span>
            <span className="font-mono text-zinc-300">{seg.ms.toFixed(0)} ms</span>
          </div>
        ))}
      </div>
      <div className="flex items-center justify-between border-t border-zinc-800 pt-2 text-xs">
        <span className="text-zinc-500">Total</span>
        <span className="font-mono font-bold text-zinc-200">{totalMs.toFixed(0)} ms</span>
      </div>
    </div>
  );
}

function CvdPressureGauge({ cvd }: { cvd: any }) {
  const delta = safeNumber(cvd?.delta);
  const oneMinFlow = safeNumber(cvd?.one_min_delta);
  const threshold = Math.max(1, safeNumber(cvd?.threshold, 12000));
  const normalized = clamp(Math.abs(delta) / threshold, 0, 1);
  const magnitudePct = normalized * 100;
  const isPositive = delta >= 0;
  const isNeutral = normalizeSignedInt(delta) === 0;
  const divergence = String(cvd?.divergence || "NONE").toUpperCase();
  const divergenceStrength = safeNumber(cvd?.divergence_strength);
  const pressureLabel = isNeutral ? "Neutral" : isPositive ? "Buy Pressure" : "Sell Pressure";
  const pressureClass = isNeutral ? "text-az-text" : isPositive ? "text-az-profit" : "text-az-loss";
  const fillClass = isNeutral ? "bg-az-surface-2" : isPositive ? "bg-az-profit" : "bg-az-loss";

  return (
    <div className="space-y-4 pt-1">
      <div className="grid grid-cols-[52px,1fr] items-center gap-4">
        <div className="relative h-40 w-12 overflow-hidden rounded-md border border-az-border bg-az-surface shadow-inner">
          <div className="absolute inset-x-1 top-1 h-1/4 rounded bg-az-surface-2" />
          <div className="absolute inset-x-1 bottom-1 h-1/4 rounded bg-az-surface-2" />
          <div className="absolute inset-x-1 top-1/2 border-t border-az-border/80" />
          <div
            className={`absolute inset-x-0 bottom-0 transition-all duration-300 ${fillClass}`}
            style={{ height: `${Math.max(2, magnitudePct)}%` }}
          />
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-az-text-muted">Pressure</div>
            <div className={`text-[11px] font-bold uppercase tracking-wide ${pressureClass}`}>{pressureLabel}</div>
          </div>

          <div className="rounded-md border border-az-border bg-az-surface-2 px-3 py-2">
            <div className="flex items-end justify-between">
              <div className={`font-mono text-xl font-black tabular-nums ${isNeutral ? "text-az-text" : isPositive ? "text-az-profit" : "text-az-loss"}`}>
                {formatSignedInt(delta)}
              </div>
              <div className="text-[10px] font-mono tabular-nums text-az-text-muted">{magnitudePct.toFixed(0)}% of min</div>
            </div>
            <div className="mt-2 h-1 overflow-hidden rounded-full bg-az-surface">
              <div className={`h-full rounded-full ${fillClass}`} style={{ width: `${Math.max(3, magnitudePct)}%` }} />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-2 text-[10px]">
            <div className="rounded-md border border-az-border bg-az-surface-2 px-2 py-1.5 text-az-text-muted">
              Threshold
              <div className="font-mono tabular-nums text-az-text">{normalizeSignedInt(threshold)}</div>
            </div>
            <div className="rounded-md border border-az-border bg-az-surface-2 px-2 py-1.5 text-az-text-muted">
              1m Flow
              <div className={`font-mono tabular-nums ${normalizeSignedInt(oneMinFlow) >= 0 ? "text-az-profit" : "text-az-loss"}`}>
                {formatSignedInt(oneMinFlow)}
              </div>
            </div>
          </div>
        </div>
      </div>

      {divergence !== "NONE" && (
        <div className="rounded-md border border-az-warning/40 bg-az-warning/10 px-3 py-2">
          <div className="text-[10px] font-black uppercase tracking-wide text-az-warning">Divergence Alert</div>
          <div className="mt-1 text-xs font-semibold text-az-warning/80">
            {divergence} <span className="font-mono tabular-nums text-az-warning">({(divergenceStrength * 100).toFixed(0)}%)</span>
          </div>
        </div>
      )}
    </div>
  );
}

function SystemLocksPanel({ locksData }: { locksData: any }) {
  const locks = Array.isArray(locksData?.locks) ? locksData.locks : [];
  const aiCircuitOpen = Boolean(locksData?.ai_circuit_open);
  const aiCircuitRemaining = safeNumber(locksData?.ai_circuit_remaining_secs);
  const aiFailures = safeNumber(locksData?.ai_failures);

  return (
    <div className="space-y-2 text-xs">
      {aiCircuitOpen && (
        <div className="rounded-md border border-az-loss/50 bg-az-loss-muted p-3">
          <div className="text-[10px] font-bold uppercase tracking-wide text-az-loss">AI Circuit Breaker</div>
          <div className="mt-1 flex items-center justify-between">
            <span className="text-az-loss/80">Reopening in {formatDuration(aiCircuitRemaining)}</span>
            <span className="font-mono text-az-loss">{aiFailures} failures</span>
          </div>
        </div>
      )}

      {locks.length === 0 ? (
        <div className="rounded-md border border-az-border bg-az-surface-2 p-3 text-az-text-muted">No active cooldowns.</div>
      ) : (
        <div className="space-y-2">
          {locks.map((lock: any, idx: number) => (
            <div key={`${String(lock?.key || idx)}`} className="rounded-md border border-az-border bg-az-surface-2 p-3">
              <div className="flex items-center justify-between">
                <span className="font-semibold text-az-text">{String(lock?.label || "LOCK")}</span>
                {safeNumber(lock?.remaining_secs) > 0 ? (
                  <span className="font-mono text-az-warning">{formatDuration(safeNumber(lock?.remaining_secs))}</span>
                ) : (
                  <span className="font-mono text-az-text-muted">state</span>
                )}
              </div>
              <div className="mt-1 truncate font-mono text-[10px] text-az-text-muted">{String(lock?.scope || "global")}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function AdaptiveThresholdPanel({ atr, cvd, thresholds }: { atr: number; cvd: any; thresholds: any }) {
  const atrNow = safeNumber(atr);
  const atrMin = Math.max(1, safeNumber(thresholds?.atr_min, 1));
  const atrRatio = atrNow / atrMin;

  const cvdDeltaAbs = Math.abs(safeNumber(cvd?.delta));
  const cvdThreshold = Math.max(1, safeNumber(thresholds?.cvd_threshold, safeNumber(cvd?.threshold, 12000)));
  const cvdRatio = cvdDeltaAbs / cvdThreshold;

  const row = (label: string, nowValue: number, minValue: number, ratio: number, tone: "accent" | "profit") => {
    const progress = clamp(ratio * 100, 0, 100);
    const color = tone === "accent" ? "bg-az-accent" : "bg-az-profit";
    return (
      <div className="space-y-1.5">
        <div className="flex items-center justify-between text-xs">
          <span className="text-[10px] font-semibold uppercase tracking-wider text-az-text-muted">{label}</span>
          <span className={`font-mono tabular-nums ${ratio >= 1 ? "text-az-profit" : "text-az-warning"}`}>
            {nowValue.toFixed(2)} / {minValue.toFixed(2)}
          </span>
        </div>
        <div className="relative h-1.5 overflow-hidden rounded-full bg-az-surface-2 border border-az-border">
          <div className={`h-full ${color}`} style={{ width: `${progress}%` }} />
          <div className="pointer-events-none absolute inset-y-0 left-[50%] border-l border-dashed border-az-text-muted/40" />
        </div>
      </div>
    );
  };

  return (
    <div className="space-y-4 pt-1">
      {row("ATR vs Min", atrNow, atrMin, atrRatio, "accent")}
      {row("CVD vs Thresh", cvdDeltaAbs, cvdThreshold, cvdRatio, "profit")}
    </div>
  );
}

function TerminalView({ liveData, portfolio, setReplayTrade, liveLoading, timeMode }: any) {
  const [chartType, setChartType] = useState<"candle" | "snake">("snake");
  const strategy = liveData?.strategy || DEFAULT_DASHBOARD_LIVE_DATA.strategy;
  const chartCandle = liveData?.candle || (Array.isArray(liveData?.history) && liveData.history.length > 0 ? liveData.history[liveData.history.length - 1] : null);
  const flowEvents = useMemo(() => logsToFlowEvents(liveData?.logs || []), [liveData?.logs]);
  const brierSummary = portfolio?.brier_summary;
  const strikePrice = safeNumber(liveData?.meta?.strike_price, liveData?.market?.latest_context?.strike_price || strategy.edge_tracker?.strike_price || 0);
  const currentPrice = safeNumber(liveData?.price);
  const diffToBeat = currentPrice - strikePrice;
  const timeToExpiry = formatSeconds(safeNumber(liveData?.meta?.seconds_remaining, liveData?.seconds_remaining));

  return (
    <motion.div key="terminal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="grid grid-cols-1 gap-2 md:grid-cols-4 md:gap-3 lg:grid-cols-6">
      <Panel title="Live Price Action • 1H Candles" className="md:col-span-4 lg:col-span-4" contentClassName="flex flex-col p-4">
        <div className="mb-4 flex flex-col md:flex-row md:items-end justify-between gap-4">
          <div className="flex items-center gap-6">
            <div>
              <div className="text-[11px] font-semibold uppercase tracking-wider text-az-text-muted mb-1 flex items-center gap-2">
                Price to beat
                {(timeToExpiry !== "00:00:00" && timeToExpiry !== "") && <span className="px-1.5 py-0.5 rounded-sm bg-zinc-800 text-zinc-300 text-[10px] border border-zinc-700 font-mono font-medium tracking-wide">Exp: {timeToExpiry}</span>}
              </div>
              <div className="font-mono text-2xl font-bold tabular-nums text-zinc-300">
                ${strikePrice > 0 ? strikePrice.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "---"}
              </div>
            </div>
            {strikePrice > 0 && (
              <div className="h-10 w-px bg-zinc-800" />
            )}
            <div>
              <div className="flex items-center gap-2 mb-1">
                <div className="text-[11px] font-semibold uppercase tracking-wider text-az-accent">Current price</div>
                <div className={`text-[10px] font-bold ${diffToBeat >= 0 ? "text-az-profit" : "text-az-loss"}`}>
                  {diffToBeat >= 0 ? "▲" : "▼"} ${Math.abs(diffToBeat).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </div>
              </div>
              <div className="font-mono text-2xl font-bold tabular-nums text-az-accent">
                ${currentPrice > 0 ? currentPrice.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : "---"}
              </div>
            </div>
          </div>
          <div className="flex items-center gap-1 rounded-md border border-az-border bg-az-surface-2 p-1">
             <button
                onClick={() => setChartType("candle")}
                className={`rounded px-3 py-1 text-xs font-semibold transition-colors ${chartType === "candle" ? "bg-zinc-700 text-white shadow-sm" : "text-az-text-muted hover:text-white"}`}
             >
               Candles
             </button>
             <button
                onClick={() => setChartType("snake")}
                className={`rounded px-3 py-1 text-xs font-semibold transition-colors ${chartType === "snake" ? "bg-amber-500/20 text-amber-500 shadow-sm border border-amber-500/30" : "text-az-text-muted hover:text-white"}`}
             >
               Snake Graph
             </button>
          </div>
        </div>
        {liveLoading ? (
          <div className="grid grid-cols-1 gap-3">
            <SkeletonBox className="h-[350px] w-full" />
          </div>
        ) : chartCandle || (Array.isArray(liveData?.history) && liveData.history.length > 0) ? (
          <PriceChart candle={chartCandle} history={liveData.history} vwap={liveData.vwap} type={chartType} />
        ) : (
          <div className="flex h-[350px] items-center justify-center rounded-xl border border-dashed border-white/10">
            <div className="section-label">Awaiting stream...</div>
          </div>
        )}
      </Panel>

      <Panel title="Active Positions" className="md:col-span-4 lg:col-span-2">
        <ActiveTradesPanel trades={liveData.active_trades} currentUnderlying={safeNumber(liveData.price)} />
      </Panel>

      <Panel title="Order Flow" className="md:col-span-4 lg:col-span-4">
        <OrderFlowStrip events={flowEvents} />
      </Panel>
      
      {brierSummary && (
        <Panel title="Brier Score · Model vs Market" className="md:col-span-2 lg:col-span-2">
          <BrierScoreGauge
            modelBrier={safeNumber(brierSummary.model_brier, 0.25)}
            marketBrier={safeNumber(brierSummary.market_brier, 0.25)}
            calibratedBrier={brierSummary.calibrated_brier != null ? safeNumber(brierSummary.calibrated_brier) : undefined}
          />
        </Panel>
      )}

      <Panel title="Arbitrage Gap (Math vs Poly)" className="md:col-span-2 lg:col-span-2">
        <ArbitrageGapPanel edge={strategy.edge_tracker} />
      </Panel>
      
      <Panel title="Signal Alignment Matrix" className="md:col-span-2 lg:col-span-2">
        <SignalAlignmentMatrix signal={strategy.signal_alignment} />
      </Panel>
      
      <Panel title="Smart Money Flow (CVD)" className="md:col-span-2 lg:col-span-2">
        <CvdPressureGauge cvd={strategy.cvd_gauge} />
      </Panel>

      <Panel title="Equity Curve" className="md:col-span-2 lg:col-span-3">
        {portfolio ? <EquityCurve data={portfolio?.equity_curve || []} /> : <SkeletonBox className="h-48 w-full" />}
      </Panel>
      
      <Panel title="Monte Carlo Projection" className="md:col-span-2 lg:col-span-3">
        {portfolio ? <GrowthChart data={portfolio?.projections?.paths || []} /> : <SkeletonBox className="h-48 w-full" />}
      </Panel>

      <Panel title="Adaptive Thresholds" className="md:col-span-2 lg:col-span-2">
        <AdaptiveThresholdPanel atr={safeNumber(liveData.atr)} cvd={strategy.cvd_gauge} thresholds={strategy.adaptive_thresholds} />
      </Panel>
      
      <Panel title="Execution Latency" className="md:col-span-2 lg:col-span-2">
        <LatencyWaterfallPanel latency={strategy.execution_latency} />
      </Panel>

      <Panel title="System Locks" className="md:col-span-2 lg:col-span-2">
        <SystemLocksPanel locksData={strategy.system_locks} />
      </Panel>

      <div className="flex flex-col gap-2 md:col-span-4 lg:col-span-4 min-h-0">
        <Panel title={`Performance Heatmap • ${timeMode}`}>
          {portfolio ? <WinHeatmap data={portfolio?.heatmap || []} /> : <SkeletonBox className="h-36 w-full" />}
        </Panel>

        <Panel title="Live Engine Terminal" className="flex-1 min-h-0" contentClassName="flex flex-col min-h-0 relative p-4">
          <TerminalStream logs={liveData.logs || []} />
        </Panel>
      </div>
      
      <div className="flex flex-col gap-2 md:col-span-4 lg:col-span-2">
        <Panel title="AI Context">
          {portfolio ? <AiInsights insights={portfolio?.insights || []} /> : <SkeletonBox className="h-32 w-full" />}
        </Panel>
        {liveData.ai_log && (
          <Panel title="Latest AI Reasoning">
            <AiLogDisplay aiLog={liveData.ai_log} />
          </Panel>
        )}
        <Panel title="Replay Shortcut">
          <button onClick={() => setReplayTrade((portfolio?.journal || [])[0] || null)} className="w-full rounded-md border border-az-accent/30 bg-az-accent/10 px-3 py-2 text-[10px] font-bold uppercase tracking-wider text-az-accent transition-all duration-300 hover:scale-[1.02] hover:bg-az-accent/20">
            Replay Most Recent Trade
          </button>
        </Panel>
      </div>
    </motion.div>
  );
}

function TerminalStream({ logs }: { logs: string[] }) {
  const terminalRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    if (!autoScroll || !terminalRef.current) {
      return;
    }
    terminalRef.current.scrollTop = terminalRef.current.scrollHeight;
  }, [logs, autoScroll]);

  const onScroll = () => {
    const el = terminalRef.current;
    if (!el) {
      return;
    }
    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    if (distanceFromBottom > 24 && autoScroll) {
      setAutoScroll(false);
    } else if (distanceFromBottom <= 24 && !autoScroll) {
      setAutoScroll(true);
    }
  };

  const renderClass = (line: string) => {
    if (line.includes("[ERROR]")) return "text-az-loss";
    if (line.includes("[WARNING]") || line.includes("[SL HIT]")) return "text-az-warning";
    if (line.includes("[BET]") || line.includes("DECISION LOCKED")) return "text-az-profit";
    if (line.includes("[GATE]")) return "text-az-accent";
    return "text-az-text-muted";
  };

  return (
    <div className="flex h-full flex-col gap-3 min-h-0">
      <div className="flex shrink-0 items-center justify-between">
        <div className="text-[10px] uppercase tracking-widest text-az-text-muted">{logs.length} entries</div>
        <button
          onClick={() => setAutoScroll((prev) => !prev)}
          className={`flex items-center gap-1.5 rounded-md border px-2 py-1 text-[10px] font-bold transition ${
            autoScroll ? "border-az-profit/30 bg-az-profit/10 text-az-profit" : "border-az-border bg-az-surface text-az-text"
          }`}
        >
          {autoScroll ? <Pause size={12} /> : <Play size={12} />}
          {autoScroll ? "Pause Auto-scroll" : "Resume Auto-scroll"}
        </button>
      </div>
      <div className="relative flex-1 min-h-0">
        <div ref={terminalRef} onScroll={onScroll} className="absolute inset-0 space-y-1 overflow-y-auto rounded bg-az-bg p-3 font-mono text-[10px] leading-relaxed border border-az-border/50">
          {logs.slice(-200).map((log, i) => (
            <div key={`${i}-${log.slice(0, 20)}`} className={renderClass(String(log))}>
              {log}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function ActiveTradesPanel({ trades, currentUnderlying }: { trades: Record<string, any>; currentUnderlying: number }) {
  const entries = Object.entries(trades || {});
  if (entries.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-az-border py-8">
        <Crosshair size={24} className="mb-2 text-az-text-muted" />
        <div className="font-mono text-xs uppercase tracking-wide text-az-text-muted">No Active Positions</div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {entries.map(([slug, trade]) => {
        const direction = trade.decision === "UP" ? "UP" : "DOWN";
        const entryPrice = safeNumber(trade.bought_price);
        const markPrice = safeNumber(trade.mark_price, entryPrice);
        const entryUnderlying = safeNumber(trade.entry_underlying_price, currentUnderlying);
        const liveUnderlying = safeNumber(trade.live_underlying_price, currentUnderlying);
        const strike = safeNumber(trade.strike, entryUnderlying);
        const minUnderlying = Math.min(entryUnderlying, strike, liveUnderlying);
        const maxUnderlying = Math.max(entryUnderlying, strike, liveUnderlying);
        const span = maxUnderlying - minUnderlying || 1;
        const currentPos = clamp(((liveUnderlying - minUnderlying) / span) * 100, 0, 100);
        const strikePos = clamp(((strike - minUnderlying) / span) * 100, 0, 100);
        const tokenMoveCents = (markPrice - entryPrice) * 100;
        const tpToken = safeNumber(trade.tp_token_price, entryPrice + Math.max(0, safeNumber(trade.tp_delta)));
        const slToken = safeNumber(trade.sl_token_price, Math.max(0, entryPrice + safeNumber(trade.sl_delta)));
        const hasBounds =
          (Math.abs(safeNumber(trade.tp_delta)) > 0 || Math.abs(safeNumber(trade.sl_delta)) > 0) &&
          Number.isFinite(tpToken) &&
          Number.isFinite(slToken);
        const tokenMin = Math.min(slToken, entryPrice, markPrice, tpToken);
        const tokenMax = Math.max(slToken, entryPrice, markPrice, tpToken);
        const tokenSpan = tokenMax - tokenMin || 0.0001;
        const tokenMarkPos = clamp(((markPrice - tokenMin) / tokenSpan) * 100, 0, 100);
        const tokenEntryPos = clamp(((entryPrice - tokenMin) / tokenSpan) * 100, 0, 100);
        const tokenTpPos = clamp(((tpToken - tokenMin) / tokenSpan) * 100, 0, 100);
        const tokenSlPos = clamp(((slToken - tokenMin) / tokenSpan) * 100, 0, 100);
        const betSize = safeNumber(trade.bet_size_usd);

        return (
          <div key={slug} className="rounded-xl border border-az-border/50 bg-az-surface/50 p-4">
            <div className="mb-3 flex items-center justify-between">
              <div className="flex flex-col">
                <span className={`mb-1 w-fit rounded px-2 py-0.5 text-[10px] font-black ${direction === "UP" ? "bg-az-profit/20 text-az-profit" : "bg-az-loss/20 text-az-loss"}`}>
                  {direction} {safeNumber(trade.score)}/4 CORE
                </span>
                <span className="font-mono text-[9px] italic text-az-text-muted">{trade.signals?.[0] || "Awaiting signal context..."}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="rounded border border-az-border bg-az-surface-2 px-2 py-1 font-mono text-[10px] font-bold text-az-text">${betSize.toFixed(0)}</span>
                <span className="rounded border border-az-border bg-az-surface-2 px-2 py-1 font-mono text-[10px] text-az-text-muted">{slug.split("-").slice(-2).join("-")}</span>
              </div>
            </div>

            <div className="mb-3 grid grid-cols-1 gap-3 text-xs sm:grid-cols-2 sm:gap-4">
              <div>
                <div className="text-[10px] font-bold uppercase tracking-tighter text-az-text-muted">Entry Price</div>
                <div className="font-mono font-bold text-az-text">${entryPrice.toFixed(3)}</div>
              </div>
              <div>
                <div className="text-[10px] font-bold uppercase tracking-tighter text-az-text-muted">Mark Price</div>
                <div className={`font-mono font-bold ${tokenMoveCents >= 0 ? "text-az-profit" : "text-az-loss"}`}>
                  ${markPrice.toFixed(3)} ({tokenMoveCents >= 0 ? "+" : ""}
                  {tokenMoveCents.toFixed(1)}c)
                </div>
              </div>
            </div>

            <div className="mb-2 flex items-center justify-between text-[10px] text-az-text-muted">
              <span>Entry ${entryUnderlying.toFixed(2)}</span>
              <span>Strike ${strike.toFixed(2)}</span>
              <span>Live ${liveUnderlying.toFixed(2)}</span>
            </div>

            <div className="relative h-2 rounded-full bg-az-bg">
              <div className="absolute top-0 h-2 rounded-full bg-blue-500/60" style={{ width: `${currentPos}%` }} />
              <div className="absolute top-[-4px] h-4 w-[2px] bg-az-accent" style={{ left: `${strikePos}%` }} />
            </div>

            {hasBounds && (
              <div className="mt-3 rounded-lg border border-az-border/70 bg-az-surface-2/70 p-2">
                <div className="mb-1 flex items-center justify-between text-[10px] text-az-text-muted">
                  <span>Token Bounds</span>
                  <span>{safeNumber(trade.seconds_remaining)}s left</span>
                </div>
                <div className="relative h-2 rounded-full bg-az-bg">
                  <div className="absolute inset-y-0 border-l border-az-loss/80" style={{ left: `${tokenSlPos}%` }} />
                  <div className="absolute inset-y-0 border-l border-blue-400/80" style={{ left: `${tokenEntryPos}%` }} />
                  <div className="absolute inset-y-0 border-l border-az-profit/80" style={{ left: `${tokenTpPos}%` }} />
                  <div className="absolute top-[-2px] h-3 w-[2px] bg-amber-300" style={{ left: `${tokenMarkPos}%` }} />
                </div>
                <div className="mt-1 grid grid-cols-4 gap-1 font-mono text-[9px] text-az-text-muted">
                  <span>SL {slToken.toFixed(3)}</span>
                  <span>EN {entryPrice.toFixed(3)}</span>
                  <span>TP {tpToken.toFixed(3)}</span>
                  <span className="text-right">MK {markPrice.toFixed(3)}</span>
                </div>
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function AiLogDisplay({ aiLog }: any) {
  return (
    <div className="space-y-3">
      <div className="rounded-lg border border-zinc-800/50 bg-zinc-950/50 p-3">
        <div className="mb-2 font-mono text-[10px] font-black uppercase tracking-widest text-zinc-600">Prompt</div>
        <div className="whitespace-pre-wrap text-xs italic leading-relaxed text-zinc-400">{aiLog.prompt}</div>
      </div>
      <div className="flex items-center justify-between rounded-lg border border-zinc-800/50 bg-zinc-950/50 p-3">
        <div className="font-mono text-[10px] font-black uppercase tracking-widest text-zinc-600">Response</div>
        <div className={`rounded border px-3 py-1 text-xs font-black ${String(aiLog.response).includes("SKIP") ? "border-red-500/30 bg-red-500/20 text-red-400" : "border-green-500/30 bg-green-500/20 text-green-400"}`}>
          {aiLog.response || "PROCESSING"}
        </div>
      </div>
    </div>
  );
}

function JournalView({ portfolio, setReplayTrade, timeMode }: any) {
  return (
    <motion.div key="journal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }}>
      <Panel title={`Execution History • ${timeMode}`}>
        <TradeJournalTable trades={portfolio?.journal || []} onReplay={setReplayTrade} timeMode={timeMode} />
      </Panel>
    </motion.div>
  );
}

function TradeJournalTable({ trades, onReplay, timeMode }: { trades: any[]; onReplay: (trade: any) => void; timeMode: TimeMode }) {
  const [outcomeFilter, setOutcomeFilter] = useState<"ALL" | "WIN" | "LOSS" | "TIE">("ALL");
  const [sortBy, setSortBy] = useState<"time" | "pnl">("time");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [page, setPage] = useState(1);

  const processed = useMemo(() => {
    const items = (trades || []).map((trade) => {
      const pnl = safeNumber(trade?.PnL);
      const outcome = getOutcomeFromPnl(pnl);
      const date = parseUtcTimestamp(String(trade?.["Timestamp (UTC)"] || ""));
      return {
        trade,
        pnl,
        outcome,
        timestampMs: date ? date.getTime() : 0,
      };
    });

    const filtered = outcomeFilter === "ALL" ? items : items.filter((row) => row.outcome === outcomeFilter);

    filtered.sort((a, b) => {
      if (sortBy === "pnl") {
        return sortDirection === "asc" ? a.pnl - b.pnl : b.pnl - a.pnl;
      }
      return sortDirection === "asc" ? a.timestampMs - b.timestampMs : b.timestampMs - a.timestampMs;
    });

    return filtered;
  }, [trades, outcomeFilter, sortBy, sortDirection]);

  useEffect(() => {
    setPage(1);
  }, [outcomeFilter, sortBy, sortDirection, trades.length]);

  const totalPages = Math.max(1, Math.ceil(processed.length / PAGE_SIZE));
  const safePage = clamp(page, 1, totalPages);
  const pageRows = processed.slice((safePage - 1) * PAGE_SIZE, safePage * PAGE_SIZE);

  const toggleSort = (target: "time" | "pnl") => {
    if (sortBy === target) {
      setSortDirection((prev) => (prev === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(target);
      setSortDirection("desc");
    }
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          {(["ALL", "WIN", "LOSS", "TIE"] as const).map((option) => (
            <button
              key={option}
              onClick={() => setOutcomeFilter(option)}
              className={`rounded-md border px-2.5 py-1 text-[10px] font-bold tracking-wide transition ${
                outcomeFilter === option ? "border-blue-500/40 bg-blue-500/15 text-blue-300" : "border-zinc-700 bg-zinc-900 text-zinc-400 hover:text-zinc-200"
              }`}
            >
              {option}
            </button>
          ))}
        </div>
        <div className="font-mono text-[11px] text-zinc-500">
          {processed.length} trade{processed.length === 1 ? "" : "s"} • Page {safePage}/{totalPages}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead className="border-b border-zinc-800">
            <tr className="text-[10px] font-black uppercase tracking-widest text-zinc-500">
              <th className="pb-4 text-left">
                <button onClick={() => toggleSort("time")} className="inline-flex items-center gap-1 hover:text-zinc-300">
                  Time {sortBy === "time" ? (sortDirection === "asc" ? "↑" : "↓") : ""}
                </button>
              </th>
              <th className="pb-4 text-left">Market</th>
              <th className="pb-4 text-center">Dir</th>
              <th className="pb-4 text-right">Strike</th>
              <th className="pb-4 text-right">Final</th>
              <th className="pb-4 text-right">
                <button onClick={() => toggleSort("pnl")} className="inline-flex items-center gap-1 hover:text-zinc-300">
                  P&L {sortBy === "pnl" ? (sortDirection === "asc" ? "↑" : "↓") : ""}
                </button>
              </th>
              <th className="pb-4 text-center">Action</th>
            </tr>
          </thead>
          <tbody>
            {pageRows.length === 0 ? (
              <tr>
                <td colSpan={7} className="py-10 text-center text-zinc-500">
                  No records match this filter.
                </td>
              </tr>
            ) : (
              pageRows.map((row, idx) => {
                const t = row.trade;
                const pnl = row.pnl;
                return (
                  <tr key={`${idx}-${t?.["Timestamp (UTC)"] || ""}`} className="border-b border-zinc-800/30 transition-colors hover:bg-zinc-800/20">
                    <td className="py-4 font-mono text-zinc-400">{formatTimestamp(String(t?.["Timestamp (UTC)"] || ""), timeMode)}</td>
                    <td className="py-4 font-mono text-[10px] text-zinc-300">{String(t?.["Market Slug"] || "").slice(-20)}</td>
                    <td className="py-4 text-center">
                      <span className={`rounded px-2 py-0.5 text-[10px] font-black ${t?.["AI Decision"] === "UP" ? "bg-green-500/10 text-green-400" : "bg-red-500/10 text-red-400"}`}>{t?.["AI Decision"] || "-"}</span>
                    </td>
                    <td className="py-4 text-right font-mono text-zinc-400">${safeNumber(t?.["Strike Price"]).toFixed(2)}</td>
                    <td className="py-4 text-right font-mono text-zinc-300">${safeNumber(t?.["Final Price"]).toFixed(2)}</td>
                    <td className={`py-4 text-right font-mono font-black ${pnl >= 0 ? "text-emerald-400" : "text-rose-400"}`}>
                      {pnl >= 0 ? "+" : ""}${pnl.toFixed(2)}
                    </td>
                    <td className="py-4 text-center">
                      <button onClick={() => onReplay(t)} className="rounded border border-blue-500/30 bg-blue-500/10 px-2 py-1 text-[9px] font-black tracking-widest text-blue-400 transition-all hover:bg-blue-500/20">
                        REPLAY
                      </button>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-end gap-2">
        <button
          onClick={() => setPage((p) => clamp(p - 1, 1, totalPages))}
          disabled={safePage <= 1}
          className="rounded border border-zinc-700 bg-zinc-900 px-3 py-1 text-[11px] font-bold text-zinc-300 transition enabled:hover:border-zinc-500 enabled:hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Previous
        </button>
        <button
          onClick={() => setPage((p) => clamp(p + 1, 1, totalPages))}
          disabled={safePage >= totalPages}
          className="rounded border border-zinc-700 bg-zinc-900 px-3 py-1 text-[11px] font-bold text-zinc-300 transition enabled:hover:border-zinc-500 enabled:hover:text-white disabled:cursor-not-allowed disabled:opacity-40"
        >
          Next
        </button>
      </div>
    </div>
  );
}

function TooltipLabel({ label, tooltip }: { label: string; tooltip: string }) {
  return (
    <div className="group relative inline-flex items-center gap-1">
      <span className="text-xs font-bold uppercase tracking-tighter text-zinc-500">{label}</span>
      <Info size={12} className="text-zinc-600" />
      <div className="pointer-events-none absolute bottom-full left-0 z-20 mb-2 w-64 rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-[11px] leading-snug text-zinc-300 opacity-0 shadow-lg transition-opacity group-hover:opacity-100">
        {tooltip}
      </div>
    </div>
  );
}

function AnalyticsView({ portfolio, engineHealth, liveData }: { portfolio: any; engineHealth: EngineHealth; liveData: any }) {
  const attr = portfolio?.attribution;
  const aiStatus = String(engineHealth?.ai_status || "ACTIVE").toUpperCase();
  const aiStatusClass = aiStatus === "DEGRADED" ? "text-az-loss" : aiStatus === "SLOW" ? "text-az-warning" : "text-az-profit";
  const drawdownGuard = liveData?.strategy?.drawdown_guard || {};
  const dailyPnl = Array.isArray(portfolio?.daily_pnl) ? portfolio.daily_pnl : [];
  const executionMetrics = Array.isArray(portfolio?.execution_metrics) ? portfolio.execution_metrics : [];
  const calibrationBuckets = Array.isArray(portfolio?.calibration_buckets) ? portfolio.calibration_buckets : [];

  return (
    <motion.div key="analytics" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="space-y-3">
      <div className="grid grid-cols-1 gap-2 md:gap-3 xl:grid-cols-3">
        <Panel title="Risk Profile">
          <div className="space-y-4">
            <div className="flex items-center justify-between gap-3">
              <TooltipLabel label="Value at Risk (95%)" tooltip="Estimated one-day loss threshold that should only be exceeded about 5% of the time." />
              <span className="data-mono text-sm font-black text-[var(--az-loss)]">${safeNumber(portfolio?.metrics?.var_95).toFixed(2)}</span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <TooltipLabel label="Profit Expectancy" tooltip="Average expected profit per trade based on historical outcomes." />
              <span className="data-mono text-sm font-black text-[var(--az-profit)]">${safeNumber(portfolio?.metrics?.expectancy).toFixed(2)}</span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <TooltipLabel label="Optimal Kelly" tooltip="Suggested bankroll percentage to risk per trade from historical win rate and payoff ratio." />
              <span className="data-mono text-sm font-black text-[var(--az-accent)]">{portfolio?.metrics?.kelly_optimal || "0.0%"}</span>
            </div>
            {/* Drawdown Risk Gauge */}
            <DrawdownRiskGauge
              dailyPnl={safeNumber(drawdownGuard?.daily_pnl)}
              maxDailyLossPct={safeNumber(drawdownGuard?.max_daily_loss_pct, 0.2)}
              currentBalance={safeNumber(drawdownGuard?.current_balance, safeNumber(drawdownGuard?.bankroll))}
              regime={String(drawdownGuard?.regime || "NORMAL")}
            />
          </div>
        </Panel>

        <Panel title="Flow Attribution">
          {attr && (safeNumber(attr?.ai_confirmed?.trades) > 0 || safeNumber(attr?.system_only?.trades) > 0) ? (
            <div className="mt-1 space-y-4">
              <div className="flex items-center justify-between">
                <span className="section-label">AI Confirmed ({safeNumber(attr.ai_confirmed.trades)})</span>
                <span className="data-mono text-sm font-black text-[var(--az-accent)]">
                  {safeNumber(attr.ai_confirmed.win_rate).toFixed(1)}%{" "}
                  <span className={safeNumber(attr.ai_confirmed.pnl) >= 0 ? "text-[var(--az-profit)]" : "text-[var(--az-loss)]"}>
                    ({safeNumber(attr.ai_confirmed.pnl) >= 0 ? "+" : ""}${safeNumber(attr.ai_confirmed.pnl).toFixed(2)})
                  </span>
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="section-label">System Only ({safeNumber(attr.system_only.trades)})</span>
                <span className="data-mono text-sm font-black text-purple-400">
                  {safeNumber(attr.system_only.win_rate).toFixed(1)}%{" "}
                  <span className={safeNumber(attr.system_only.pnl) >= 0 ? "text-[var(--az-profit)]" : "text-[var(--az-loss)]"}>
                    ({safeNumber(attr.system_only.pnl) >= 0 ? "+" : ""}${safeNumber(attr.system_only.pnl).toFixed(2)})
                  </span>
                </span>
              </div>
            </div>
          ) : (
            <div className="data-mono text-[10px] uppercase italic text-[var(--az-text-dim)]">Awaiting high-volume data...</div>
          )}
        </Panel>

        <Panel title="Engine Health">
          <div className="space-y-3 text-xs">
            <div className="flex items-center justify-between">
              <span className="text-[var(--az-text-muted)]">WS Latency</span>
              <span className="data-mono text-[var(--az-text-secondary)]">{safeNumber(engineHealth.ws_latency_ms)} ms</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[var(--az-text-muted)]">Feed Stale</span>
              <span className="data-mono text-[var(--az-text-secondary)]">{safeNumber(engineHealth.feed_stale_ms)} ms</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[var(--az-text-muted)]">Memory</span>
              <span className="data-mono text-[var(--az-text-secondary)]">
                {safeNumber(engineHealth.memory_mb).toFixed(1)} MB (peak {safeNumber(engineHealth.memory_peak_mb).toFixed(1)} MB)
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[var(--az-text-muted)]">AI Response</span>
              <span className="data-mono text-[var(--az-text-secondary)]">
                {safeNumber(engineHealth.ai_response_ms).toFixed(0)} ms (EMA {safeNumber(engineHealth.ai_response_ema_ms).toFixed(0)} ms)
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-[var(--az-text-muted)]">API Saturation</span>
              <span className="data-mono text-[var(--az-text-secondary)]">
                {safeNumber(engineHealth.api_saturation_pct).toFixed(1)}% ({safeNumber(engineHealth.api_inflight)}/{safeNumber(engineHealth.api_capacity)})
              </span>
            </div>
            <div className="flex items-center justify-between border-t border-[var(--az-border)] pt-2">
              <span className="text-[var(--az-text-muted)]">ML Status</span>
              <span className={`data-mono font-bold ${aiStatusClass}`}>
                {aiStatus}
                {safeNumber(engineHealth.ai_response_ema_ms) > 5000 ? " (Bottleneck)" : ""}
              </span>
            </div>
          </div>
        </Panel>
      </div>

      {/* Calibration Band */}
      <div className="grid grid-cols-1 gap-5 xl:grid-cols-2">
        <Panel title="Model Calibration Curve">
          <CalibrationBand buckets={calibrationBuckets} />
        </Panel>
        <Panel title="Daily P&L Bars">
          <DailyPnlBarChart rows={dailyPnl} />
        </Panel>
      </div>

      <Panel title="Execution Slippage Scatter (bps)">
        <SlippageScatterPlot rows={executionMetrics} />
      </Panel>
    </motion.div>
  );
}

function DailyPnlBarChart({ rows }: { rows: any[] }) {
  if (!rows || rows.length === 0) {
    return <div className="text-xs text-zinc-500">No daily P&L records yet.</div>;
  }

  const maxAbs = Math.max(1, ...rows.map((r) => Math.abs(safeNumber(r?.pnl))));
  return (
    <div className="space-y-3">
      <div className="grid grid-cols-1 gap-2">
        {rows.map((row) => {
          const pnl = safeNumber(row?.pnl);
          const pct = clamp((Math.abs(pnl) / maxAbs) * 100, 2, 100);
          const isPos = pnl >= 0;
          return (
            <div key={String(row?.date)} className="grid grid-cols-[56px_1fr_90px] items-center gap-3 text-xs">
              <div className="font-mono text-zinc-500">{String(row?.day || "").toUpperCase()}</div>
              <div className="h-5 overflow-hidden rounded border border-zinc-800 bg-zinc-950">
                <div className={`h-full ${isPos ? "bg-emerald-500/70" : "bg-rose-500/70"}`} style={{ width: `${pct}%` }} />
              </div>
              <div className={`text-right font-mono font-bold ${isPos ? "text-emerald-400" : "text-rose-400"}`}>
                {isPos ? "+" : ""}${pnl.toFixed(2)}
              </div>
            </div>
          );
        })}
      </div>
      <div className="grid grid-cols-7 gap-1 border-t border-zinc-800 pt-3">
        {rows.slice(-35).map((row) => {
          const pnl = safeNumber(row?.pnl);
          const absRatio = Math.abs(pnl) / maxAbs;
          const shade = clamp(Math.round(absRatio * 3), 0, 3);
          const cls =
            pnl > 0 ? ["bg-emerald-900/30", "bg-emerald-800/40", "bg-emerald-700/50", "bg-emerald-600/70"][shade] :
            pnl < 0 ? ["bg-rose-900/30", "bg-rose-800/40", "bg-rose-700/50", "bg-rose-600/70"][shade] :
            "bg-zinc-800/40";
          return (
            <div key={`cal-${String(row?.date)}`} className={`h-5 rounded border border-zinc-800 ${cls}`} title={`${row?.date}: ${pnl >= 0 ? "+" : ""}$${pnl.toFixed(2)}`} />
          );
        })}
      </div>
    </div>
  );
}

function SlippageScatterPlot({ rows }: { rows: any[] }) {
  const points = (rows || [])
    .map((row, idx) => ({
      x: idx,
      y: safeNumber(row?.slippage_bps),
      spread: safeNumber(row?.spread_cents),
      slug: String(row?.slug || ""),
      direction: String(row?.direction || ""),
      time: String(row?.timestamp || ""),
    }))
    .slice(-180);

  if (points.length === 0) {
    return <div className="text-xs text-zinc-500">No execution telemetry yet.</div>;
  }

  const width = 900;
  const height = 260;
  const padding = 30;
  const maxAbs = Math.max(1, ...points.map((p) => Math.abs(p.y)));
  const toX = (idx: number) => padding + (idx / Math.max(1, points.length - 1)) * (width - padding * 2);
  const toY = (value: number) => {
    const norm = (value + maxAbs) / (maxAbs * 2);
    return height - padding - norm * (height - padding * 2);
  };
  const zeroY = toY(0);

  return (
    <div className="space-y-3">
      <div className="overflow-x-auto rounded-lg border border-zinc-800 bg-zinc-950/60">
        <svg viewBox={`0 0 ${width} ${height}`} className="h-64 w-full min-w-[680px]">
          <line x1={padding} y1={zeroY} x2={width - padding} y2={zeroY} stroke="#52525b" strokeDasharray="4 4" />
          {points.map((p, idx) => {
            const color = p.y >= 0 ? "#22c55e" : "#ef4444";
            const r = clamp(2 + Math.abs(p.spread) * 0.15, 2, 5);
            return (
              <circle key={`${idx}-${p.slug}`} cx={toX(idx)} cy={toY(p.y)} r={r} fill={color} fillOpacity={0.85}>
                <title>{`${p.time} | ${p.direction} | ${p.slug}\nslippage=${p.y.toFixed(2)} bps | spread=${p.spread.toFixed(2)}c`}</title>
              </circle>
            );
          })}
        </svg>
      </div>
      <div className="flex items-center justify-between text-[11px] text-zinc-500">
        <span>Older</span>
        <span>Slippage range ±{maxAbs.toFixed(1)} bps</span>
        <span>Newest</span>
      </div>
    </div>
  );
}

function NotificationCenter({
  items,
  timeMode,
  onClear,
  onClose,
}: {
  items: ToastItem[];
  timeMode: TimeMode;
  onClear: () => void;
  onClose: () => void;
}) {
  return (
    <div className="absolute right-0 top-10 z-40 w-[min(92vw,24rem)] overflow-hidden rounded-xl border border-zinc-800 bg-zinc-950/95 shadow-2xl backdrop-blur">
      <div className="flex items-center justify-between border-b border-zinc-800 px-3 py-2">
        <div className="text-[11px] font-black uppercase tracking-wide text-zinc-400">Notification History</div>
        <div className="flex items-center gap-2">
          <button onClick={onClear} className="text-[10px] font-bold uppercase tracking-wide text-zinc-500 hover:text-zinc-300">Clear</button>
          <button onClick={onClose} className="rounded p-1 text-zinc-500 hover:text-zinc-200"><X size={12} /></button>
        </div>
      </div>
      <div className="max-h-72 space-y-2 overflow-y-auto p-3">
        {items.length === 0 ? (
          <div className="text-xs text-zinc-500">No recent notifications.</div>
        ) : (
          items.map((item) => (
            <div key={`hist-${item.id}-${item.ts}`} className="rounded-lg border border-zinc-800 bg-zinc-900/60 p-2">
              <div className="flex items-center justify-between gap-2">
                <div className="truncate text-[11px] font-bold text-zinc-200">{item.title}</div>
                <div className="font-mono text-[10px] text-zinc-500">
                  {timeMode === "LOCAL"
                    ? new Date(item.ts).toLocaleTimeString()
                    : new Date(item.ts).toISOString().slice(11, 19) + " UTC"}
                </div>
              </div>
              <div className="mt-1 text-[11px] text-zinc-400">{item.message}</div>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

function AlertSettingsDrawer({
  settings,
  setSettings,
  onClose,
}: {
  settings: AlertSettings;
  setSettings: React.Dispatch<React.SetStateAction<AlertSettings>>;
  onClose: () => void;
}) {
  const toggle = (key: keyof AlertSettings) => {
    if (key === "volume") {
      return;
    }
    setSettings((prev) => ({ ...prev, [key]: !prev[key] }));
  };

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 z-[115] bg-black/60 backdrop-blur-sm" onClick={onClose}>
      <motion.div
        initial={{ x: 380 }}
        animate={{ x: 0 }}
        exit={{ x: 380 }}
        transition={{ type: "spring", stiffness: 280, damping: 32 }}
        onClick={(e) => e.stopPropagation()}
        className="absolute right-0 top-0 h-full w-full max-w-sm border-l border-zinc-800 bg-zinc-950 p-5"
      >
        <div className="mb-5 flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm font-black uppercase tracking-wide text-zinc-200">
            <SlidersHorizontal size={16} />
            Alert Settings
          </div>
          <button onClick={onClose} className="rounded border border-zinc-800 p-1.5 text-zinc-500 hover:text-zinc-200">
            <X size={14} />
          </button>
        </div>

        <div className="space-y-5">
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-xs font-bold uppercase tracking-wide text-zinc-500">Volume</label>
              <span className="font-mono text-xs text-zinc-300">{Math.round(settings.volume * 100)}%</span>
            </div>
            <div className="flex items-center gap-2">
              {settings.volume > 0 ? <Volume2 size={14} className="text-zinc-400" /> : <VolumeX size={14} className="text-zinc-500" />}
              <input
                type="range"
                min={0}
                max={100}
                step={1}
                value={Math.round(settings.volume * 100)}
                onChange={(e) => setSettings((prev) => ({ ...prev, volume: Number(e.target.value) / 100 }))}
                className="w-full accent-blue-500"
              />
            </div>
          </div>

          {[
            ["sound_trade_entered", "Trade Entered"],
            ["sound_stop_loss", "Stop Loss Hit"],
            ["sound_take_profit", "Take Profit Hit"],
            ["sound_circuit_breaker", "AI Circuit Breaker"],
          ].map(([key, label]) => (
            <button
              key={key}
              onClick={() => toggle(key as keyof AlertSettings)}
              className={`flex w-full items-center justify-between rounded-lg border px-3 py-2 text-sm font-semibold transition ${
                settings[key as keyof AlertSettings] ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-200" : "border-zinc-700 bg-zinc-900 text-zinc-400"
              }`}
            >
              <span>{label}</span>
              <span>{settings[key as keyof AlertSettings] ? "ON" : "OFF"}</span>
            </button>
          ))}
        </div>
      </motion.div>
    </motion.div>
  );
}

function ControlModal({ draft, setDraft, onClose, onSave, saving }: { draft: EngineControl; setDraft: (next: EngineControl) => void; onClose: () => void; onSave: () => void; saving: boolean }) {
  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 z-[110] flex items-center justify-center bg-black/80 p-4 backdrop-blur-sm" onClick={onClose}>
      <motion.div initial={{ scale: 0.95, y: 16 }} animate={{ scale: 1, y: 0 }} exit={{ scale: 0.95, y: 16 }} onClick={(e) => e.stopPropagation()} className="w-full max-w-xl rounded-2xl border border-zinc-800 bg-zinc-900 shadow-2xl">
        <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-950 px-6 py-4">
          <div className="flex items-center gap-2 text-sm font-black uppercase tracking-wide text-zinc-300">
            <Settings2 size={16} />
            Engine Controls
          </div>
          <button onClick={onClose} className="rounded-lg border border-zinc-800 bg-zinc-900 p-2 text-zinc-500 transition hover:text-white">
            <X size={16} />
          </button>
        </div>

        <div className="space-y-6 p-6">
          <div className="space-y-2">
            <label className="text-xs font-bold uppercase tracking-wide text-zinc-500">Kill Switch</label>
            <button
              onClick={() => setDraft({ ...draft, kill_switch: !draft.kill_switch })}
              className={`w-full rounded-xl border px-4 py-3 text-left text-sm font-bold transition ${draft.kill_switch ? "border-red-500/40 bg-red-500/10 text-red-300" : "border-zinc-700 bg-zinc-900 text-zinc-300"}`}
            >
              {draft.kill_switch ? "ENABLED: New entries paused" : "DISABLED: Trading allowed"}
            </button>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-bold uppercase tracking-wide text-zinc-500">Trading Mode</label>
            <div className="grid grid-cols-2 gap-2">
              <button onClick={() => setDraft({ ...draft, paper_trading: true })} className={`rounded-xl border px-4 py-3 text-sm font-bold transition ${draft.paper_trading ? "border-amber-500/40 bg-amber-500/10 text-amber-300" : "border-zinc-700 bg-zinc-900 text-zinc-300"}`}>
                PAPER
              </button>
              <button onClick={() => setDraft({ ...draft, paper_trading: false })} className={`rounded-xl border px-4 py-3 text-sm font-bold transition ${!draft.paper_trading ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-300" : "border-zinc-700 bg-zinc-900 text-zinc-300"}`}>
                LIVE
              </button>
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-bold uppercase tracking-wide text-zinc-500">Max Bet %</label>
            <div className="flex items-center gap-3">
              <input type="range" min={0.5} max={20} step={0.5} value={draft.max_trade_pct * 100} onChange={(e) => setDraft({ ...draft, max_trade_pct: Number(e.target.value) / 100 })} className="w-full accent-blue-500" />
              <span className="w-14 text-right font-mono text-sm text-zinc-300">{(draft.max_trade_pct * 100).toFixed(1)}%</span>
            </div>
          </div>

          <div className="space-y-2">
            <label className="text-xs font-bold uppercase tracking-wide text-zinc-500">Daily Drawdown Limit</label>
            <div className="flex items-center gap-3">
              <input type="range" min={1} max={50} step={1} value={draft.max_daily_loss_pct * 100} onChange={(e) => setDraft({ ...draft, max_daily_loss_pct: Number(e.target.value) / 100 })} className="w-full accent-red-500" />
              <span className="w-14 text-right font-mono text-sm text-zinc-300">{(draft.max_daily_loss_pct * 100).toFixed(0)}%</span>
            </div>
          </div>
        </div>

        <div className="flex items-center justify-end gap-2 border-t border-zinc-800 px-6 py-4">
          <button onClick={onClose} className="rounded-lg border border-zinc-700 bg-zinc-900 px-3 py-2 text-xs font-bold text-zinc-300 transition hover:border-zinc-500 hover:text-white">
            Cancel
          </button>
          <button onClick={onSave} disabled={saving} className="rounded-lg border border-blue-500/40 bg-blue-500/20 px-3 py-2 text-xs font-bold text-blue-200 transition hover:bg-blue-500/30 disabled:cursor-not-allowed disabled:opacity-50">
            {saving ? "Saving..." : "Save Controls"}
          </button>
        </div>
      </motion.div>
    </motion.div>
  );
}

function ToastStack({ toasts, onDismiss }: { toasts: ToastItem[]; onDismiss: (id: number) => void }) {
  const getClasses = (level: ToastLevel) => {
    if (level === "success") return "border-emerald-500/40 bg-emerald-500/15 text-emerald-200";
    if (level === "warning") return "border-amber-500/40 bg-amber-500/15 text-amber-200";
    if (level === "error") return "border-red-500/40 bg-red-500/15 text-red-200";
    return "border-blue-500/40 bg-blue-500/15 text-blue-200";
  };

  const getIcon = (level: ToastLevel) => {
    if (level === "success") return <CheckCircle2 size={14} />;
    if (level === "warning") return <AlertTriangle size={14} />;
    if (level === "error") return <AlertTriangle size={14} />;
    return <Clock size={14} />;
  };

  return (
    <div className="pointer-events-none fixed right-3 top-3 z-[140] space-y-2 sm:right-6 sm:top-6">
      <AnimatePresence>
        {toasts.map((toast) => (
          <motion.div key={toast.id} initial={{ opacity: 0, x: 30 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: 30 }} className={`pointer-events-auto flex w-[min(92vw,20rem)] items-start gap-2 rounded-xl border p-3 shadow-xl sm:w-80 ${getClasses(toast.level)}`}>
            <div className="mt-0.5">{getIcon(toast.level)}</div>
            <div className="min-w-0 flex-1">
              <div className="text-xs font-black uppercase tracking-wide">{toast.title}</div>
              <div className="mt-0.5 text-xs opacity-90">{toast.message}</div>
            </div>
            <button onClick={() => onDismiss(toast.id)} className="rounded p-1 opacity-70 transition hover:opacity-100">
              <X size={12} />
            </button>
          </motion.div>
        ))}
      </AnimatePresence>
    </div>
  );
}

function TradeReplayModal({ trade, onClose, timeMode }: { trade: any; onClose: () => void; timeMode: TimeMode }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !trade) return;
    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#71717a" },
      grid: { vertLines: { color: "#18181b" }, horzLines: { color: "#18181b" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#27272a" },
      height: 450,
    });
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e",
      downColor: "#ef4444",
      borderVisible: false,
      wickUpColor: "#22c55e",
      wickDownColor: "#ef4444",
    });
    const strikePrice = parseFloat(trade["Strike Price"]);
    if (!Number.isNaN(strikePrice)) {
      candleSeries.createPriceLine({ price: strikePrice, color: "#a855f7", lineWidth: 2, lineStyle: 2, axisLabelVisible: true, title: "Strike" });
    }

    const loadReplay = async () => {
      try {
        const dateStr = String(trade["Timestamp (UTC)"] || "").replace(" ", "T");
        const timestamp = Math.floor(new Date(dateStr + "Z").getTime() / 1000);
        const res = await fetchBackend(`/api/history/replay?timestamp=${timestamp}`);
        const data = await res.json();
        if (data.history?.length > 0) {
          candleSeries.setData(
            dedupePriceBars(
              data.history
                .map((item: unknown) => normalizePriceBar(item))
                .filter((bar: PriceBar | null): bar is PriceBar => Boolean(bar)),
            ) as any,
          );
          (candleSeries as any).setMarkers([{ time: timestamp as any, position: trade["AI Decision"] === "UP" ? "belowBar" : "aboveBar", color: trade["AI Decision"] === "UP" ? "#22c55e" : "#ef4444", shape: trade["AI Decision"] === "UP" ? "arrowUp" : "arrowDown", text: `${trade["AI Decision"]}` }]);
          chart.timeScale().fitContent();
        }
      } catch (error) {
        console.error("Replay error:", error);
      }
    };
    loadReplay();
    return () => chart.remove();
  }, [trade]);

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 z-[120] flex items-center justify-center bg-black/90 p-4 backdrop-blur-md" onClick={onClose}>
      <motion.div initial={{ scale: 0.9, y: 20 }} animate={{ scale: 1, y: 0 }} onClick={(e) => e.stopPropagation()} className="w-full max-w-5xl overflow-hidden rounded-2xl border border-zinc-800 bg-zinc-900 shadow-2xl">
        <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-950 p-6">
          <div>
            <div className="mb-2 font-mono text-xs font-bold uppercase tracking-widest text-zinc-500">Replaying Execution • {formatTimestamp(String(trade["Timestamp (UTC)"] || ""), timeMode)}</div>
            <div className="flex items-center gap-3">
              <span className="text-xl font-black text-white">{trade["Market Slug"]}</span>
              <span className={`rounded-lg px-3 py-1 text-sm font-black ${trade["AI Decision"] === "UP" ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>{trade["AI Decision"]}</span>
            </div>
          </div>
          <button onClick={onClose} className="rounded-xl border border-zinc-800 bg-zinc-900 p-3 text-zinc-500 transition hover:text-white">
            <X size={20} />
          </button>
        </div>
        <div className="p-6">
          <div ref={chartContainerRef} className="h-[450px] w-full" />
        </div>
      </motion.div>
    </motion.div>
  );
}
