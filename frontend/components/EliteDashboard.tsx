"use client";

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  AlertTriangle,
  BarChart3,
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
  Target,
  TrendingDown,
  TrendingUp,
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
import GrowthChart from "../components/GrowthChart";
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
};

const PAGE_SIZE = 20;
const WS_RECONNECT_SECONDS = 3;
const TOAST_DURATION_MS = 4200;
const METRICS_POLL_MS = 10_000;
const HEALTH_POLL_MS = 5_000;
const CONTROL_POLL_MS = 15_000;

const DEFAULT_ENGINE_CONTROL: EngineControl = {
  kill_switch: false,
  paper_trading: true,
  max_trade_pct: 0.05,
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

const DEFAULT_LIVE_DATA = {
  price: 0,
  vwap: 0,
  active_trades: {},
  candle: null,
  is_paper: true,
  logs: [],
  regime: "UNKNOWN",
  atr: 0,
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
  const [liveData, setLiveData] = useState<any>(DEFAULT_LIVE_DATA);
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

  const [toasts, setToasts] = useState<ToastItem[]>([]);

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

  useEffect(() => {
    if (typeof window !== "undefined") {
      window.localStorage.setItem("elite-time-mode", timeMode);
    }
  }, [timeMode]);

  const addToast = useCallback((level: ToastLevel, title: string, message: string) => {
    const id = toastIdRef.current++;
    setToasts((prev) => [...prev, { id, level, title, message }]);
    window.setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, TOAST_DURATION_MS);
  }, []);

  const playNotification = useCallback(() => {
    const audio = new Audio("https://assets.mixkit.co/active_storage/sfx/2358/2358-preview.mp3");
    audio.volume = 0.25;
    audio.play().catch(() => {
      // Ignore autoplay restrictions.
    });
  }, []);

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
        const data = JSON.parse(event.data);
        if (data.candle && data.candle.o !== undefined) {
          const k = data.candle;
          data.candle = {
            open: parseFloat(k.o),
            high: parseFloat(k.h),
            low: parseFloat(k.l),
            close: parseFloat(k.c),
            volume: parseFloat(k.v),
            ...(k.t ? { time: k.t } : {}),
          };
        }

        if (data.price && lastPriceRef.current) {
          const change = ((data.price - lastPriceRef.current) / lastPriceRef.current) * 100;
          setPriceChange(change);
        }
        lastPriceRef.current = safeNumber(data.price);

        setLiveData(data);
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
      const res = await fetch(`${getHttpUrl()}/api/metrics`);
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
      const res = await fetch(`${getHttpUrl()}/api/engine/health`);
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
        const res = await fetch(`${getHttpUrl()}/api/engine/control`);
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
          max_trade_pct: safeNumber(data?.max_trade_pct, 0.05),
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

  useEffect(() => {
    fetchMetrics();
    fetchEngineHealth();
    fetchEngineControl(true);

    const metricsInterval = setInterval(fetchMetrics, METRICS_POLL_MS);
    const healthInterval = setInterval(fetchEngineHealth, HEALTH_POLL_MS);
    const controlInterval = setInterval(() => fetchEngineControl(false), CONTROL_POLL_MS);
    return () => {
      clearInterval(metricsInterval);
      clearInterval(healthInterval);
      clearInterval(controlInterval);
    };
  }, [fetchMetrics, fetchEngineHealth, fetchEngineControl]);

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
      playNotification();
    } else if (latestLog.includes("[SL HIT]") || latestLog.includes("STOP_LOSS_CONFIRMED")) {
      addToast("error", "Stop Loss Hit", "A position was exited by stop loss.");
      playNotification();
    } else if (latestLog.includes("[TP HIT]")) {
      addToast("info", "Take Profit Hit", "A position reached take-profit conditions.");
    } else if (latestLog.includes("[KILL SWITCH]")) {
      addToast("warning", "Kill Switch Active", "New trade entries are paused.");
    }
  }, [liveData.logs, addToast, playNotification]);

  const saveControls = useCallback(async () => {
    if (!controlSupportedRef.current) {
      addToast("error", "Controls Unsupported", "Current backend does not support runtime control updates.");
      return;
    }
    setSavingControl(true);
    try {
      const res = await fetch(`${getHttpUrl()}/api/engine/control`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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
  const isPnlPositive = safeNumber(metrics?.current_pnl) >= 0;
  const isPriceUp = priceChange > 0;
  const activePositions = Object.keys(liveData.active_trades || {}).length;

  return (
    <div className="min-h-screen bg-gradient-to-br from-zinc-950 via-zinc-900 to-zinc-950 text-zinc-100 font-sans selection:bg-blue-500/30">
      <nav className="fixed left-0 top-0 z-50 flex h-full w-20 flex-col items-center gap-8 border-r border-zinc-800/50 bg-zinc-950/80 py-8 backdrop-blur-xl shadow-2xl">
        <motion.div
          className="flex h-12 w-12 items-center justify-center rounded-2xl bg-gradient-to-br from-blue-600 to-blue-500 text-sm font-black text-white shadow-lg shadow-blue-500/30"
          whileHover={{ scale: 1.05, rotate: 5 }}
          whileTap={{ scale: 0.95 }}
        >
          OZ
        </motion.div>
        <NavIcon icon={<LayoutDashboard size={20} />} active={activeTab === "terminal"} onClick={() => setActiveTab("terminal")} label="Terminal" />
        <NavIcon icon={<BookOpen size={20} />} active={activeTab === "journal"} onClick={() => setActiveTab("journal")} label="Journal" />
        <NavIcon icon={<BarChart3 size={20} />} active={activeTab === "analytics"} onClick={() => setActiveTab("analytics")} label="Analytics" />
      </nav>

      <main className="px-10 py-10 pl-32">
        <header className="mb-10 flex flex-wrap items-start justify-between gap-5">
          <motion.div initial={{ opacity: 0, y: -20 }} animate={{ opacity: 1, y: 0 }} className="space-y-4">
            <div>
              <div className="mb-2 flex items-center gap-3">
                <h1 className="text-xs font-bold uppercase tracking-wider text-zinc-500">Portfolio Value</h1>
                {liveData.is_paper && (
                  <div className="flex items-center gap-1.5 rounded-md border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-tight text-amber-400">
                    <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-amber-400" />
                    Paper Trading
                  </div>
                )}
              </div>

              {metricsLoading ? (
                <SkeletonBox className="h-14 w-80" />
              ) : (
                <motion.div
                  className={`text-5xl font-black tabular-nums ${isPnlPositive ? "text-white" : "text-red-400"}`}
                  key={metrics?.current_pnl}
                  initial={{ scale: 1.03, opacity: 0.85 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ type: "spring", stiffness: 280, damping: 22 }}
                >
                  ${safeNumber(metrics?.current_pnl).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </motion.div>
              )}
            </div>

            <div className="flex items-center gap-4 text-sm">
              <span className="font-medium text-zinc-500">BTC/USD</span>
              {liveLoading ? (
                <SkeletonBox className="h-6 w-28" />
              ) : (
                <>
                  <motion.span className={`font-bold tabular-nums ${isPriceUp ? "text-green-400" : "text-red-400"}`} key={liveData.price} initial={{ scale: 1.04 }} animate={{ scale: 1 }}>
                    ${safeNumber(liveData.price).toLocaleString()}
                  </motion.span>
                  {Math.abs(priceChange) > 0 && (
                    <span className={`text-xs font-mono ${isPriceUp ? "text-green-400" : "text-red-400"}`}>
                      {isPriceUp ? "+" : ""}
                      {priceChange.toFixed(3)}%
                    </span>
                  )}
                </>
              )}
            </div>
          </motion.div>

          <div className="flex flex-col items-end gap-3">
            <div className="flex items-center gap-2">
              <button
                onClick={() => setTimeMode((prev) => (prev === "UTC" ? "LOCAL" : "UTC"))}
                className="rounded-lg border border-zinc-700/70 bg-zinc-900/70 px-3 py-1.5 text-[11px] font-bold tracking-wide text-zinc-300 transition hover:border-zinc-500 hover:text-white"
              >
                {timeMode === "UTC" ? "UTC" : "LOCAL"}
              </button>
              <button
                onClick={() => {
                  setControlDraft(engineControl);
                  setControlDirty(false);
                  setShowControls(true);
                }}
                className="flex items-center gap-1.5 rounded-lg border border-zinc-700/70 bg-zinc-900/70 px-3 py-1.5 text-[11px] font-bold tracking-wide text-zinc-300 transition hover:border-zinc-500 hover:text-white"
              >
                <Settings2 size={14} />
                Controls
              </button>
            </div>
            <StatusBadge status={wsStatus} reconnectIn={reconnectIn} onReconnect={forceReconnect} />
            <RegimeBadge regime={liveData.regime} atr={safeNumber(liveData.atr)} />
          </div>
        </header>

        <div className="mb-8 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
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
          {activeTab === "analytics" && <AnalyticsView portfolio={portfolio} engineHealth={engineHealth} />}
        </AnimatePresence>
      </main>

      <AnimatePresence>
        {replayTrade && <TradeReplayModal trade={replayTrade} onClose={() => setReplayTrade(null)} timeMode={timeMode} />}
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
      className={`group relative flex h-14 w-14 items-center justify-center rounded-xl transition-all ${
        active ? "bg-blue-600 text-white shadow-lg shadow-blue-500/30" : "bg-zinc-900/50 text-zinc-500 hover:bg-zinc-900 hover:text-zinc-300"
      }`}
      whileHover={{ scale: 1.05 }}
      whileTap={{ scale: 0.95 }}
    >
      {icon}
      <div className="pointer-events-none absolute left-full ml-4 whitespace-nowrap rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-1.5 text-xs font-medium opacity-0 transition-opacity group-hover:opacity-100">
        {label}
      </div>
    </motion.button>
  );
}

function StatusBadge({ status, reconnectIn, onReconnect }: { status: WsStatus; reconnectIn: number; onReconnect: () => void }) {
  const isLive = status === "LIVE";
  const icon = isLive ? <Wifi size={14} /> : <WifiOff size={14} />;
  return (
    <div className="flex items-center gap-2">
      <motion.div
        className={`flex items-center gap-2 rounded-full border px-4 py-2 text-xs font-bold tracking-wide ${
          isLive ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400" : "border-red-500/30 bg-red-500/10 text-red-400"
        }`}
        animate={{
          boxShadow: isLive ? ["0 0 0 0 rgba(16, 185, 129, 0.4)", "0 0 0 8px rgba(16, 185, 129, 0)"] : "none",
        }}
        transition={{ duration: 2, repeat: Infinity }}
      >
        {icon} {status}
      </motion.div>

      {!isLive && (
        <button
          onClick={onReconnect}
          className="flex items-center gap-1.5 rounded-full border border-zinc-700 bg-zinc-900/70 px-3 py-2 text-[11px] font-bold text-zinc-300 transition hover:border-zinc-500 hover:text-white"
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
  const trendColors = { up: "text-green-400", down: "text-red-400", neutral: "text-zinc-400" } as const;
  return (
    <motion.div
      className="group relative overflow-hidden rounded-2xl border border-zinc-800/50 bg-zinc-900/50 p-5 backdrop-blur-sm transition-all hover:border-zinc-700/50"
      whileHover={{ scale: 1.02 }}
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
    >
      <div className="absolute inset-0 bg-gradient-to-br from-blue-600/5 via-transparent to-transparent opacity-0 transition-opacity group-hover:opacity-100" />
      <div className="relative z-10">
        <div className="mb-3 flex items-start justify-between">
          <div className="rounded-lg bg-zinc-900/80 p-2">{icon}</div>
        </div>
        <div className={`mb-1 text-2xl font-black ${trendColors[trend as keyof typeof trendColors]}`}>{value}</div>
        <div className="mb-1 text-xs font-medium uppercase tracking-wide text-zinc-500">{label}</div>
        {subtitle && <div className="font-mono text-[10px] text-zinc-600">{subtitle}</div>}
      </div>
    </motion.div>
  );
}

function SkeletonBox({ className = "" }: { className?: string }) {
  return <div className={`animate-pulse rounded-lg bg-zinc-800/70 ${className}`} />;
}

function SkeletonCard() {
  return (
    <div className="rounded-2xl border border-zinc-800/50 bg-zinc-900/50 p-5">
      <SkeletonBox className="mb-4 h-8 w-8" />
      <SkeletonBox className="mb-2 h-8 w-20" />
      <SkeletonBox className="h-3 w-32" />
    </div>
  );
}

function Panel({ title, children, className = "" }: { title: string; children: React.ReactNode; className?: string }) {
  return (
    <motion.div className={`overflow-hidden rounded-2xl border border-zinc-800/50 bg-zinc-900/30 backdrop-blur-sm ${className}`} initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
      <div className="border-b border-zinc-800/50 bg-zinc-900/50 px-5 py-3">
        <h3 className="text-xs font-bold uppercase tracking-wider text-zinc-400">{title}</h3>
      </div>
      <div className="p-5">{children}</div>
    </motion.div>
  );
}

function TerminalView({ liveData, portfolio, setReplayTrade, liveLoading, timeMode }: any) {
  return (
    <motion.div key="terminal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="grid grid-cols-12 gap-6">
      <div className="col-span-12 space-y-6 xl:col-span-8">
        <Panel title="Live Price Action • 15m Candles">
          {liveLoading ? (
            <div className="grid grid-cols-1 gap-3">
              <SkeletonBox className="h-64 w-full" />
              <SkeletonBox className="h-4 w-44" />
            </div>
          ) : liveData.candle ? (
            <PriceChart candle={liveData.candle} vwap={liveData.vwap} />
          ) : (
            <div className="flex h-64 items-center justify-center rounded-xl border border-dashed border-zinc-800">
              <div className="text-center text-xs uppercase tracking-wider text-zinc-500">Awaiting stream...</div>
            </div>
          )}
        </Panel>

        <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
          <Panel title="Equity Curve">{portfolio ? <EquityCurve data={portfolio?.equity_curve || []} /> : <SkeletonBox className="h-48 w-full" />}</Panel>
          <Panel title="Monte Carlo Projection">{portfolio ? <GrowthChart data={portfolio?.projections?.paths || []} /> : <SkeletonBox className="h-48 w-full" />}</Panel>
        </div>

        <Panel title={`Performance Heatmap • ${timeMode}`}>{portfolio ? <WinHeatmap data={portfolio?.heatmap || []} /> : <SkeletonBox className="h-36 w-full" />}</Panel>
        <Panel title="Live Engine Terminal">
          <TerminalStream logs={liveData.logs || []} />
        </Panel>
      </div>

      <div className="col-span-12 space-y-6 xl:col-span-4">
        <Panel title="Active Positions">
          <ActiveTradesPanel trades={liveData.active_trades} currentUnderlying={safeNumber(liveData.price)} />
        </Panel>
        <Panel title="AI Context">{portfolio ? <AiInsights insights={portfolio?.insights || []} /> : <SkeletonBox className="h-32 w-full" />}</Panel>
        {liveData.ai_log && (
          <Panel title="Latest AI Reasoning">
            <AiLogDisplay aiLog={liveData.ai_log} />
          </Panel>
        )}
        <Panel title="Replay Shortcut">
          <button onClick={() => setReplayTrade((portfolio?.journal || [])[0] || null)} className="rounded-lg border border-blue-500/40 bg-blue-500/10 px-3 py-2 text-xs font-bold text-blue-300 transition hover:bg-blue-500/20">
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
    if (line.includes("[ERROR]")) return "text-red-400";
    if (line.includes("[WARNING]") || line.includes("[SL HIT]")) return "text-amber-400";
    if (line.includes("[BET]") || line.includes("DECISION LOCKED")) return "text-green-400";
    if (line.includes("[GATE]")) return "text-blue-400";
    return "text-zinc-500";
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="text-[10px] uppercase tracking-widest text-zinc-500">{logs.length} entries</div>
        <button
          onClick={() => setAutoScroll((prev) => !prev)}
          className={`flex items-center gap-1.5 rounded-md border px-2 py-1 text-[10px] font-bold transition ${
            autoScroll ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-400" : "border-zinc-700 bg-zinc-900 text-zinc-300"
          }`}
        >
          {autoScroll ? <Pause size={12} /> : <Play size={12} />}
          {autoScroll ? "Pause Auto-scroll" : "Resume Auto-scroll"}
        </button>
      </div>
      <div ref={terminalRef} onScroll={onScroll} className="h-96 space-y-1 overflow-y-auto rounded-xl border border-zinc-800/50 bg-zinc-950/80 p-4 font-mono text-[10px] leading-relaxed">
        {logs.slice(-200).map((log, i) => (
          <div key={`${i}-${log.slice(0, 20)}`} className={renderClass(String(log))}>
            {log}
          </div>
        ))}
      </div>
    </div>
  );
}

function ActiveTradesPanel({ trades, currentUnderlying }: { trades: Record<string, any>; currentUnderlying: number }) {
  const entries = Object.entries(trades || {});
  if (entries.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center rounded-xl border border-dashed border-zinc-800 py-8">
        <Crosshair size={24} className="mb-2 text-zinc-700" />
        <div className="font-mono text-xs uppercase tracking-wide text-zinc-500">No Active Positions</div>
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

        return (
          <div key={slug} className="rounded-xl border border-zinc-800/50 bg-zinc-950/50 p-4">
            <div className="mb-3 flex items-center justify-between">
              <div className="flex flex-col">
                <span className={`mb-1 w-fit rounded px-2 py-0.5 text-[10px] font-black ${direction === "UP" ? "bg-green-500/20 text-green-400" : "bg-red-500/20 text-red-400"}`}>
                  {direction} {safeNumber(trade.score)}/4 CORE
                </span>
                <span className="font-mono text-[9px] italic text-zinc-600">{trade.signals?.[0] || "Awaiting signal context..."}</span>
              </div>
              <span className="rounded border border-zinc-800 bg-zinc-900 px-2 py-1 font-mono text-[10px] text-zinc-500">{slug.split("-").slice(-2).join("-")}</span>
            </div>

            <div className="mb-3 grid grid-cols-2 gap-4 text-xs">
              <div>
                <div className="text-[10px] font-bold uppercase tracking-tighter text-zinc-600">Entry Price</div>
                <div className="font-mono font-bold text-zinc-300">${entryPrice.toFixed(3)}</div>
              </div>
              <div>
                <div className="text-[10px] font-bold uppercase tracking-tighter text-zinc-600">Mark Price</div>
                <div className={`font-mono font-bold ${tokenMoveCents >= 0 ? "text-green-400" : "text-red-400"}`}>
                  ${markPrice.toFixed(3)} ({tokenMoveCents >= 0 ? "+" : ""}
                  {tokenMoveCents.toFixed(1)}c)
                </div>
              </div>
            </div>

            <div className="mb-2 flex items-center justify-between text-[10px] text-zinc-500">
              <span>Entry ${entryUnderlying.toFixed(2)}</span>
              <span>Strike ${strike.toFixed(2)}</span>
              <span>Live ${liveUnderlying.toFixed(2)}</span>
            </div>

            <div className="relative h-2 rounded-full bg-zinc-800">
              <div className="absolute top-0 h-2 rounded-full bg-blue-500/60" style={{ width: `${currentPos}%` }} />
              <div className="absolute top-[-4px] h-4 w-[2px] bg-fuchsia-400" style={{ left: `${strikePos}%` }} />
            </div>
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

function AnalyticsView({ portfolio, engineHealth }: { portfolio: any; engineHealth: EngineHealth }) {
  const attr = portfolio?.attribution;
  const aiStatus = String(engineHealth?.ai_status || "ACTIVE").toUpperCase();
  const aiStatusClass = aiStatus === "DEGRADED" ? "text-red-400" : aiStatus === "SLOW" ? "text-amber-400" : "text-emerald-400";

  return (
    <motion.div key="analytics" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="space-y-6">
      <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
        <Panel title="Risk Profile">
          <div className="space-y-4">
            <div className="flex items-center justify-between gap-3">
              <TooltipLabel label="Value at Risk (95%)" tooltip="Estimated one-day loss threshold that should only be exceeded about 5% of the time." />
              <span className="text-sm font-black text-rose-400">${safeNumber(portfolio?.metrics?.var_95).toFixed(2)}</span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <TooltipLabel label="Profit Expectancy" tooltip="Average expected profit per trade based on historical outcomes." />
              <span className="text-sm font-black text-emerald-400">${safeNumber(portfolio?.metrics?.expectancy).toFixed(2)}</span>
            </div>
            <div className="flex items-center justify-between gap-3">
              <TooltipLabel label="Optimal Kelly" tooltip="Suggested bankroll percentage to risk per trade from historical win rate and payoff ratio." />
              <span className="text-sm font-black text-blue-400">{portfolio?.metrics?.kelly_optimal || "0.0%"}</span>
            </div>
          </div>
        </Panel>

        <Panel title="Flow Attribution">
          {attr && (safeNumber(attr?.ai_confirmed?.trades) > 0 || safeNumber(attr?.system_only?.trades) > 0) ? (
            <div className="mt-1 space-y-4">
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold uppercase tracking-tighter text-zinc-500">AI Confirmed ({safeNumber(attr.ai_confirmed.trades)})</span>
                <span className="text-sm font-black text-blue-400">
                  {safeNumber(attr.ai_confirmed.win_rate).toFixed(1)}%{" "}
                  <span className={safeNumber(attr.ai_confirmed.pnl) >= 0 ? "text-emerald-400" : "text-rose-400"}>
                    ({safeNumber(attr.ai_confirmed.pnl) >= 0 ? "+" : ""}${safeNumber(attr.ai_confirmed.pnl).toFixed(2)})
                  </span>
                </span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-xs font-bold uppercase tracking-tighter text-zinc-500">System Only ({safeNumber(attr.system_only.trades)})</span>
                <span className="text-sm font-black text-purple-400">
                  {safeNumber(attr.system_only.win_rate).toFixed(1)}%{" "}
                  <span className={safeNumber(attr.system_only.pnl) >= 0 ? "text-emerald-400" : "text-rose-400"}>
                    ({safeNumber(attr.system_only.pnl) >= 0 ? "+" : ""}${safeNumber(attr.system_only.pnl).toFixed(2)})
                  </span>
                </span>
              </div>
            </div>
          ) : (
            <div className="font-mono text-[10px] uppercase italic text-zinc-600">Awaiting high-volume data...</div>
          )}
        </Panel>

        <Panel title="Engine Health">
          <div className="space-y-3 text-xs">
            <div className="flex items-center justify-between">
              <span className="text-zinc-500">WS Latency</span>
              <span className="font-mono text-zinc-300">{safeNumber(engineHealth.ws_latency_ms)} ms</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-zinc-500">Feed Stale</span>
              <span className="font-mono text-zinc-300">{safeNumber(engineHealth.feed_stale_ms)} ms</span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-zinc-500">Memory</span>
              <span className="font-mono text-zinc-300">
                {safeNumber(engineHealth.memory_mb).toFixed(1)} MB (peak {safeNumber(engineHealth.memory_peak_mb).toFixed(1)} MB)
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-zinc-500">AI Response</span>
              <span className="font-mono text-zinc-300">
                {safeNumber(engineHealth.ai_response_ms).toFixed(0)} ms (EMA {safeNumber(engineHealth.ai_response_ema_ms).toFixed(0)} ms)
              </span>
            </div>
            <div className="flex items-center justify-between">
              <span className="text-zinc-500">API Saturation</span>
              <span className="font-mono text-zinc-300">
                {safeNumber(engineHealth.api_saturation_pct).toFixed(1)}% ({safeNumber(engineHealth.api_inflight)}/{safeNumber(engineHealth.api_capacity)})
              </span>
            </div>
            <div className="flex items-center justify-between border-t border-zinc-800 pt-2">
              <span className="text-zinc-500">ML Status</span>
              <span className={`font-mono font-bold ${aiStatusClass}`}>
                {aiStatus}
                {safeNumber(engineHealth.ai_response_ema_ms) > 5000 ? " (Bottleneck)" : ""}
              </span>
            </div>
          </div>
        </Panel>
      </div>
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
    <div className="pointer-events-none fixed right-6 top-6 z-[140] space-y-2">
      <AnimatePresence>
        {toasts.map((toast) => (
          <motion.div key={toast.id} initial={{ opacity: 0, x: 30 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: 30 }} className={`pointer-events-auto flex w-80 items-start gap-2 rounded-xl border p-3 shadow-xl ${getClasses(toast.level)}`}>
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
        const res = await fetch(`${getHttpUrl()}/api/history/replay?timestamp=${timestamp}`);
        const data = await res.json();
        if (data.history?.length > 0) {
          candleSeries.setData(data.history.sort((a: any, b: any) => a.time - b.time));
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
