"use client";

import React, { useState, useEffect, useRef } from "react";
import { 
  LayoutDashboard, BookOpen, Activity, TrendingUp, TrendingDown,
  ShieldAlert, Crosshair, Zap, Target, LineChart, X, Clock,
  AlertTriangle, CheckCircle2, Wifi, WifiOff, BarChart3,
  DollarSign, Percent, ArrowUpRight, ArrowDownRight
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { createChart, CandlestickSeries, ColorType } from "lightweight-charts";
import PriceChart from "../components/PriceChart";
import EquityCurve from "../components/EquityCurve";
import WinHeatmap from "../components/WinHeatmap";
import AiInsights from "../components/AiInsights";
import GrowthChart from "../components/GrowthChart";
import { getWsUrl, getHttpUrl } from "../config";

export default function EliteDashboard() {
  const [activeTab, setActiveTab] = useState("terminal");
  const [portfolio, setPortfolio] = useState<any>(null);
  const [liveData, setLiveData] = useState<any>({ 
    price: 0, vwap: 0, active_trades: {}, candle: null, 
    is_paper: false, logs: [], regime: "UNKNOWN", atr: 0 
  });
  const [wsStatus, setWsStatus] = useState("Connecting...");
  const [replayTrade, setReplayTrade] = useState<any>(null);
  const [priceChange, setPriceChange] = useState(0);
  const lastPriceRef = useRef(0);

  // --- IMPROVEMENT: AUDIO HEARTBEAT SYSTEM ---
  useEffect(() => {
    if (liveData.logs && liveData.logs.length > 0) {
      const latestLog = liveData.logs[liveData.logs.length - 1];
      
      // Play a subtle notification sound on trade execution or locking
      if (latestLog.includes("🎯 BET PLACED") || latestLog.includes("DECISION LOCKED")) {
        const audio = new Audio('https://assets.mixkit.co/active_storage/sfx/2358/2358-preview.mp3');
        audio.volume = 0.35;
        audio.play().catch(() => console.log("Audio interaction required by browser"));
      }
    }
  }, [liveData.logs]);

  // Enhanced WebSocket Feed
  useEffect(() => {
    const ws = new WebSocket(getWsUrl());
    
    ws.onopen = () => setWsStatus("LIVE");
    ws.onclose = () => setWsStatus("OFFLINE");
    ws.onerror = () => setWsStatus("ERROR");
    
    ws.onmessage = (e) => {
      const data = JSON.parse(e.data);

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
      lastPriceRef.current = data.price;

      setLiveData((prev: any) => {
        const changed = JSON.stringify(data.logs) !== JSON.stringify(prev.logs) ||
                       JSON.stringify(data.ai_log) !== JSON.stringify(prev.ai_log) ||
                       data.price !== prev.price;
        return changed ? data : prev;
      });
    };

    return () => ws.close();
  }, []);

  // Metrics Polling
  useEffect(() => {
    const fetchStats = async () => {
      try {
        const res = await fetch(`${getHttpUrl()}/api/metrics`);
        const data = await res.json();
        if (!data.error) setPortfolio(data);
      } catch (e) { 
        console.error("Metrics sync error:", e); 
      }
    };
    fetchStats();
    const interval = setInterval(fetchStats, 30000);
    return () => clearInterval(interval);
  }, []);

  const metrics = portfolio?.metrics;
  const isPositive = (metrics?.current_pnl ?? 0) >= 0;
  const isPriceUp = priceChange > 0;

  return (
    <div className="min-h-screen bg-gradient-to-br from-zinc-950 via-zinc-900 to-zinc-950 text-zinc-100 font-sans selection:bg-blue-500/30">
      {/* Side Navigation */}
      <nav className="fixed left-0 top-0 h-full w-20 bg-zinc-950/80 backdrop-blur-xl border-r border-zinc-800/50 flex flex-col items-center py-8 gap-8 z-50 shadow-2xl">
        <motion.div 
          className="w-12 h-12 bg-gradient-to-br from-blue-600 to-blue-500 rounded-2xl flex items-center justify-center font-black text-white shadow-lg shadow-blue-500/30 text-sm"
          whileHover={{ scale: 1.05, rotate: 5 }}
          whileTap={{ scale: 0.95 }}
        >
          Ω
        </motion.div>
        <NavIcon icon={<LayoutDashboard size={20} />} active={activeTab === "terminal"} onClick={() => setActiveTab("terminal")} label="Terminal" />
        <NavIcon icon={<BookOpen size={20} />} active={activeTab === "journal"} onClick={() => setActiveTab("journal")} label="Journal" />
        <NavIcon icon={<BarChart3 size={20} />} active={activeTab === "analytics"} onClick={() => setActiveTab("analytics")} label="Analytics" />
      </nav>

      <main className="pl-32 pr-10 py-10">
        <header className="flex justify-between items-start mb-10">
          <motion.div 
            initial={{ opacity: 0, y: -20 }} 
            animate={{ opacity: 1, y: 0 }}
            className="space-y-4"
          >
            {/* Portfolio Value */}
            <div>
              <div className="flex items-center gap-3 mb-2">
                <h1 className="text-xs font-bold text-zinc-500 uppercase tracking-wider">Portfolio Value</h1>
                {liveData.is_paper && (
                  <motion.div 
                    initial={{ opacity: 0, scale: 0.8 }}
                    animate={{ opacity: 1, scale: 1 }}
                    className="px-2 py-0.5 rounded-md bg-amber-500/10 border border-amber-500/30 text-amber-400 text-[10px] font-bold tracking-tight uppercase flex items-center gap-1.5"
                  >
                    <div className="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse" />
                    Paper Trading
                  </motion.div>
                )}
              </div>
              
              <div className="flex items-baseline gap-4">
                <motion.div 
                  className={`text-5xl font-black tabular-nums ${isPositive ? 'text-white' : 'text-red-400'}`}
                  key={metrics?.current_pnl}
                  initial={{ scale: 1.05, opacity: 0.8 }}
                  animate={{ scale: 1, opacity: 1 }}
                  transition={{ type: "spring", stiffness: 300, damping: 20 }}
                >
                  ${(metrics?.current_pnl ?? 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
                </motion.div>
              </div>
            </div>

            {/* Live BTC Ticker */}
            <div className="flex items-center gap-4 text-sm">
              <span className="text-zinc-500 font-medium">BTC/USD</span>
              <motion.span 
                className={`font-bold tabular-nums ${isPriceUp ? 'text-green-400' : 'text-red-400'}`}
                key={liveData.price}
                initial={{ scale: 1.05 }}
                animate={{ scale: 1 }}
              >
                ${liveData.price.toLocaleString()}
              </motion.span>
              {Math.abs(priceChange) > 0 && (
                <span className={`text-xs font-mono ${isPriceUp ? 'text-green-400' : 'text-red-400'}`}>
                  {isPriceUp ? '+' : ''}{priceChange.toFixed(3)}%
                </span>
              )}
            </div>
          </motion.div>

          <div className="flex flex-col gap-3 items-end">
            <StatusBadge status={wsStatus} icon={wsStatus === "LIVE" ? <Wifi size={14} /> : <WifiOff size={14} />} />
            <RegimeBadge regime={liveData.regime} atr={liveData.atr} />
          </div>
        </header>

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 mb-8">
          <EnhancedStatCard icon={<Percent size={18} className="text-emerald-400" />} label="Win Rate" value={`${(metrics?.win_rate ?? 0).toFixed(1)}%`} trend={metrics?.win_rate >= 60 ? "up" : "neutral"} subtitle={`${metrics?.total_wins ?? 0}W / ${metrics?.total_losses ?? 0}L`} />
          <EnhancedStatCard icon={<TrendingDown size={18} className="text-rose-400" />} label="Max Drawdown" value={`$${(metrics?.max_drawdown ?? 0).toFixed(2)}`} trend="down" subtitle="Peak to trough" />
          <EnhancedStatCard icon={<Target size={18} className="text-blue-400" />} label="Sharpe Ratio" value={metrics?.sharpe ?? "—"} trend={parseFloat(metrics?.sharpe) > 1.5 ? "up" : "neutral"} subtitle="Risk-adjusted returns" />
          <EnhancedStatCard icon={<Zap size={18} className="text-amber-400" />} label="Active Positions" value={Object.keys(liveData.active_trades || {}).length.toString()} trend="neutral" subtitle="Open trades" />
        </div>

        <AnimatePresence mode="wait">
          {activeTab === "terminal" && <TerminalView liveData={liveData} portfolio={portfolio} setReplayTrade={setReplayTrade} />}
          {activeTab === "journal" && <JournalView portfolio={portfolio} setReplayTrade={setReplayTrade} />}
          {activeTab === "analytics" && <AnalyticsView portfolio={portfolio} />}
        </AnimatePresence>
      </main>

      <AnimatePresence>
        {replayTrade && <TradeReplayModal trade={replayTrade} onClose={() => setReplayTrade(null)} />}
      </AnimatePresence>
    </div>
  );
}

// --- SUB-COMPONENTS ---

function NavIcon({ icon, active, onClick, label }: any) {
  return (
    <motion.button onClick={onClick} className={`relative w-14 h-14 rounded-xl flex items-center justify-center transition-all group ${active ? 'bg-blue-600 text-white shadow-lg shadow-blue-500/30' : 'bg-zinc-900/50 text-zinc-500 hover:bg-zinc-900 hover:text-zinc-300'}`} whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }}>
      {icon}
      <div className="absolute left-full ml-4 px-3 py-1.5 bg-zinc-900 border border-zinc-800 rounded-lg text-xs font-medium whitespace-nowrap opacity-0 group-hover:opacity-100 pointer-events-none transition-opacity">{label}</div>
    </motion.button>
  );
}

function StatusBadge({ status, icon }: { status: string; icon: React.ReactNode }) {
  const isLive = status === "LIVE";
  return (
    <motion.div className={`px-4 py-2 rounded-full flex items-center gap-2 text-xs font-bold tracking-wide ${isLive ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/30' : 'bg-red-500/10 text-red-400 border border-red-500/30'}`} animate={{ boxShadow: isLive ? ['0 0 0 0 rgba(16, 185, 129, 0.4)', '0 0 0 8px rgba(16, 185, 129, 0)'] : 'none' }} transition={{ duration: 2, repeat: Infinity }}>
      {icon} {status}
    </motion.div>
  );
}

function RegimeBadge({ regime, atr }: { regime: string; atr: number }) {
  const colors = {
    TRENDING: 'bg-blue-500/10 text-blue-400 border-blue-500/30',
    RANGING: 'bg-amber-500/10 text-amber-400 border-amber-500/30',
    VOLATILE: 'bg-red-500/10 text-red-400 border-red-500/30',
    UNKNOWN: 'bg-zinc-700/10 text-zinc-500 border-zinc-700/30'
  };
  const normalizedRegime = String(regime || "").trim().toUpperCase();
  const safeRegime = (normalizedRegime in colors ? normalizedRegime : "UNKNOWN") as keyof typeof colors;
  const showAtr = Number.isFinite(atr) && atr > 0;
  return (
    <div className={`px-4 py-2 rounded-full border text-xs font-bold ${colors[safeRegime]}`}>
      {safeRegime} {showAtr && <span className="opacity-60">• ATR ${atr.toFixed(0)}</span>}
    </div>
  );
}

function EnhancedStatCard({ icon, label, value, trend, subtitle }: any) {
  const trendColors = { up: 'text-green-400', down: 'text-red-400', neutral: 'text-zinc-400' };
  return (
    <motion.div className="relative p-5 bg-zinc-900/50 backdrop-blur-sm border border-zinc-800/50 rounded-2xl group hover:border-zinc-700/50 transition-all overflow-hidden" whileHover={{ scale: 1.02 }} initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
      <div className="absolute inset-0 bg-gradient-to-br from-blue-600/5 via-transparent to-transparent opacity-0 group-hover:opacity-100 transition-opacity" />
      <div className="relative z-10">
        <div className="flex items-start justify-between mb-3"><div className="p-2 bg-zinc-900/80 rounded-lg">{icon}</div></div>
        <div className={`text-2xl font-black mb-1 ${trendColors[trend as keyof typeof trendColors]}`}>{value}</div>
        <div className="text-xs text-zinc-500 font-medium uppercase tracking-wide mb-1">{label}</div>
        {subtitle && <div className="text-[10px] text-zinc-600 font-mono">{subtitle}</div>}
      </div>
    </motion.div>
  );
}

function Panel({ title, children, className = "" }: any) {
  return (
    <motion.div className={`bg-zinc-900/30 backdrop-blur-sm border border-zinc-800/50 rounded-2xl overflow-hidden ${className}`} initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }}>
      <div className="px-5 py-3 border-b border-zinc-800/50 bg-zinc-900/50"><h3 className="text-xs font-bold text-zinc-400 uppercase tracking-wider">{title}</h3></div>
      <div className="p-5">{children}</div>
    </motion.div>
  );
}

// --- IMPROVEMENT: ENHANCED ACTIVE TRADES PANEL WITH REASONING ---
function ActiveTradesPanel({ trades }: { trades: any }) {
  if (!trades || Object.keys(trades).length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-8 border border-dashed border-zinc-800 rounded-xl">
        <Crosshair size={24} className="text-zinc-700 mb-2" />
        <div className="text-xs text-zinc-500 font-mono uppercase tracking-wide">No Active Positions</div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {Object.entries(trades).map(([slug, trade]: any) => (
        <div key={slug} className="p-4 bg-zinc-950/50 border border-zinc-800/50 rounded-xl">
          <div className="flex items-center justify-between mb-3">
            <div className="flex flex-col">
              <span className={`px-2 py-0.5 rounded text-[10px] font-black w-fit mb-1 ${
                trade.decision === 'UP' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
              }`}>
                {trade.decision} {trade.score}/3 CORE
              </span>
              <span className="text-[9px] text-zinc-600 font-mono italic">
                {trade.signals?.[0] || "Calculating flow..."}
              </span>
            </div>
            <span className="text-[10px] text-zinc-500 font-mono bg-zinc-900 px-2 py-1 rounded border border-zinc-800">
  {slug.split('-').slice(-2).join('-')}
</span>
          </div>
          <div className="grid grid-cols-2 gap-4 text-xs">
            <div>
              <div className="text-[10px] text-zinc-600 uppercase font-bold tracking-tighter">Entry Price</div>
              <div className="font-mono text-zinc-300 font-bold">${trade.bought_price}</div>
            </div>
            <div>
              <div className="text-[10px] text-zinc-600 uppercase font-bold tracking-tighter">Position Size</div>
              <div className="font-mono text-blue-400 font-bold">${trade.bet_size}</div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}

function TerminalView({ liveData, portfolio, setReplayTrade }: any) {
  return (
    <motion.div key="terminal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="grid grid-cols-12 gap-6">
      <div className="col-span-12 xl:col-span-8 space-y-6">
        <Panel title="Live Price Action • 15m Candles">
          {liveData.candle ? <PriceChart candle={liveData.candle} vwap={liveData.vwap} /> : (
            <div className="flex items-center justify-center h-64 border border-dashed border-zinc-800 rounded-xl">
              <div className="text-center"><div className="w-12 h-12 border-4 border-blue-500/20 border-t-blue-500 rounded-full animate-spin mx-auto mb-3" /><span className="text-xs text-zinc-500 font-mono uppercase tracking-wider">Awaiting stream...</span></div>
            </div>
          )}
        </Panel>
        <div className="grid grid-cols-2 gap-6">
          <Panel title="Equity Curve"><EquityCurve data={portfolio?.equity_curve || []} /></Panel>
          <Panel title="Monte Carlo Projection"><GrowthChart data={portfolio?.projections?.paths || []} /></Panel>
        </div>
        <Panel title="Performance Heatmap • UTC"><WinHeatmap data={portfolio?.heatmap || []} /></Panel>
        <Panel title="Live Engine Terminal"><TerminalStream logs={liveData.logs || []} /></Panel>
      </div>
      <div className="col-span-12 xl:col-span-4 space-y-6">
        <Panel title="Active Positions"><ActiveTradesPanel trades={liveData.active_trades} /></Panel>
        <Panel title="AI Context"><AiInsights insights={portfolio?.insights || []} /></Panel>
        {liveData.ai_log && <Panel title="Latest AI Reasoning"><AiLogDisplay aiLog={liveData.ai_log} /></Panel>}
      </div>
    </motion.div>
  );
}

function TerminalStream({ logs }: { logs: string[] }) {
  const terminalRef = useRef<HTMLDivElement>(null);
  useEffect(() => { if (terminalRef.current) terminalRef.current.scrollTop = terminalRef.current.scrollHeight; }, [logs]);
  return (
    <div ref={terminalRef} className="h-96 overflow-y-auto bg-zinc-950/80 border border-zinc-800/50 rounded-xl p-4 font-mono text-[10px] leading-relaxed space-y-1 scroll-smooth">
      {logs.slice(-100).map((log, i) => (
        <div key={i} className={`${log.includes('[ERROR]') ? 'text-red-400' : log.includes('[WARNING]') ? 'text-amber-400' : log.includes('[GATE]') ? 'text-blue-400' : log.includes('[DECISION]') ? 'text-green-400' : 'text-zinc-500'}`}>{log}</div>
      ))}
    </div>
  );
}

function AiLogDisplay({ aiLog }: any) {
  return (
    <div className="space-y-3">
      <div className="p-3 bg-zinc-950/50 rounded-lg border border-zinc-800/50">
        <div className="text-[10px] text-zinc-600 font-mono mb-2 uppercase tracking-widest font-black">Prompt</div>
        <div className="text-xs text-zinc-400 leading-relaxed whitespace-pre-wrap italic">{aiLog.prompt}</div>
      </div>
      <div className="flex items-center justify-between p-3 bg-zinc-950/50 rounded-lg border border-zinc-800/50">
        <div className="text-[10px] text-zinc-600 font-mono uppercase tracking-widest font-black">Response</div>
        <div className={`px-3 py-1 rounded font-black text-xs ${aiLog.response?.includes('SKIP') ? 'bg-red-500/20 text-red-400 border border-red-500/30' : 'bg-green-500/20 text-green-400 border border-green-500/30'}`}>{aiLog.response || "PROCESSING"}</div>
      </div>
    </div>
  );
}

function JournalView({ portfolio, setReplayTrade }: any) {
  return (
    <motion.div key="journal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }}>
      <Panel title="Execution History"><TradeJournalTable trades={portfolio?.journal || []} onReplay={setReplayTrade} /></Panel>
    </motion.div>
  );
}

function TradeJournalTable({ trades, onReplay }: any) {
  if (!trades || trades.length === 0) return <div className="text-center py-12 text-zinc-500 text-sm">No records found.</div>;
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="border-b border-zinc-800"><tr className="text-zinc-500 uppercase tracking-widest font-black text-[10px]"><th className="pb-4 text-left">Time</th><th className="pb-4 text-left">Market</th><th className="pb-4 text-center">Dir</th><th className="pb-4 text-right">Strike</th><th className="pb-4 text-right">Final</th><th className="pb-4 text-right">P&L</th><th className="pb-4 text-center">Action</th></tr></thead>
        <tbody>
          {trades.map((t: any, i: number) => (
            <tr key={i} className="border-b border-zinc-800/30 hover:bg-zinc-800/20 transition-colors">
              <td className="py-4 text-zinc-400 font-mono">{t["Timestamp (UTC)"]?.slice(11, 19)}</td>
              <td className="py-4 text-zinc-300 font-mono text-[10px]">{t["Market Slug"]?.slice(-10)}</td>
              <td className="py-4 text-center"><span className={`px-2 py-0.5 rounded text-[10px] font-black ${t["AI Decision"] === 'UP' ? 'text-green-400 bg-green-500/10' : 'text-red-400 bg-red-500/10'}`}>{t["AI Decision"]}</span></td>
              <td className="py-4 text-right font-mono text-zinc-400">${t["Strike Price"]}</td>
              <td className="py-4 text-right font-mono text-zinc-300">${t["Final Price"]}</td>
              <td className={`py-4 text-right font-mono font-black ${t["PnL"] >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{t["PnL"] >= 0 ? '+' : ''}${t["PnL"]?.toFixed(2)}</td>
              <td className="py-4 text-center"><button onClick={() => onReplay(t)} className="px-2 py-1 text-[9px] bg-blue-500/10 hover:bg-blue-500/20 text-blue-400 border border-blue-500/30 rounded font-black tracking-widest transition-all">REPLAY</button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function AnalyticsView({ portfolio }: any) {
  const attr = portfolio?.attribution;

  return (
    <motion.div key="analytics" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="space-y-6">
      <div className="grid grid-cols-3 gap-6">
        <Panel title="Risk Profile">
          <div className="space-y-4">
            <div className="flex justify-between items-center"><span className="text-xs text-zinc-500 font-bold uppercase tracking-tighter">Value at Risk (95%)</span><span className="text-sm font-black text-rose-400">${portfolio?.metrics?.var_95 || "0.00"}</span></div>
            <div className="flex justify-between items-center"><span className="text-xs text-zinc-500 font-bold uppercase tracking-tighter">Profit Expectancy</span><span className="text-sm font-black text-emerald-400">${portfolio?.metrics?.expectancy || "0.00"}</span></div>
            <div className="flex justify-between items-center"><span className="text-xs text-zinc-500 font-bold uppercase tracking-tighter">Optimal Kelly</span><span className="text-sm font-black text-blue-400">{portfolio?.metrics?.kelly_optimal || "0.0%"}</span></div>
          </div>
        </Panel>

        {/* --- UPDATED ATTRIBUTION PANEL --- */}
        <Panel title="Flow Attribution">
          {attr && (attr.ai_confirmed.trades > 0 || attr.system_only.trades > 0) ? (
            <div className="space-y-4 mt-1">
              <div className="flex justify-between items-center">
                <span className="text-xs text-zinc-500 font-bold uppercase tracking-tighter">
                  🤖 AI Confirmed ({attr.ai_confirmed.trades})
                </span>
                <span className="text-sm font-black text-blue-400">
                  {attr.ai_confirmed.win_rate}% <span className={attr.ai_confirmed.pnl >= 0 ? "text-emerald-400" : "text-rose-400"}>
                    ({attr.ai_confirmed.pnl >= 0 ? '+' : ''}${attr.ai_confirmed.pnl.toFixed(2)})
                  </span>
                </span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-xs text-zinc-500 font-bold uppercase tracking-tighter">
                  ⚙️ System Only ({attr.system_only.trades})
                </span>
                <span className="text-sm font-black text-purple-400">
                  {attr.system_only.win_rate}% <span className={attr.system_only.pnl >= 0 ? "text-emerald-400" : "text-rose-400"}>
                    ({attr.system_only.pnl >= 0 ? '+' : ''}${attr.system_only.pnl.toFixed(2)})
                  </span>
                </span>
              </div>
            </div>
          ) : (
            <div className="text-[10px] text-zinc-600 font-mono uppercase italic">Awaiting high-volume data...</div>
          )}
        </Panel>
        {/* ---------------------------------- */}

        <Panel title="Engine Health">
          <div className="text-[10px] text-zinc-600 font-mono uppercase italic">Latency: 42ms | ML: ACTIVE</div>
        </Panel>
      </div>
    </motion.div>
  );
}
function TradeReplayModal({ trade, onClose }: any) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!chartContainerRef.current || !trade) return;
    const chart = createChart(chartContainerRef.current, { layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#71717a" }, grid: { vertLines: { color: "#18181b" }, horzLines: { color: "#18181b" } }, timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#27272a" }, height: 450 });
    const candleSeries = chart.addSeries(CandlestickSeries, { upColor: "#22c55e", downColor: "#ef4444", borderVisible: false, wickUpColor: "#22c55e", wickDownColor: "#ef4444" });
    const strikePrice = parseFloat(trade["Strike Price"]);
    if (!isNaN(strikePrice)) candleSeries.createPriceLine({ price: strikePrice, color: '#a855f7', lineWidth: 2, lineStyle: 2, axisLabelVisible: true, title: 'Strike' });
    const loadReplay = async () => {
      try {
        const dateStr = trade["Timestamp (UTC)"].replace(" ", "T");
        const timestamp = Math.floor(new Date(dateStr + "Z").getTime() / 1000);
        const res = await fetch(`${getHttpUrl()}/api/history/replay?timestamp=${timestamp}`);
        const data = await res.json();
        if (data.history?.length > 0) {
          candleSeries.setData(data.history.sort((a: any, b: any) => a.time - b.time));
          (candleSeries as any).setMarkers([{ time: timestamp as any, position: trade["AI Decision"] === 'UP' ? 'belowBar' : 'aboveBar', color: trade["AI Decision"] === 'UP' ? '#22c55e' : '#ef4444', shape: trade["AI Decision"] === 'UP' ? 'arrowUp' : 'arrowDown', text: `${trade["AI Decision"]}` }]);
          chart.timeScale().fitContent();
        }
      } catch (e) { console.error("Replay error:", e); }
    };
    loadReplay();
    return () => chart.remove();
  }, [trade]);
  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} className="fixed inset-0 z-[100] flex items-center justify-center bg-black/90 backdrop-blur-md p-4" onClick={onClose}>
      <motion.div initial={{ scale: 0.9, y: 20 }} animate={{ scale: 1, y: 0 }} onClick={(e) => e.stopPropagation()} className="bg-zinc-900 border border-zinc-800 rounded-2xl w-full max-w-5xl shadow-2xl overflow-hidden">
        <div className="flex justify-between items-center p-6 border-b border-zinc-800 bg-zinc-950">
          <div><div className="text-zinc-500 text-xs uppercase font-bold tracking-widest mb-2 font-mono">Replaying Execution • {trade["Timestamp (UTC)"]}</div><div className="flex items-center gap-3"><span className="text-2xl text-white font-black">{trade["Market Slug"]}</span><span className={`px-3 py-1 rounded-lg text-sm font-black ${trade["AI Decision"] === 'UP' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}`}>{trade["AI Decision"]}</span></div></div>
          <button onClick={onClose} className="p-3 text-zinc-500 hover:text-white bg-zinc-900 rounded-xl border border-zinc-800 transition-all"><X size={20} /></button>
        </div>
        <div className="p-6"><div ref={chartContainerRef} className="w-full h-[450px]" /></div>
      </motion.div>
    </motion.div>
  );
}
