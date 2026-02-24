"use client";

import React, { useState, useEffect, cloneElement, useRef } from "react";
import { 
  LayoutDashboard, BookOpen, Activity, TrendingUp, 
  ShieldAlert, Crosshair, Zap, Target, LineChart, X 
} from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { createChart, CandlestickSeries, ColorType } from "lightweight-charts";
import PriceChart from "../components/PriceChart";
import EquityCurve from "../components/EquityCurve";
import WinHeatmap from "../components/WinHeatmap";
import AiInsights from "../components/AiInsights";
import GrowthChart from "../components/GrowthChart";
import { getWsUrl, getHttpUrl } from "../config"; // Adjust path if needed

export default function EliteDashboard() {
  const [activeTab, setActiveTab] = useState("terminal");
  const [portfolio, setPortfolio] = useState<any>(null);
  
  // Added vwap to initial state structure
  const [liveData, setLiveData] = useState<any>({ price: 0, vwap: 0, active_trades: {}, candle: null, is_paper: false, logs: [] });
  const [wsStatus, setWsStatus] = useState("Connecting...");
  const [replayTrade, setReplayTrade] = useState<any>(null); // State for the Replay Modal
  
  const lastPriceRef = useRef(0);

  // 1. Optimized WebSocket Feed
  useEffect(() => {
    const ws = new WebSocket(getWsUrl());
    
    ws.onopen = () => setWsStatus("🟢 LIVE");
    ws.onclose = () => setWsStatus("🔴 OFFLINE");
    
    ws.onmessage = (e) => {
      const data = JSON.parse(e.data);
      setLiveData((prev: any) => {
        // Check if logs actually changed to avoid infinite re-renders
        const logsChanged = JSON.stringify(data.logs) !== JSON.stringify(prev.logs);
        const aiLogChanged = JSON.stringify(data.ai_log) !== JSON.stringify(prev.ai_log);
        const priceChanged = data.price !== prev.price;
        
        if (logsChanged || aiLogChanged || priceChanged) {
          return data;
        }
        return prev;
      });
    };

    return () => ws.close();
  }, []); // <--- This empty array prevents the WS from constantly disconnecting

  // 2. Intelligence Layer API Polling (30s interval)
  useEffect(() => {
    const fetchStats = async () => {
      try {
        // Dynamically fetch from the correct IP
        const res = await fetch(`${getHttpUrl()}/api/metrics`);
        const data = await res.json();
        if (!data.error) setPortfolio(data);
      } catch (e) { console.error("Metrics Sync Error", e); }
    };
    fetchStats();
    const interval = setInterval(fetchStats, 30000); 
    return () => clearInterval(interval);
  }, []);

  const metrics = portfolio?.metrics;
  const isPositive = (metrics?.current_pnl ?? 0) >= 0;

  return (
    <div className="min-h-screen bg-[#09090b] text-zinc-100 font-sans selection:bg-blue-500/30">
      {/* Side Navigation */}
      <nav className="fixed left-0 top-0 h-full w-20 bg-[#121214] border-r border-zinc-800 flex flex-col items-center py-8 gap-8 z-50">
        <div className="w-10 h-10 bg-blue-600 rounded-xl flex items-center justify-center font-black text-white shadow-lg shadow-blue-500/20 text-xs">OA</div>
        <NavIcon icon={<LayoutDashboard />} active={activeTab === "terminal"} onClick={() => setActiveTab("terminal")} />
        <NavIcon icon={<BookOpen />} active={activeTab === "journal"} onClick={() => setActiveTab("journal")} />
      </nav>

      <main className="pl-28 pr-8 py-8">
        {/* Header Section */}
        <header className="flex justify-between items-end mb-8">
          <motion.div initial={{ opacity: 0, y: -10 }} animate={{ opacity: 1, y: 0 }}>
            <h1 className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest mb-1 font-mono">Real-Time Equity</h1>
            <div className="flex items-center gap-4">
              <div className={`text-4xl font-black tabular-nums transition-all duration-500 ${
                isPositive 
                  ? 'text-white drop-shadow-[0_0_10px_rgba(255,255,255,0.1)]' 
                  : 'text-red-400 drop-shadow-[0_0_10px_rgba(248,113,113,0.2)]'
              }`}>
                ${metrics?.current_pnl?.toLocaleString() ?? "0.00"}
              </div>

              {/* Simulation Mode Badge */}
              {liveData.is_paper && (
                <motion.div 
                  initial={{ opacity: 0, scale: 0.8 }}
                  animate={{ opacity: 1, scale: 1 }}
                  className="px-3 py-1 rounded-md bg-yellow-500/10 border border-yellow-500/50 text-yellow-500 text-[10px] font-black tracking-tighter uppercase flex items-center gap-1.5 shadow-[0_0_15px_rgba(234,179,8,0.1)]"
                >
                  <div className="w-1.5 h-1.5 rounded-full bg-yellow-500 animate-pulse" />
                  Simulation Active
                </motion.div>
              )}
            </div>
          </motion.div>
          <div className="text-[10px] px-4 py-1.5 rounded-full bg-zinc-900 border border-zinc-800 font-black tracking-widest font-mono uppercase flex items-center gap-2">
            <div className={`w-1.5 h-1.5 rounded-full ${wsStatus.includes('LIVE') ? 'bg-green-500 shadow-[0_0_8px_#22c55e]' : 'bg-red-500'}`} />
            {wsStatus}
          </div>
        </header>

        {/* Institutional Stat Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
            <StatCard icon={<Activity size={14} className="text-blue-400"/>} label="BTC Index" value={`$${liveData.price.toLocaleString()}`} />
            <StatCard icon={<Zap size={14} className="text-yellow-400"/>} label="Win Rate" value={`${metrics?.win_rate ?? 0}%`} />
            <StatCard icon={<ShieldAlert size={14} className="text-red-400"/>} label="Max Drawdown" value={`$${metrics?.max_drawdown ?? 0}`} />
            <StatCard icon={<TrendingUp size={14} className="text-green-400"/>} label="Sharpe Ratio" value={metrics?.sharpe ?? "---"} />
        </div>

        <AnimatePresence mode="wait">
          {activeTab === "terminal" ? (
            <motion.div key="terminal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }} className="grid grid-cols-12 gap-6">
              
              {/* Center Column: Charts & Analysis */}
              <div className="col-span-12 lg:col-span-8 space-y-6">
                <Panel title="Market Execution View (1m Candles)">
                  {/* PASS THE VWAP DOWN HERE */}
                  <PriceChart candle={liveData.candle} vwap={liveData.vwap} />
                </Panel>
                
                <div className="grid grid-cols-2 gap-6">
                  <Panel title="Realized Equity Curve">
                    <EquityCurve data={portfolio?.equity_curve || []} color="#3b82f6" />
                  </Panel>
                  <Panel title="180D Growth Projection (Monte Carlo)">
                     <GrowthChart data={portfolio?.projections?.paths} />
                     <div className="mt-3 text-center text-[10px] text-zinc-500 font-bold uppercase tracking-widest font-mono">
                        Median Expected Outcome: <span className="text-blue-400">${portfolio?.projections?.median_180d}</span>
                     </div>
                  </Panel>
                </div>

                <Panel title="Hourly Alpha Heatmap (UTC Win %)">
                  <WinHeatmap data={portfolio?.heatmap} />
                </Panel>
                {/* Live Engine Terminal */}
                <Panel title="Live Engine Terminal">
                  <TerminalStream logs={liveData.logs || []} />
                </Panel>
              </div>


              {/* Sidebar Column: Risk & AI */}
              <div className="col-span-12 lg:col-span-4 space-y-6">

                {/* No-Bet Zone & Countdown Panel */}
                <Panel title="Market Lifecycle">
                  <div className="flex flex-col items-center justify-center py-2">
                    <div className={`text-4xl font-black tabular-nums ${
                      liveData.seconds_remaining < 45 ? 'text-red-500 animate-pulse' : 'text-blue-500'
                    }`}>
                      {liveData.seconds_remaining}s
                    </div>
                    <div className="text-[9px] font-bold text-zinc-500 uppercase tracking-widest mt-2">
                      {liveData.seconds_remaining < 45 ? "⚠️ NO-BET ZONE ACTIVE" : "ENGINE EVALUATING"}
                    </div>
                    {/* Progress bar */}
                    <div className="w-full h-1 bg-zinc-800 rounded-full mt-4 overflow-hidden">
                      <motion.div 
                        initial={{ width: "100%" }}
                        animate={{ width: `${(liveData.seconds_remaining / 300) * 100}%` }}
                        className={`h-full ${liveData.seconds_remaining < 45 ? 'bg-red-500' : 'bg-blue-500'}`}
                      />
                    </div>
                  </div>
                </Panel>

                <LiveTrades trades={liveData.active_trades} />

                 {/* AI Inference Log Panel */}
                 <Panel title="AI Decision Brain">
                  <AiInferencePanel aiLog={liveData.ai_log} />
                 </Panel>

                 <Panel title="Risk Intelligence Engine">
                    <div className="space-y-6">
                       <StreakMonitor label="Active Performance Streak" value={metrics?.current_streak} isWin={metrics?.is_winning_streak} />
                       <RiskMeter label="Drawdown Threshold" value={metrics?.max_drawdown} limit={-5.00} />
                       <div className="grid grid-cols-2 gap-3">
                          <RiskItem label="Value at Risk (95%)" value={`$${metrics?.var_95 ?? "0.00"}`} color="text-yellow-500" />
                          <RiskItem label="Kelly Optimal Size" value={metrics?.kelly_optimal ?? "---"} color="text-blue-400" />
                       </div>
                    </div>
                 </Panel>

                 <Panel title="AI Strategy Insights">
                    <AiInsights insights={portfolio?.insights} />
                 </Panel>
              </div>
            </motion.div>
          ) : (
            /* Journal Tab */
            <motion.div key="journal" initial={{ opacity: 0, x: 20 }} animate={{ opacity: 1, x: 0 }} exit={{ opacity: 0, x: -20 }}>
              <Panel title="Execution History & Audit Trail">
                <JournalTable trades={portfolio?.journal} onTradeClick={setReplayTrade} />
              </Panel>
            </motion.div>
          )}
        </AnimatePresence>

        {/* --- REPLAY MODAL --- */}
        <AnimatePresence>
          {replayTrade && (
            <TradeReplayModal 
              trade={replayTrade} 
              onClose={() => setReplayTrade(null)} 
            />
          )}
        </AnimatePresence>

      </main>
    </div>
  );
}

// --- Sub-Components ---

function NavIcon({ icon, active, onClick }: any) {
  return (
    <button onClick={onClick} className={`p-3 rounded-xl transition-all duration-300 ${active ? 'bg-blue-600/10 text-blue-500 border border-blue-500/20 shadow-lg shadow-blue-500/5' : 'text-zinc-600 hover:text-white'}`}>
      {cloneElement(icon, { size: 22 })}
    </button>
  );
}

function StatCard({ label, value, icon }: any) {
  return (
    <div className="bg-[#121214] p-5 rounded-2xl border border-zinc-800 hover:border-zinc-700 transition-colors shadow-sm">
      <div className="text-[9px] font-bold text-zinc-500 uppercase tracking-[0.2em] flex items-center gap-2 mb-2 font-mono">{icon}{label}</div>
      <div className="text-2xl font-black tabular-nums text-white leading-none">{value}</div>
    </div>
  );
}

function Panel({ title, children }: any) {
  return (
    <div className="bg-[#121214] border border-zinc-800 rounded-2xl p-6 shadow-sm overflow-hidden">
      <h3 className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest mb-6 flex items-center gap-2 font-mono">
        <div className="w-1.5 h-1.5 rounded-full bg-blue-600 animate-pulse"/>{title}
      </h3>
      {children}
    </div>
  );
}

function RiskItem({ label, value, color }: any) {
    return (
      <div className="p-4 bg-[#09090b] rounded-xl border border-zinc-800/50">
          <div className="text-[8px] font-bold text-zinc-500 uppercase tracking-widest mb-1 font-mono">{label}</div>
          <div className={`text-lg font-black tabular-nums ${color}`}>{value}</div>
      </div>
    );
}

function RiskMeter({ label, value, limit }: any) {
  const percentage = Math.min(Math.abs(value / limit) * 100, 100);
  return (
    <div className="space-y-2">
      <div className="flex justify-between text-[10px] font-bold text-zinc-500 uppercase font-mono">
        <span>{label}</span>
        <span>{percentage.toFixed(0)}% Capacity</span>
      </div>
      <div className="h-1.5 w-full bg-zinc-800 rounded-full overflow-hidden">
        <motion.div initial={{ width: 0 }} animate={{ width: `${percentage}%` }}
          className={`h-full ${percentage > 80 ? 'bg-red-500 shadow-[0_0_10px_rgba(239,68,68,0.5)]' : 'bg-blue-500'}`} 
        />
      </div>
    </div>
  );
}

function StreakMonitor({ label, value, isWin }: any) {
  return (
    <div className="flex justify-between items-center p-3 bg-zinc-900/50 rounded-xl border border-zinc-800">
      <span className="text-[10px] font-bold text-zinc-500 uppercase font-mono">{label}</span>
      <div className={`px-3 py-1 rounded-lg text-[10px] font-black tracking-tighter ${isWin ? 'bg-green-500/10 text-green-500 border border-green-500/20' : 'bg-red-500/10 text-red-500 border border-red-500/20'}`}>
        {value} CONSECUTIVE {isWin ? 'WINS' : 'LOSSES'}
      </div>
    </div>
  );
}

function LiveTrades({ trades }: any) {
  const list = Object.entries(trades || {});
  
  return (
    <div className="bg-[#121214] border border-zinc-800 rounded-2xl p-6 shadow-sm overflow-hidden flex flex-col gap-4">
      <div className="flex items-center justify-between mb-2">
        <h3 className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest flex items-center gap-2 font-mono">
          <Activity size={14} className="text-blue-500" />
          Live Exposures
        </h3>
        {list.length > 0 && (
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
          </span>
        )}
      </div>

      <AnimatePresence mode="popLayout">
        {list.length === 0 ? (
          <motion.div 
            initial={{ opacity: 0 }} 
            animate={{ opacity: 1 }} 
            exit={{ opacity: 0 }}
            className="text-[10px] text-zinc-600 text-center py-8 uppercase font-bold tracking-tighter border border-dashed border-zinc-800/50 rounded-xl font-mono italic"
          >
            Scanning for Inefficiencies...
          </motion.div>
        ) : (
          <div className="space-y-3">
            {list.map(([slug, trade]: any) => (
              <motion.div 
                key={slug}
                initial={{ opacity: 0, y: 10, scale: 0.95 }} 
                animate={{ opacity: 1, y: 0, scale: 1 }} 
                exit={{ opacity: 0, scale: 0.95 }}
                className="flex flex-col gap-3 bg-[#09090b] p-4 rounded-xl border border-zinc-800/80 hover:border-blue-500/30 transition-all"
              >
                <div className="flex items-center justify-between">
                  <div>
                    <div className="text-[9px] text-zinc-500 font-mono mb-1.5 truncate w-32 sm:w-auto uppercase tracking-widest font-bold">
                      {slug}
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`text-xs font-black px-2 py-0.5 rounded text-white ${
                        trade.decision === 'UP' ? 'bg-green-500/20 text-green-400 border border-green-500/20' : 'bg-red-500/20 text-red-400 border border-red-500/20'
                      }`}>
                        {trade.decision}
                      </span>
                      <span className="text-[11px] font-mono font-bold text-zinc-300">Target: ${trade.strike?.toFixed(2)}</span>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="text-lg font-black text-white">${trade.bet_size?.toFixed(2)}</div>
                    <div className="text-[9px] text-zinc-500 uppercase tracking-widest mt-1 font-mono font-bold">
                      Cost: {(trade.bought_price * 100)?.toFixed(1)}¢
                    </div>
                  </div>
                </div>

                <div className="space-y-1.5 mt-1 border-t border-zinc-800/50 pt-3">
                  <div className="flex justify-between items-center text-[8px] uppercase font-bold tracking-widest text-zinc-500">
                    <span>Conviction</span>
                    <span className={trade.score >= 3 ? 'text-blue-400' : 'text-zinc-500'}>
                      {trade.score}/4 Signals
                    </span>
                  </div>
                  <div className="flex gap-1 h-1">
                    {[1, 2, 3, 4].map((i) => (
                      <div 
                        key={i} 
                        className={`flex-1 rounded-full transition-all duration-500 ${
                          i <= trade.score 
                            ? (trade.score >= 3 ? 'bg-blue-500 shadow-[0_0_5px_#3b82f6]' : 'bg-zinc-500') 
                            : 'bg-zinc-800'
                        }`} 
                      />
                    ))}
                  </div>
                </div>

              </motion.div>
            ))}
          </div>
        )}
      </AnimatePresence>
    </div>
  );
}

export function JournalTable({ trades, onTradeClick }: any) {
  if (!trades || trades.length === 0) {
    return <div className="p-4 text-xs text-zinc-500 font-mono text-center">No execution history.</div>;
  }

  return (
    <div className="h-[400px] w-full overflow-y-auto custom-scrollbar">
      <div className="flex items-center text-[10px] uppercase text-zinc-600 tracking-widest font-black font-mono pb-3 border-b border-zinc-800 sticky top-0 bg-[#121214]">
        <div className="w-1/6 px-4">Time (UTC)</div>
        <div className="w-2/6 px-4">Market Identity</div>
        <div className="w-1/6 px-4 text-center">Direction</div>
        <div className="w-1/6 px-4 text-right">Strike</div>
        <div className="w-1/6 px-4 text-right">Net PnL</div>
      </div>
      {trades.map((t: any, i: number) => (
        <div
          key={i}
          onClick={() => onTradeClick(t)}
          className="flex items-center text-xs bg-[#09090b] hover:bg-zinc-800 transition-all cursor-pointer group border-b border-zinc-900/50 h-12"
        >
          <div className="w-1/6 px-4 text-zinc-500 font-mono italic">
            {t["Timestamp (UTC)"] ? t["Timestamp (UTC)"].split(' ')[1] : "---"}
          </div>
          <div className="w-2/6 px-4 font-bold text-zinc-300 group-hover:text-white truncate">
            {t["Market Slug"]}
          </div>
          <div className={`w-1/6 px-4 text-center font-black tracking-widest ${t["AI Decision"] === 'UP' ? 'text-green-500' : 'text-red-500'}`}>
            {t["AI Decision"]}
          </div>
          <div className="w-1/6 px-4 text-right font-mono text-zinc-400">
            ${parseFloat(t["Strike Price"])?.toFixed(2)}
          </div>
          <div className={`w-1/6 px-4 text-right font-mono font-bold ${t["PnL"] > 0 ? 'text-green-400' : (t["PnL"] < 0 ? 'text-red-400' : 'text-zinc-500')}`}>
            {t["PnL"] > 0 ? '+' : ''}${parseFloat(t["PnL"])?.toFixed(2)}
          </div>
        </div>
      ))}
    </div>
  );
}

function TerminalStream({ logs }: { logs: string[] }) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const [isPinned, setIsPinned] = useState(true);

  const onScroll = () => {
    if (scrollRef.current) {
      const { scrollTop, scrollHeight, clientHeight } = scrollRef.current;
      const isAtBottom = scrollHeight - scrollTop - clientHeight < 50;
      setIsPinned(isAtBottom);
    }
  };

  useEffect(() => {
    if (isPinned && scrollRef.current) {
      scrollRef.current.scrollTo({
        top: scrollRef.current.scrollHeight,
        behavior: "smooth",
      });
    }
  }, [logs, isPinned]);

  return (
    <div className="relative group">
      {!isPinned && (
        <button 
          onClick={() => setIsPinned(true)}
          className="absolute bottom-4 right-4 z-10 px-3 py-1 bg-blue-600 text-[9px] font-black uppercase rounded-full shadow-lg border border-blue-400 animate-bounce"
        >
          Resume Auto-Scroll ↓
        </button>
      )}

      <div 
        ref={scrollRef}
        onScroll={onScroll}
        className="bg-[#050505] rounded-xl border border-zinc-800/80 p-4 h-64 overflow-y-auto font-mono text-[10px] text-zinc-400 custom-scrollbar shadow-inner flex flex-col gap-1.5"
      >
        {logs?.length === 0 && <div className="text-zinc-500 italic">Waiting for terminal stream...</div>}
        
        {logs?.map((log, i) => {
          let colorClass = "text-zinc-500"; 
          if (log.includes("[GATE]")) colorClass = "text-zinc-600";
          else if (log.includes("[INFO]")) colorClass = "text-blue-300";
          else if (log.includes("[WARNING]")) colorClass = "text-yellow-400";
          else if (log.includes("[ERROR]") || log.includes("❌")) colorClass = "text-red-400";
          else if (log.includes("[RULE]") || log.includes("✅")) colorClass = "text-green-400";
          else if (log.includes("BET PLACED")) colorClass = "text-purple-400 font-bold";
          
          return (
            <div key={i} className={`leading-relaxed break-all hover:bg-zinc-900/50 px-1 rounded transition-colors ${colorClass}`}>
              {log}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function AiInferencePanel({ aiLog }: { aiLog: any }) {
  if (!aiLog?.prompt || aiLog.prompt === "No AI calls yet.") {
    return (
      <div className="flex flex-col items-center justify-center py-10 border border-dashed border-zinc-800 rounded-xl">
        <div className="w-2 h-2 bg-blue-500 rounded-full animate-ping mb-4" />
        <div className="text-[10px] text-zinc-600 font-bold uppercase tracking-tighter">
          Waiting for borderline signal...
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex justify-between items-center px-1">
        <span className="text-[9px] font-black text-blue-500 uppercase tracking-widest">Model: Mistral NeMo</span>
        <span className="text-[9px] font-mono text-zinc-600">{aiLog.timestamp}</span>
      </div>
      
      <div className="bg-black/60 rounded-lg border border-zinc-800/50 p-3 shadow-inner">
        <pre className="text-[9px] leading-relaxed text-zinc-400 overflow-x-hidden whitespace-pre-wrap font-mono custom-scrollbar max-h-64 overflow-y-auto">
          {aiLog.prompt}
        </pre>
      </div>

      <div className="flex items-center justify-between pt-2 border-t border-zinc-800/50">
        <div className="text-[10px] font-bold text-zinc-500 uppercase tracking-widest">Model Decision:</div>
        <div className={`text-[10px] font-black px-3 py-1 rounded tracking-widest ${
          aiLog.response?.includes('SKIP') 
            ? 'bg-red-500/10 text-red-500 border border-red-500/20' 
            : 'bg-green-500/10 text-green-500 border border-green-500/20'
        }`}>
          {aiLog.response || "PROCESSING..."}
        </div>
      </div>
    </div>
  );
}

function TradeReplayModal({ trade, onClose }: any) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !trade) return;

    // 1. Initialize Replay Chart
    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#71717a" },
      grid: { vertLines: { color: "#18181b" }, horzLines: { color: "#18181b" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#27272a" },
      height: 400,
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e", downColor: "#ef4444", 
      borderVisible: false, wickUpColor: "#22c55e", wickDownColor: "#ef4444" 
    });

    chartRef.current = chart;

    // 2. Add Horizontal Strike Price Line
    const strikePrice = parseFloat(trade["Strike Price"]);
    if (!isNaN(strikePrice)) {
      candleSeries.createPriceLine({
        price: strikePrice,
        color: '#a855f7', // Purple marker
        lineWidth: 2,
        lineStyle: 2, // Dashed
        axisLabelVisible: true,
        title: 'Strike',
      });
    }

    // 3. Fetch and Load Data
    const loadReplay = async () => {
      try {
        const dateStr = trade["Timestamp (UTC)"].replace(" ", "T");
        const exactTimestampSecs = Math.floor(new Date(dateStr + "Z").getTime() / 1000);

        const res = await fetch(`${getHttpUrl()}/api/history/replay?timestamp=${exactTimestampSecs}`);
        const data = await res.json();

        if (data.history && data.history.length > 0) {
          // Sort the data to be absolutely sure the chart doesn't reject it
          const safeData = data.history.sort((a: any, b: any) => a.time - b.time);
          candleSeries.setData(safeData);

          // BULLETPROOF FIX: Find the closest matching candle that actually exists in the data
          const closestCandle = safeData.reduce((prev: any, curr: any) => 
            Math.abs(curr.time - exactTimestampSecs) < Math.abs(prev.time - exactTimestampSecs) ? curr : prev
          );

          // 🚨 THE FIX: Attach to candleSeries, NOT chart.timeScale()
          candleSeries.setMarkers([
            {
              time: closestCandle.time as any,
              position: trade["AI Decision"] === 'UP' ? 'belowBar' : 'aboveBar',
              color: trade["AI Decision"] === 'UP' ? '#22c55e' : '#ef4444',
              shape: trade["AI Decision"] === 'UP' ? 'arrowUp' : 'arrowDown',
              text: `AI: ${trade["AI Decision"]}`,
            }
          ]);

          // Auto-zoom the chart to fit the data nicely
          chart.timeScale().fitContent();
        }
      } catch (e) {
        console.error("Replay fetch error:", e);
      }
    };

    loadReplay();

    // Clean up
    return () => chart.remove();
  }, [trade]);

  return (
    <motion.div 
      initial={{ opacity: 0 }} 
      animate={{ opacity: 1 }} 
      exit={{ opacity: 0 }} 
      className="fixed inset-0 z-[100] flex items-center justify-center bg-black/80 backdrop-blur-sm p-4"
    >
      <motion.div 
        initial={{ scale: 0.95, y: 20 }} 
        animate={{ scale: 1, y: 0 }} 
        className="bg-[#121214] border border-zinc-800 rounded-2xl w-full max-w-4xl shadow-2xl flex flex-col overflow-hidden"
      >
        <div className="flex justify-between items-center p-5 border-b border-zinc-800 bg-[#09090b]">
          <div>
            <div className="text-zinc-500 text-[10px] uppercase font-black tracking-widest font-mono mb-1">
              Trade Replay • {trade["Timestamp (UTC)"]}
            </div>
            <div className="text-xl text-white font-black tracking-tight flex items-center gap-3">
              {trade["Market Slug"]} 
              <span className={`px-2 py-0.5 rounded text-sm ${trade["AI Decision"] === 'UP' ? 'bg-green-500/20 text-green-400 border border-green-500/30' : 'bg-red-500/20 text-red-400 border border-red-500/30'}`}>
                {trade["AI Decision"]}
              </span>
              <span className="text-sm font-mono text-zinc-500 border border-zinc-800 px-2 py-0.5 rounded bg-zinc-900">
                Strike: ${trade["Strike Price"]}
              </span>
            </div>
          </div>
          <button onClick={onClose} className="p-2 text-zinc-500 hover:text-white bg-zinc-900 rounded-lg border border-zinc-800 hover:bg-zinc-800 transition-colors">
            <X size={20} />
          </button>
        </div>
        <div className="p-6">
          <div ref={chartContainerRef} className="w-full h-[400px]" />
        </div>
      </motion.div>
    </motion.div>
  );
}