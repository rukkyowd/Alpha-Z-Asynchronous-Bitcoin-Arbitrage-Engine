"use client";

import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

export default function WinHeatmap({ data }: { data: any[] }) {
  const [hoveredHour, setHoveredHour] = useState<number | null>(null);

  if (!data || data.length === 0) {
    return (
      <div className="flex items-center justify-center py-12 border border-dashed border-zinc-800 rounded-xl">
        <div className="text-center">
          <div className="w-16 h-16 border-4 border-zinc-800 border-t-blue-500 rounded-full animate-spin mx-auto mb-4" />
          <div className="text-xs text-zinc-600 uppercase tracking-wide font-mono">
            Collecting hourly data...
          </div>
        </div>
      </div>
    );
  }

  // Helper to determine color intensity and styling
  const getCellStyle = (wr: number, trades: number) => {
    if (trades === 0) {
      return {
        bg: "bg-zinc-900/30",
        border: "border-zinc-800/50",
        text: "text-zinc-700",
        shadow: ""
      };
    }
    
    if (wr >= 70) {
      return {
        bg: "bg-emerald-500/80",
        border: "border-emerald-400/50",
        text: "text-white",
        shadow: "shadow-lg shadow-emerald-500/20"
      };
    }
    if (wr >= 60) {
      return {
        bg: "bg-emerald-500/50",
        border: "border-emerald-500/40",
        text: "text-emerald-100",
        shadow: "shadow-md shadow-emerald-500/10"
      };
    }
    if (wr >= 50) {
      return {
        bg: "bg-green-500/30",
        border: "border-green-500/30",
        text: "text-green-200",
        shadow: ""
      };
    }
    if (wr >= 40) {
      return {
        bg: "bg-amber-500/20",
        border: "border-amber-500/30",
        text: "text-amber-200",
        shadow: ""
      };
    }
    return {
      bg: "bg-red-500/20",
      border: "border-red-500/30",
      text: "text-red-200",
      shadow: ""
    };
  };

  // Get trend icon
  const getTrendIcon = (wr: number, trades: number) => {
    if (trades === 0) return <Minus size={14} className="opacity-40" />;
    if (wr >= 55) return <TrendingUp size={14} />;
    if (wr < 45) return <TrendingDown size={14} />;
    return <Minus size={14} />;
  };

  // Calculate summary stats
  const totalTrades = data.reduce((sum, item) => sum + item.trades, 0);
  const avgWinRate = totalTrades > 0 
    ? data.reduce((sum, item) => sum + (item.win_rate * item.trades), 0) / totalTrades 
    : 0;
  const bestHour = data.reduce((best, curr) => 
    curr.trades > 0 && curr.win_rate > (best?.win_rate || 0) ? curr : best, 
    null
  );
  const worstHour = data.reduce((worst, curr) => 
    curr.trades > 0 && curr.win_rate < (worst?.win_rate || 100) ? curr : worst,
    null
  );

  return (
    <div className="space-y-6">
      {/* Heatmap Grid */}
      <div className="grid grid-cols-6 sm:grid-cols-8 md:grid-cols-12 gap-2">
        {data.map((item) => {
          const style = getCellStyle(item.win_rate, item.trades);
          const isHovered = hoveredHour === item.hour;
          
          return (
            <motion.div
              key={item.hour}
              className={`relative group flex flex-col items-center justify-center p-3 rounded-xl border transition-all cursor-pointer ${style.bg} ${style.border} ${style.text} ${style.shadow}`}
              whileHover={{ scale: 1.05, zIndex: 10 }}
              whileTap={{ scale: 0.95 }}
              onHoverStart={() => setHoveredHour(item.hour)}
              onHoverEnd={() => setHoveredHour(null)}
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: item.hour * 0.02 }}
            >
              {/* Hour Label */}
              <span className="text-[9px] font-bold opacity-60 mb-1.5 uppercase tracking-wide">
                {String(item.hour).padStart(2, '0')}h
              </span>
              
              {/* Win Rate */}
              <span className="text-sm font-black tracking-tighter">
                {item.trades > 0 ? `${Math.round(item.win_rate)}%` : "—"}
              </span>

              {/* Trend Indicator */}
              {item.trades > 0 && (
                <div className="mt-1 opacity-70">
                  {getTrendIcon(item.win_rate, item.trades)}
                </div>
              )}

              {/* Trade Count Badge */}
              {item.trades > 0 && (
                <div className="absolute -top-1 -right-1 bg-blue-500 text-white text-[8px] font-bold px-1.5 py-0.5 rounded-full border border-blue-400 shadow-lg">
                  {item.trades}
                </div>
              )}

              {/* Hover Tooltip */}
              <AnimatePresence>
                {isHovered && (
                  <motion.div
                    initial={{ opacity: 0, y: 10, scale: 0.9 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: 10, scale: 0.9 }}
                    className="absolute bottom-full mb-3 z-50 pointer-events-none"
                    style={{
                      left: '50%',
                      transform: 'translateX(-50%)'
                    }}
                  >
                    <div className="bg-zinc-950 border border-zinc-700 p-3 rounded-lg shadow-2xl text-[10px] whitespace-nowrap leading-relaxed min-w-[140px]">
                      <div className="font-bold text-blue-400 mb-2 flex items-center gap-2">
                        <div className="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />
                        Hour {String(item.hour).padStart(2, '0')}:00 UTC
                      </div>
                      <div className="space-y-1 text-zinc-400">
                        <div className="flex justify-between">
                          <span>Trades:</span>
                          <span className="font-mono text-white">{item.trades}</span>
                        </div>
                        <div className="flex justify-between">
                          <span>Win Rate:</span>
                          <span className={`font-mono font-bold ${
                            item.win_rate >= 60 ? 'text-emerald-400' : 
                            item.win_rate >= 50 ? 'text-green-400' : 
                            'text-red-400'
                          }`}>
                            {item.win_rate.toFixed(1)}%
                          </span>
                        </div>
                        {item.avg_pnl !== undefined && (
                          <div className="flex justify-between pt-1 border-t border-zinc-800">
                            <span>Avg P&L:</span>
                            <span className={`font-mono font-bold ${
                              item.avg_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'
                            }`}>
                              {item.avg_pnl >= 0 ? '+' : ''}${item.avg_pnl.toFixed(2)}
                            </span>
                          </div>
                        )}
                      </div>
                      {/* Tooltip Arrow */}
                      <div className="absolute top-full -mt-[1px]" style={{ left: '50%', transform: 'translateX(-50%)' }}>
                        <div className="w-2 h-2 bg-zinc-950 border-r border-b border-zinc-700 transform rotate-45" />
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          );
        })}
      </div>

      {/* Summary Statistics */}
      <motion.div 
        className="grid grid-cols-3 gap-4 pt-4 border-t border-zinc-800"
        initial={{ opacity: 0, y: 10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.5 }}
      >
        <div className="text-center p-3 bg-zinc-900/50 rounded-xl border border-zinc-800/50">
          <div className="text-xs text-zinc-500 mb-1 uppercase tracking-wide">Total Trades</div>
          <div className="text-2xl font-black text-white">{totalTrades}</div>
        </div>
        
        <div className="text-center p-3 bg-zinc-900/50 rounded-xl border border-zinc-800/50">
          <div className="text-xs text-zinc-500 mb-1 uppercase tracking-wide">Avg Win Rate</div>
          <div className={`text-2xl font-black ${
            avgWinRate >= 60 ? 'text-emerald-400' : 
            avgWinRate >= 50 ? 'text-green-400' : 
            'text-amber-400'
          }`}>
            {avgWinRate.toFixed(1)}%
          </div>
        </div>
        
        <div className="text-center p-3 bg-zinc-900/50 rounded-xl border border-zinc-800/50">
          <div className="text-xs text-zinc-500 mb-1 uppercase tracking-wide">Best Hour</div>
          <div className="text-2xl font-black text-blue-400">
            {bestHour ? `${String(bestHour.hour).padStart(2, '0')}:00` : '—'}
          </div>
          {bestHour && (
            <div className="text-[10px] text-zinc-600 mt-1">
              {bestHour.win_rate.toFixed(0)}% WR • {bestHour.trades} trades
            </div>
          )}
        </div>
      </motion.div>

      {/* Legend */}
      <motion.div 
        className="flex items-center justify-center gap-4 pt-4 border-t border-zinc-800"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.6 }}
      >
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-emerald-500/50 border border-emerald-500/40 rounded" />
          <span className="text-[10px] text-zinc-500 font-mono">60%+</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-green-500/30 border border-green-500/30 rounded" />
          <span className="text-[10px] text-zinc-500 font-mono">50-60%</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-amber-500/20 border border-amber-500/30 rounded" />
          <span className="text-[10px] text-zinc-500 font-mono">40-50%</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-red-500/20 border border-red-500/30 rounded" />
          <span className="text-[10px] text-zinc-500 font-mono">&lt;40%</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="w-4 h-4 bg-zinc-900/30 border border-zinc-800/50 rounded" />
          <span className="text-[10px] text-zinc-500 font-mono">No Data</span>
        </div>
      </motion.div>
    </div>
  );
}
