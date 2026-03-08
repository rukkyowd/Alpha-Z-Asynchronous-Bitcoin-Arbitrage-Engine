"use client";

import React, { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { TrendingUp, TrendingDown, Minus } from "lucide-react";

export default function WinHeatmap({ data }: { data: any[] }) {
  const [hoveredHour, setHoveredHour] = useState<number | null>(null);

  if (!data || data.length === 0) {
    return (
      <div className="flex h-24 items-center justify-center text-[10px] font-mono uppercase tracking-widest text-az-text-muted">
        Awaiting Heatmap Data...
      </div>
    );
  }

  // Helper to determine color intensity and styling
  const getCellStyle = (pnl: number, trades: number) => {
    if (trades === 0) {
      return "bg-az-surface border border-az-border/50 text-az-text-muted/30";
    }
    
    if (pnl >= 100) return "bg-az-profit/40 border border-az-profit/50 text-az-profit";
    if (pnl >= 0) return "bg-az-profit/10 border border-az-profit/20 text-az-profit/80";
    if (pnl <= -100) return "bg-az-loss/40 border border-az-loss/50 text-az-loss";
    return "bg-az-loss/10 border border-az-loss/20 text-az-loss/80";
  };

  return (
    <div className="flex flex-col gap-3">
      {/* Heatmap Grid */}
      <div className="grid grid-cols-6 sm:grid-cols-8 md:grid-cols-12 gap-1">
        {data.map((item) => {
          const totalPnl = (item.avg_pnl || 0) * item.trades;
          const style = getCellStyle(totalPnl, item.trades);
          const isHovered = hoveredHour === item.hour;
          
          return (
            <motion.div
              key={item.hour}
              className={`relative group flex h-14 flex-col items-center justify-center rounded-sm transition-all cursor-pointer ${style}`}
              whileHover={{ scale: 1.05, zIndex: 10 }}
              onHoverStart={() => setHoveredHour(item.hour)}
              onHoverEnd={() => setHoveredHour(null)}
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: item.hour * 0.01 }}
            >
              <div className="absolute top-1 left-1">
                <span className="text-[8px] font-mono tracking-tighter opacity-70">
                  {String(item.hour).padStart(2, '0')}h
                </span>
              </div>
              
              <div className="flex flex-col items-center justify-center mt-2">
                <span className={`text-[11px] font-mono font-bold tracking-tight ${item.trades === 0 ? 'text-az-text-muted/50' : totalPnl >= 0 ? 'text-az-profit' : 'text-az-loss'}`}>
                  {item.trades > 0 ? `${totalPnl >= 0 ? '+' : '-'}$${Math.abs(totalPnl).toFixed(2)}` : "$0.00"}
                </span>
                <span className="text-[8px] font-mono opacity-60">
                  {item.trades} trades
                </span>
              </div>

              {/* Hover Tooltip */}
              <AnimatePresence>
                {isHovered && (
                  <motion.div
                    initial={{ opacity: 0, y: 5, scale: 0.95 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: 5, scale: 0.95 }}
                    className="absolute bottom-full mb-2 z-50 pointer-events-none left-1/2 -translate-x-1/2"
                  >
                    <div className="bg-az-surface border border-az-border p-2 rounded shadow-lg text-[9px] font-mono whitespace-nowrap min-w-[120px]">
                      <div className="font-bold text-az-text mb-1 border-b border-az-border/50 pb-1 flex justify-between">
                        <span>{String(item.hour).padStart(2, '0')}:00 UTC</span>
                        <span className={totalPnl >= 0 ? 'text-az-profit' : 'text-az-loss'}>
                          {totalPnl >= 0 ? '+' : '-'}${Math.abs(totalPnl).toFixed(0)}
                        </span>
                      </div>
                      <div className="flex justify-between text-az-text-muted mt-1">
                        <span>Trades:</span>
                        <span className="text-az-text">{item.trades}</span>
                      </div>
                      <div className="flex justify-between text-az-text-muted">
                        <span>Win Rate:</span>
                        <span className={`font-bold ${
                          item.win_rate >= 50 ? 'text-az-profit' : 'text-az-loss'
                        }`}>
                          {item.win_rate.toFixed(1)}%
                        </span>
                      </div>
                      <div className="flex justify-between text-az-text-muted">
                        <span>Avg PnL:</span>
                        <span className={`font-bold ${
                          (item.avg_pnl || 0) >= 0 ? 'text-az-profit' : 'text-az-loss'
                        }`}>
                          {(item.avg_pnl || 0) >= 0 ? '+' : '-'}${Math.abs(item.avg_pnl || 0).toFixed(2)}
                        </span>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </motion.div>
          );
        })}
      </div>

      {/* Mini Legend */}
      <div className="flex items-center justify-end gap-3 px-1">
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-profit/40 border border-az-profit/50 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">+$100+</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-profit/10 border border-az-profit/20 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">Profit</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-loss/10 border border-az-loss/20 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">Loss</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-loss/40 border border-az-loss/50 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">-$100+</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-surface border border-az-border/50 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">None</span>
        </div>
      </div>
    </div>
  );
}
