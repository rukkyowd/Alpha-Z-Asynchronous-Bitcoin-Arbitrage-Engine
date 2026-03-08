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
  const getCellStyle = (wr: number, trades: number) => {
    if (trades === 0) {
      return "bg-az-surface border border-az-border/50 text-az-text-muted/30";
    }
    
    if (wr >= 60) return "bg-az-profit/40 border border-az-profit/50 text-az-profit";
    if (wr >= 50) return "bg-az-profit/20 border border-az-profit/30 text-az-profit";
    if (wr >= 40) return "bg-az-warning/20 border border-az-warning/30 text-az-warning";
    return "bg-az-loss/20 border border-az-loss/30 text-az-loss";
  };

  // Get trend icon
  const getTrendIcon = (wr: number, trades: number) => {
    if (trades === 0) return <Minus size={10} className="opacity-30" />;
    if (wr >= 55) return <TrendingUp size={10} />;
    if (wr < 45) return <TrendingDown size={10} />;
    return <Minus size={10} />;
  };

  return (
    <div className="flex flex-col gap-3">
      {/* Heatmap Grid */}
      <div className="grid grid-cols-6 sm:grid-cols-8 md:grid-cols-12 gap-1">
        {data.map((item) => {
          const style = getCellStyle(item.win_rate, item.trades);
          const isHovered = hoveredHour === item.hour;
          
          return (
            <motion.div
              key={item.hour}
              className={`relative group flex h-10 flex-col items-center justify-center rounded-sm transition-all cursor-pointer ${style}`}
              whileHover={{ scale: 1.05, zIndex: 10 }}
              onHoverStart={() => setHoveredHour(item.hour)}
              onHoverEnd={() => setHoveredHour(null)}
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: item.hour * 0.01 }}
            >
              <div className="flex w-full items-center justify-between px-1">
                <span className="text-[8px] font-mono tracking-tighter opacity-70">
                  {String(item.hour).padStart(2, '0')}h
                </span>
                <span className="opacity-60">
                  {getTrendIcon(item.win_rate, item.trades)}
                </span>
              </div>
              
              <span className="text-[10px] font-mono font-bold">
                {item.trades > 0 ? `${Math.round(item.win_rate)}%` : "—"}
              </span>

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
                      <div className="font-bold text-az-text mb-1 border-b border-az-border/50 pb-1">
                        {String(item.hour).padStart(2, '0')}:00 UTC
                      </div>
                      <div className="flex justify-between text-az-text-muted">
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
          <span className="text-[9px] text-az-text-muted font-mono uppercase">Good</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-warning/20 border border-az-warning/30 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">Mid</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-loss/20 border border-az-loss/30 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">Poor</span>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="w-2 h-2 bg-az-surface border border-az-border/50 rounded-sm" />
          <span className="text-[9px] text-az-text-muted font-mono uppercase">None</span>
        </div>
      </div>
    </div>
  );
}
