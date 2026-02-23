"use client";

import React from "react";

export default function WinHeatmap({ data }: { data: any[] }) {
  if (!data || data.length === 0) return <div className="py-10 text-center text-zinc-600 text-[10px] uppercase">Waiting for data...</div>;

  // Helper to determine green intensity based on win rate (0-100)
  const getBgColor = (wr: number, trades: number) => {
    if (trades === 0) return "bg-zinc-900/30 border-zinc-800/50";
    if (wr >= 60) return "bg-green-500/80 border-green-400/50 text-white";
    if (wr >= 50) return "bg-green-600/40 border-green-500/30 text-green-200";
    if (wr >= 40) return "bg-yellow-600/20 border-yellow-500/20 text-yellow-200";
    return "bg-red-600/20 border-red-500/20 text-red-200";
  };

  return (
    <div className="grid grid-cols-6 sm:grid-cols-8 md:grid-cols-12 gap-2">
      {data.map((item) => (
        <div 
          key={item.hour}
          className={`relative group flex flex-col items-center justify-center p-2 rounded-lg border transition-all ${getBgColor(item.win_rate, item.trades)}`}
        >
          <span className="text-[9px] font-bold opacity-60 mb-1">{item.hour}:00</span>
          <span className="text-xs font-black tracking-tighter">{item.trades > 0 ? `${Math.round(item.win_rate)}%` : "—"}</span>
          
          {/* Hover Tooltip */}
          <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block z-50 pointer-events-none">
            <div className="bg-black border border-zinc-700 p-2 rounded shadow-2xl text-[9px] whitespace-nowrap leading-tight">
              <div className="font-bold text-blue-400 mb-1">HOUR {item.hour}:00 UTC</div>
              <div>Trades: {item.trades}</div>
              <div>Win Rate: {item.win_rate}%</div>
            </div>
          </div>
        </div>
      ))}
    </div>
  );
}