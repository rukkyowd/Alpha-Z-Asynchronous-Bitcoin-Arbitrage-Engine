"use client";

import React from "react";
import { motion } from "framer-motion";

type DrawdownRiskGaugeProps = {
  dailyPnl: number;
  maxDailyLossPct: number;
  currentBalance: number;
  regime?: string;
};

function describeArc(cx: number, cy: number, r: number, startAngle: number, endAngle: number): string {
  const start = polarToCartesian(cx, cy, r, endAngle);
  const end = polarToCartesian(cx, cy, r, startAngle);
  const largeArc = endAngle - startAngle > 180 ? 1 : 0;
  return `M ${start.x} ${start.y} A ${r} ${r} 0 ${largeArc} 0 ${end.x} ${end.y}`;
}

function polarToCartesian(cx: number, cy: number, r: number, angleDeg: number) {
  const rad = ((angleDeg - 90) * Math.PI) / 180;
  return { x: cx + r * Math.cos(rad), y: cy + r * Math.sin(rad) };
}

export default function DrawdownRiskGauge({
  dailyPnl,
  maxDailyLossPct,
  currentBalance,
  regime = "NORMAL",
}: DrawdownRiskGaugeProps) {
  const cx = 100, cy = 95, r = 72;
  const sweep = 180;
  const startAngle = 180;

  const maxLossUsd = currentBalance * maxDailyLossPct;
  const usedPct = maxLossUsd > 0 ? Math.max(0, Math.min(1, Math.abs(Math.min(0, dailyPnl)) / maxLossUsd)) : 0;
  const remaining = maxLossUsd > 0 ? Math.max(0, maxLossUsd - Math.abs(Math.min(0, dailyPnl))) : 0;

  const fillAngle = startAngle + usedPct * sweep;
  const fillColor = usedPct < 0.5 ? "#34d399" : usedPct < 0.8 ? "#fbbf24" : "#f87171";
  const regimeColor = regime === "PAUSED" ? "text-[var(--az-loss)]" : regime === "CAUTIOUS" ? "text-[var(--az-warning)]" : "text-[var(--az-profit)]";

  return (
    <div className="flex flex-col items-center gap-2">
      <svg viewBox="0 0 200 120" className="w-full max-w-[200px]">
        {/* Track */}
        <path d={describeArc(cx, cy, r, startAngle, startAngle + sweep)} fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth={12} strokeLinecap="round" />
        {/* Used */}
        {usedPct > 0.005 && (
          <motion.path
            d={describeArc(cx, cy, r, startAngle, fillAngle)}
            fill="none" stroke={fillColor} strokeWidth={12} strokeLinecap="round"
            initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 1, ease: "easeOut" }}
          />
        )}
        {/* Center */}
        <text x={cx} y={cy - 6} textAnchor="middle" className="fill-[var(--az-text-primary)] text-[20px] font-black font-mono">
          {(usedPct * 100).toFixed(0)}%
        </text>
        <text x={cx} y={cy + 10} textAnchor="middle" className="fill-[var(--az-text-muted)] text-[8px] font-bold uppercase tracking-widest">
          DD Used
        </text>
      </svg>

      <div className="grid grid-cols-2 gap-3 w-full text-[10px]">
        <div className="glass-card px-3 py-2 text-center">
          <div className="section-label mb-1">Remaining</div>
          <div className="data-mono font-bold text-[var(--az-text-primary)]">${remaining.toFixed(2)}</div>
        </div>
        <div className="glass-card px-3 py-2 text-center">
          <div className="section-label mb-1">Regime</div>
          <div className={`font-bold uppercase text-[11px] ${regimeColor}`}>{regime}</div>
        </div>
      </div>
    </div>
  );
}
