"use client";

import React from "react";
import { motion } from "framer-motion";

type BrierScoreGaugeProps = {
  modelBrier: number;
  marketBrier: number;
  calibratedBrier?: number;
  label?: string;
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

export default function BrierScoreGauge({
  modelBrier,
  marketBrier,
  calibratedBrier,
  label = "Brier Score",
}: BrierScoreGaugeProps) {
  const cx = 100, cy = 100, r = 78;
  const sweepFull = 240;
  const startAngle = 150;

  const clamp = (v: number) => Math.max(0, Math.min(1, v));
  const modelAngle = startAngle + clamp(modelBrier) * sweepFull;
  const marketAngle = startAngle + clamp(marketBrier) * sweepFull;
  const calAngle = calibratedBrier != null ? startAngle + clamp(calibratedBrier) * sweepFull : null;

  const modelBetter = modelBrier < marketBrier;
  const advantage = Math.abs(modelBrier - marketBrier);

  return (
    <div className="flex flex-col items-center gap-3">
      <svg viewBox="0 0 200 140" className="w-full max-w-[220px]">
        {/* Track */}
        <path d={describeArc(cx, cy, r, startAngle, startAngle + sweepFull)} fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth={10} strokeLinecap="round" />
        {/* Market arc */}
        <motion.path
          d={describeArc(cx, cy, r, startAngle, marketAngle)}
          fill="none" stroke="#8b5cf6" strokeWidth={10} strokeLinecap="round" opacity={0.5}
          initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 1, ease: "easeOut" }}
        />
        {/* Model arc */}
        <motion.path
          d={describeArc(cx, cy, r, startAngle, modelAngle)}
          fill="none" stroke={modelBetter ? "#34d399" : "#f87171"} strokeWidth={10} strokeLinecap="round"
          initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 1.2, ease: "easeOut" }}
        />
        {/* Calibrated arc */}
        {calAngle != null && (
          <motion.path
            d={describeArc(cx, cy, r, startAngle, calAngle)}
            fill="none" stroke="#3b82f6" strokeWidth={4} strokeLinecap="round" strokeDasharray="4 3"
            initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 1.4, ease: "easeOut" }}
          />
        )}
        {/* Center label */}
        <text x={cx} y={cy - 8} textAnchor="middle" className="fill-[var(--az-text-primary)] text-[22px] font-black font-mono">
          {modelBrier.toFixed(3)}
        </text>
        <text x={cx} y={cy + 12} textAnchor="middle" className="fill-[var(--az-text-muted)] text-[9px] font-semibold uppercase tracking-widest">
          {label}
        </text>
      </svg>

      {/* Legend */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-[10px]">
        <div className="flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full" style={{ background: modelBetter ? "#34d399" : "#f87171" }} />
          <span className="text-[var(--az-text-secondary)]">Model</span>
          <span className="data-mono font-bold text-[var(--az-text-primary)]">{modelBrier.toFixed(3)}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-violet-500/60" />
          <span className="text-[var(--az-text-secondary)]">Market</span>
          <span className="data-mono font-bold text-[var(--az-text-primary)]">{marketBrier.toFixed(3)}</span>
        </div>
        {calibratedBrier != null && (
          <div className="col-span-2 flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-blue-500" />
            <span className="text-[var(--az-text-secondary)]">Calibrated</span>
            <span className="data-mono font-bold text-[var(--az-text-primary)]">{calibratedBrier.toFixed(3)}</span>
          </div>
        )}
      </div>

      {/* Advantage */}
      <div className={`badge ${modelBetter ? "badge-live" : "badge-offline"}`}>
        {modelBetter ? "Edge" : "Trailing"} by {(advantage * 100).toFixed(1)} pts
      </div>
    </div>
  );
}
