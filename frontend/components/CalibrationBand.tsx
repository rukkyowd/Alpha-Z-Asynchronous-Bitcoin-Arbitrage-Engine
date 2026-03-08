"use client";

import React from "react";
import { motion } from "framer-motion";

type BucketData = {
  bucket: string;
  predicted: number;
  observed: number;
  count: number;
  calibrated_observed?: number;
};

type CalibrationBandProps = {
  buckets: BucketData[];
};

export default function CalibrationBand({ buckets }: CalibrationBandProps) {
  if (!buckets || buckets.length === 0) {
    return (
      <div className="flex h-48 items-center justify-center rounded-xl border border-dashed border-[var(--az-border)]">
        <span className="section-label">Awaiting calibration data...</span>
      </div>
    );
  }

  const W = 320, H = 200, PAD = 32;
  const plotW = W - PAD * 2, plotH = H - PAD * 2;

  const toX = (v: number) => PAD + v * plotW;
  const toY = (v: number) => PAD + (1 - v) * plotH;

  const rawPath = buckets.map((b, i) => {
    const x = toX(b.predicted);
    const y = toY(b.observed);
    return `${i === 0 ? "M" : "L"} ${x} ${y}`;
  }).join(" ");

  const calPath = buckets.some(b => b.calibrated_observed != null)
    ? buckets.map((b, i) => {
        const x = toX(b.predicted);
        const y = toY(b.calibrated_observed ?? b.observed);
        return `${i === 0 ? "M" : "L"} ${x} ${y}`;
      }).join(" ")
    : null;

  const perfectPath = `M ${toX(0)} ${toY(0)} L ${toX(1)} ${toY(1)}`;

  return (
    <div className="flex flex-col items-center gap-3">
      <svg viewBox={`0 0 ${W} ${H}`} className="w-full max-w-[320px]">
        {/* Grid */}
        {[0, 0.25, 0.5, 0.75, 1].map(v => (
          <React.Fragment key={v}>
            <line x1={toX(v)} y1={toY(0)} x2={toX(v)} y2={toY(1)} stroke="rgba(255,255,255,0.04)" />
            <line x1={toX(0)} y1={toY(v)} x2={toX(1)} y2={toY(v)} stroke="rgba(255,255,255,0.04)" />
            <text x={toX(v)} y={toY(0) + 14} textAnchor="middle" className="fill-[var(--az-text-dim)] text-[8px] font-mono">
              {(v * 100).toFixed(0)}
            </text>
            <text x={toX(0) - 6} y={toY(v) + 3} textAnchor="end" className="fill-[var(--az-text-dim)] text-[8px] font-mono">
              {(v * 100).toFixed(0)}
            </text>
          </React.Fragment>
        ))}

        {/* Perfect calibration line */}
        <line x1={toX(0)} y1={toY(0)} x2={toX(1)} y2={toY(1)} stroke="rgba(255,255,255,0.1)" strokeDasharray="4 3" />

        {/* Raw model */}
        <motion.path
          d={rawPath} fill="none" stroke="#f87171" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round"
          initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 1 }}
        />

        {/* Calibrated model */}
        {calPath && (
          <motion.path
            d={calPath} fill="none" stroke="#3b82f6" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round"
            initial={{ pathLength: 0 }} animate={{ pathLength: 1 }} transition={{ duration: 1.2 }}
          />
        )}

        {/* Data points */}
        {buckets.map((b, i) => (
          <circle key={i} cx={toX(b.predicted)} cy={toY(b.observed)} r={Math.max(2, Math.min(5, b.count / 5))}
            fill="#f87171" opacity={0.7}
          />
        ))}

        {/* Axis labels */}
        <text x={W / 2} y={H - 2} textAnchor="middle" className="fill-[var(--az-text-muted)] text-[8px] font-bold uppercase tracking-wider">
          Predicted %
        </text>
        <text x={6} y={H / 2} textAnchor="middle" transform={`rotate(-90, 6, ${H / 2})`}
          className="fill-[var(--az-text-muted)] text-[8px] font-bold uppercase tracking-wider"
        >
          Observed %
        </text>
      </svg>

      {/* Legend */}
      <div className="flex items-center gap-4 text-[10px]">
        <span className="flex items-center gap-1.5">
          <span className="h-2 w-2 rounded-full bg-red-400" />
          <span className="text-[var(--az-text-secondary)]">Raw Model</span>
        </span>
        {calPath && (
          <span className="flex items-center gap-1.5">
            <span className="h-2 w-2 rounded-full bg-blue-500" />
            <span className="text-[var(--az-text-secondary)]">Calibrated</span>
          </span>
        )}
        <span className="flex items-center gap-1.5">
          <span className="h-[1px] w-4 border-t border-dashed border-white/20" />
          <span className="text-[var(--az-text-secondary)]">Perfect</span>
        </span>
      </div>
    </div>
  );
}
