"use client";

import React, { useMemo } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
  CartesianGrid
} from 'recharts';
import { type PriceBar } from "./engine-types";

type SnakePriceChartProps = {
  candle: Partial<PriceBar> | null;
  history?: Array<Partial<PriceBar>>;
  vwap?: number;
  targetPrice?: number;
};

export default function SnakePriceChart({
  candle,
  history,
  vwap,
  targetPrice,
}: SnakePriceChartProps) {
  
  // 1. Map API Data structure: Merge history and live candle, deduplicate, and format for Recharts
  const chartData = useMemo(() => {
    const combined = [...(history || [])];
    if (candle) combined.push(candle);

    const deduped = new Map();
    combined.forEach(bar => {
      if (bar.time && bar.close) {
        // Handle varying epoch timestamp formats (seconds vs milliseconds)
        const timeMs = bar.time > 10000000000 ? bar.time : bar.time * 1000;
        const normalizedTime = Math.floor(timeMs / 1000);
        
        deduped.set(normalizedTime, {
          time: normalizedTime,
          price: Number(bar.close),
          formattedTime: new Date(timeMs).toLocaleTimeString("en-US", {
            hour: "numeric",
            minute: "2-digit"
          })
        });
      }
    });
    
    const sorted = Array.from(deduped.values()).sort((a, b) => a.time - b.time);
    return sorted.slice(-64); // Keep MAX_VISIBLE_POINTS to match your original SVG look
  }, [history, candle]);

  // 2. Calculate dynamic bounds for the Y-Axis to give the BTC prices some breathing room
  const { minBound, maxBound } = useMemo(() => {
    if (!chartData.length) return { minBound: 0, maxBound: 0 };
    const prices = chartData.map(d => d.price);
    if (targetPrice) prices.push(targetPrice);
    if (vwap && !targetPrice) prices.push(vwap);
    
    const minP = Math.min(...prices);
    const maxP = Math.max(...prices);
    const span = maxP - minP || 1; 
    
    return {
      minBound: minP - (span * 0.1),
      maxBound: maxP + (span * 0.1)
    };
  }, [chartData, targetPrice, vwap]);

  if (chartData.length === 0) {
    return (
      <div className="flex h-full min-h-[300px] items-center justify-center text-zinc-500 font-mono text-xs uppercase tracking-widest">
        Awaiting Market Data
      </div>
    );
  }

  return (
    <div className="w-full h-full min-h-[300px] font-sans relative group">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={chartData} margin={{ top: 10, right: 48, left: 0, bottom: 0 }}>
          <defs>
            {/* Matching your original UI orange theme */}
            <linearGradient id="polymarketOrange" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#fb923c" stopOpacity={0.25} />
              <stop offset="95%" stopColor="#fb923c" stopOpacity={0} />
            </linearGradient>
          </defs>

          {/* Faint horizontal lines only - exactly like Poly */}
          <CartesianGrid 
            vertical={false} 
            strokeDasharray="3 3" 
            stroke="#27272a" 
            opacity={0.6}
          />

          <XAxis 
            dataKey="formattedTime" 
            hide // Hide X-Axis in main view 
          />

          {/* Right-aligned Y-axis formatted for BTC Prices */}
          <YAxis 
            orientation="right" 
            domain={[minBound, maxBound]} 
            tickFormatter={(val) => `$${val.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
            axisLine={false}
            tickLine={false}
            tick={{ fill: '#71717a', fontSize: 11, fontWeight: 500 }}
            dx={10}
          />

          {/* Target / Strike Price reference line */}
          {targetPrice && (
            <ReferenceLine 
              y={targetPrice} 
              stroke="#a855f7" 
              strokeDasharray="4 4" 
              opacity={0.8}
              label={{ position: 'insideTopLeft', value: 'TARGET', fill: '#a855f7', fontSize: 10, offset: 5, fontWeight: 700 }}
            />
          )}
          
          {/* Fallback to VWAP if Target isn't available */}
          {vwap && !targetPrice && (
            <ReferenceLine 
              y={vwap} 
              stroke="#38bdf8" 
              strokeDasharray="4 4" 
              opacity={0.6}
              label={{ position: 'insideTopLeft', value: 'VWAP', fill: '#38bdf8', fontSize: 10, offset: 5, fontWeight: 700 }}
            />
          )}

          {/* Polymarket Tooltip and Crosshair */}
          <Tooltip 
            content={<PolymarketTooltip />} 
            cursor={{ stroke: '#a1a1aa', strokeWidth: 1, strokeDasharray: '4 4' }} 
            isAnimationActive={false}
          />

          {/* Type="stepAfter" provides the rigid snake line you had in your original SVG */}
          <Area 
            type="stepAfter" 
            dataKey="price" 
            stroke="#fb923c" 
            strokeWidth={2}
            fillOpacity={1} 
            fill="url(#polymarketOrange)" 
            activeDot={{ r: 5, strokeWidth: 2, fill: '#fb923c', stroke: '#18181b' }}
            isAnimationActive={false} 
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

// Minimalist, high-contrast dark mode tooltip
const PolymarketTooltip = ({ active, payload, label }: any) => {
  if (active && payload && payload.length) {
    const price = payload[0].value;
    return (
      <div className="bg-zinc-900 border border-zinc-700/50 rounded-lg p-3 shadow-xl flex flex-col items-start min-w-[120px]">
        <span className="text-xs text-zinc-400 font-medium mb-1 tracking-wide">
          {label}
        </span>
        <span className="text-xl font-bold text-zinc-100 tabular-nums">
          ${price.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
        </span>
      </div>
    );
  }
  return null;
};
