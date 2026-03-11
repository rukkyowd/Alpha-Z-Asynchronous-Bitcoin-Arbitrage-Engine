"use client";

import { useId } from "react";
import type { PriceBar } from "./engine-types";

const VIEWBOX_WIDTH = 100;
const VIEWBOX_HEIGHT = 56;
const PLOT_LEFT = 1.5;
const PLOT_RIGHT = 89.5;
const PLOT_TOP = 8;
const PLOT_BOTTOM = 44;
const MAX_VISIBLE_POINTS = 64;

type SnakePriceChartProps = {
  candle: Partial<PriceBar> | null;
  history?: Array<Partial<PriceBar>>;
  vwap?: number;
  targetPrice?: number;
};

type SeriesPoint = {
  time: number;
  value: number;
};

type SvgPoint = {
  x: number;
  y: number;
};

function asFiniteNumber(value: unknown): number | null {
  const next = Number(value);
  return Number.isFinite(next) ? next : null;
}

function normalizeTimestamp(value: number): number {
  return value > 1_000_000_000_000 ? value : value * 1000;
}

function normalizeSeriesPoint(bar: Partial<PriceBar> | null | undefined): SeriesPoint | null {
  if (!bar) {
    return null;
  }

  const time = asFiniteNumber(bar.time);
  const close = asFiniteNumber(bar.close);
  if (time === null || close === null) {
    return null;
  }

  return {
    time: normalizeTimestamp(time),
    value: close,
  };
}

function buildVisibleSeries(
  history: Array<Partial<PriceBar>> | undefined,
  candle: Partial<PriceBar> | null,
): SeriesPoint[] {
  const deduped = new Map<number, SeriesPoint>();

  for (const bar of history ?? []) {
    const point = normalizeSeriesPoint(bar);
    if (point) {
      deduped.set(point.time, point);
    }
  }

  const livePoint = normalizeSeriesPoint(candle);
  if (livePoint) {
    deduped.set(livePoint.time, livePoint);
  }

  const points = Array.from(deduped.values()).sort((left, right) => left.time - right.time);
  if (points.length === 0) {
    return [];
  }

  return points.slice(-MAX_VISIBLE_POINTS);
}

function buildStepPath(points: SvgPoint[]): string {
  if (points.length === 0) {
    return "";
  }

  let path = `M ${points[0].x.toFixed(2)} ${points[0].y.toFixed(2)}`;
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const current = points[index];
    const midpointX = (previous.x + current.x) / 2;

    path += ` L ${midpointX.toFixed(2)} ${previous.y.toFixed(2)}`;
    path += ` L ${midpointX.toFixed(2)} ${current.y.toFixed(2)}`;
    path += ` L ${current.x.toFixed(2)} ${current.y.toFixed(2)}`;
  }

  return path;
}

function buildAreaPath(points: SvgPoint[], baseline: number): string {
  if (points.length === 0) {
    return "";
  }

  const linePath = buildStepPath(points);
  const lastPoint = points[points.length - 1];
  const firstPoint = points[0];
  return `${linePath} L ${lastPoint.x.toFixed(2)} ${baseline.toFixed(2)} L ${firstPoint.x.toFixed(2)} ${baseline.toFixed(2)} Z`;
}

function formatPrice(value: number): string {
  return value.toLocaleString("en-US", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function formatAxisPrice(value: number): string {
  return `$${Math.round(value).toLocaleString("en-US")}`;
}

function formatTimeLabel(timestamp: number, spanMs: number): string {
  if (spanMs <= 90 * 60 * 1000) {
    return new Date(timestamp).toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  return new Date(timestamp).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
  });
}

function niceStep(value: number): number {
  if (!Number.isFinite(value) || value <= 0) {
    return 1;
  }

  const magnitude = Math.pow(10, Math.floor(Math.log10(value)));
  const normalized = value / magnitude;

  if (normalized <= 1) {
    return magnitude;
  }
  if (normalized <= 2) {
    return 2 * magnitude;
  }
  if (normalized <= 5) {
    return 5 * magnitude;
  }
  return 10 * magnitude;
}

function buildPriceTicks(minValue: number, maxValue: number): number[] {
  const range = Math.max(maxValue - minValue, 1);
  const step = niceStep(range / 4);
  let start = Math.floor(minValue / step) * step;
  let end = Math.ceil(maxValue / step) * step;

  if (start === end) {
    start -= step;
    end += step;
  }

  const ticks: number[] = [];
  for (let value = end; value >= start - step * 0.5; value -= step) {
    ticks.push(value);
  }

  if (ticks.length > 5) {
    const stride = Math.ceil(ticks.length / 5);
    return ticks.filter((_, index) => index % stride === 0);
  }

  return ticks;
}

export default function SnakePriceChart({
  candle,
  history,
  vwap,
  targetPrice,
}: SnakePriceChartProps) {
  const gradientId = useId().replace(/:/g, "");
  const series = buildVisibleSeries(history, candle);
  const fallbackPoint = normalizeSeriesPoint(candle);
  const visibleSeries = series.length > 0 ? series : fallbackPoint ? [fallbackPoint] : [];
  const currentPoint = visibleSeries[visibleSeries.length - 1] ?? null;
  const focusPrice = asFiniteNumber(targetPrice) ?? asFiniteNumber(vwap);
  const focusLabel = asFiniteNumber(targetPrice) !== null ? "Target" : asFiniteNumber(vwap) !== null ? "VWAP" : null;

  const values = visibleSeries.map((point) => point.value);
  const domainValues = focusPrice === null ? values : [...values, focusPrice];
  const maxValue = domainValues.length > 0 ? Math.max(...domainValues) : 1;
  const minValue = domainValues.length > 0 ? Math.min(...domainValues) : 0;
  const span = Math.max(maxValue - minValue, Math.max(Math.abs(maxValue), 1) * 0.0035);
  const paddedMin = minValue - span * 0.45;
  const paddedMax = maxValue + span * 0.45;
  const plotWidth = PLOT_RIGHT - PLOT_LEFT;
  const plotHeight = PLOT_BOTTOM - PLOT_TOP;
  const toY = (value: number) =>
    PLOT_TOP + ((paddedMax - value) / Math.max(paddedMax - paddedMin, 0.0001)) * plotHeight;

  const svgPoints = visibleSeries.map((point, index) => ({
    x: PLOT_LEFT + (index / Math.max(visibleSeries.length - 1, 1)) * plotWidth,
    y: toY(point.value),
  }));

  const linePath = buildStepPath(svgPoints);
  const areaPath = buildAreaPath(svgPoints, PLOT_BOTTOM);
  const headPoint = svgPoints[svgPoints.length - 1] ?? null;
  const focusY = focusPrice === null ? null : toY(focusPrice);
  const priceTicks = buildPriceTicks(paddedMin, paddedMax).filter((tick) => {
    const y = toY(tick);
    return y >= PLOT_TOP - 0.5 && y <= PLOT_BOTTOM + 0.5;
  });
  const timeSpan = Math.max(
    (visibleSeries[visibleSeries.length - 1]?.time ?? 0) - (visibleSeries[0]?.time ?? 0),
    0,
  );
  const tickCount = Math.min(6, visibleSeries.length);
  const timeTickIndexes = Array.from({ length: tickCount }, (_, index) =>
    Math.round((index * Math.max(visibleSeries.length - 1, 0)) / Math.max(tickCount - 1, 1)),
  ).filter((value, index, valuesList) => valuesList.indexOf(value) === index);

  return (
    <div className="absolute inset-0 overflow-hidden rounded-xl border border-white/[0.05] bg-[radial-gradient(circle_at_top,rgba(251,146,60,0.14),transparent_38%),linear-gradient(180deg,#0d1420_0%,#0a121c_55%,#09111a_100%)] shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
      <svg viewBox={`0 0 ${VIEWBOX_WIDTH} ${VIEWBOX_HEIGHT}`} preserveAspectRatio="none" className="h-full w-full">
        <defs>
          <linearGradient id={`${gradientId}-fill`} x1="0%" y1="0%" x2="0%" y2="100%">
            <stop offset="0%" stopColor="rgba(249,115,22,0.22)" />
            <stop offset="65%" stopColor="rgba(249,115,22,0.06)" />
            <stop offset="100%" stopColor="rgba(249,115,22,0)" />
          </linearGradient>
          <linearGradient id={`${gradientId}-stroke`} x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#fb923c" />
            <stop offset="55%" stopColor="#f59e0b" />
            <stop offset="100%" stopColor="#fbbf24" />
          </linearGradient>
        </defs>

        {priceTicks.map((tick) => {
          const y = toY(tick);
          return (
            <line
              key={tick}
              x1={PLOT_LEFT}
              x2={PLOT_RIGHT}
              y1={y}
              y2={y}
              stroke="rgba(148,163,184,0.14)"
              strokeWidth="0.32"
            />
          );
        })}

        {focusY !== null ? (
          <line
            x1={PLOT_LEFT}
            x2={PLOT_RIGHT}
            y1={focusY}
            y2={focusY}
            stroke={focusLabel === "Target" ? "rgba(251,146,60,0.68)" : "rgba(56,189,248,0.5)"}
            strokeWidth="0.35"
            strokeDasharray="1.6 1.6"
          />
        ) : null}

        {areaPath ? <path d={areaPath} fill={`url(#${gradientId}-fill)`} /> : null}
        {linePath ? (
          <path
            d={linePath}
            fill="none"
            stroke="rgba(251,146,60,0.18)"
            strokeWidth="3.6"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        ) : null}
        {linePath ? (
          <path
            d={linePath}
            fill="none"
            stroke={`url(#${gradientId}-stroke)`}
            strokeWidth="1.2"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        ) : null}

        {headPoint ? (
          <>
            <circle cx={headPoint.x} cy={headPoint.y} r="1.45" fill="rgba(251,146,60,0.18)" />
            <circle cx={headPoint.x} cy={headPoint.y} r="0.52" fill="#f59e0b" stroke="#fdba74" strokeWidth="0.24" />
          </>
        ) : null}
      </svg>

      <div className="pointer-events-none absolute inset-0">
        {priceTicks.map((tick) => (
          <div
            key={`label-${tick}`}
            className="absolute right-3 -translate-y-1/2 font-mono text-[11px] tabular-nums text-slate-500"
            style={{ top: `${(toY(tick) / VIEWBOX_HEIGHT) * 100}%` }}
          >
            {formatAxisPrice(tick)}
          </div>
        ))}

        {focusY !== null && focusLabel ? (
          <div
            className={`absolute right-3 -translate-y-1/2 rounded-full border px-3 py-1 text-[10px] font-semibold tracking-wide ${
              focusLabel === "Target"
                ? "border-orange-300/20 bg-slate-600/80 text-slate-100"
                : "border-sky-300/20 bg-sky-500/10 text-sky-200"
            }`}
            style={{ top: `${(focusY / VIEWBOX_HEIGHT) * 100}%` }}
          >
            {focusLabel}
          </div>
        ) : null}

        {headPoint && currentPoint ? (
          <div
            className="absolute rounded-full border border-orange-400/30 bg-black/40 px-2 py-1 font-mono text-[10px] font-semibold tabular-nums text-orange-200 shadow-[0_0_20px_rgba(249,115,22,0.14)]"
            style={{
              left: `min(calc(${(headPoint.x / VIEWBOX_WIDTH) * 100}% + 8px), calc(100% - 84px))`,
              top: `max(calc(${(headPoint.y / VIEWBOX_HEIGHT) * 100}% - 22px), 12px)`,
            }}
          >
            {formatPrice(currentPoint.value)}
          </div>
        ) : null}

        <div className="absolute inset-x-0 bottom-2 px-4">
          {timeTickIndexes.map((pointIndex, index) => {
            const point = visibleSeries[pointIndex];
            if (!point) {
              return null;
            }

            const left = (svgPoints[pointIndex]?.x ?? PLOT_LEFT) / VIEWBOX_WIDTH;
            const alignmentClass =
              index === 0
                ? "translate-x-0 text-left"
                : index === timeTickIndexes.length - 1
                  ? "-translate-x-full text-right"
                  : "-translate-x-1/2 text-center";

            return (
              <div
                key={`${point.time}-${index}`}
                className={`absolute bottom-0 font-mono text-[11px] tabular-nums text-slate-500 ${alignmentClass}`}
                style={{ left: `${left * 100}%` }}
              >
                {formatTimeLabel(point.time, timeSpan)}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
