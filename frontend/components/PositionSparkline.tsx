"use client";

import React, { useEffect, useRef } from "react";
import { createChart, LineSeries, ColorType, type IChartApi } from "lightweight-charts";

type PositionSparklineProps = {
  prices: number[];
  positive?: boolean;
  width?: number;
  height?: number;
};

export default function PositionSparkline({
  prices,
  positive = true,
  width = 64,
  height = 20,
}: PositionSparklineProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || !prices || prices.length < 2) return;

    const chart = createChart(containerRef.current, {
      width,
      height,
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "transparent",
      },
      grid: { vertLines: { visible: false }, horzLines: { visible: false } },
      timeScale: { visible: false },
      rightPriceScale: { visible: false },
      leftPriceScale: { visible: false },
      crosshair: { mode: 0 },
      handleScale: false,
      handleScroll: false,
    });

    const color = positive ? "#34d399" : "#f87171";
    const series = chart.addSeries(LineSeries, {
      color,
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });

    series.setData(
      prices.map((value, i) => ({ time: (i + 1) as any, value }))
    );

    chartRef.current = chart;

    return () => { chart.remove(); };
  }, [prices, positive, width, height]);

  if (!prices || prices.length < 2) {
    return <div className="h-5 w-16 skeleton-shimmer" />;
  }

  return <div ref={containerRef} style={{ width, height }} className="inline-block" />;
}
