"use client";

import { useEffect, useRef } from "react";
import { createChart, AreaSeries, ColorType, type IChartApi, type UTCTimestamp } from "lightweight-charts";

type EquityPoint = {
  time: number;
  value: number;
};

export default function EquityCurve({ data }: { data: EquityPoint[] }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current || !data || data.length === 0) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#71717a",
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.03)", style: 1 },
        horzLines: { color: "rgba(255,255,255,0.03)", style: 1 },
      },
      timeScale: { timeVisible: true, borderColor: "rgba(255,255,255,0.06)" },
      rightPriceScale: { borderColor: "rgba(255,255,255,0.06)" },
      height: 300,
    });

    // Check if overall PnL is positive or negative for color
    const isProfitable = data[data.length - 1]?.value >= 0;
    const lineColor = isProfitable ? "#3b82f6" : "#f87171"; // Blue for profit, softer red for loss
    const topColor = isProfitable ? "rgba(59, 130, 246, 0.4)" : "rgba(248, 113, 113, 0.4)";

    const series = chart.addSeries(AreaSeries, {
      lineColor,
      topColor,
      bottomColor: "rgba(0, 0, 0, 0.0)",
      lineWidth: 2,
    });

    const normalizedData = data.map((point) => {
      const seconds = point.time > 1_000_000_000_000 ? Math.floor(point.time / 1000) : Math.floor(point.time);
      return {
        time: seconds as UTCTimestamp,
        value: point.value,
      };
    });

    series.setData(normalizedData);
    chartRef.current = chart;

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current?.clientWidth });
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, [data]);

  return <div ref={chartContainerRef} className="w-full h-full min-h-[300px]" />;
}
