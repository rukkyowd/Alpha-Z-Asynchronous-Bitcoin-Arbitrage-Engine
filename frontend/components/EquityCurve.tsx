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
        textColor: "#9ca3af",
      },
      grid: {
        vertLines: { color: "#27272a", style: 1 },
        horzLines: { color: "#27272a", style: 1 },
      },
      timeScale: { timeVisible: true },
      height: 300,
    });

    // Check if overall PnL is positive or negative for color
    const isProfitable = data[data.length - 1]?.value >= 0;
    const lineColor = isProfitable ? "#3b82f6" : "#ef4444"; // Blue for profit, red for loss
    const topColor = isProfitable ? "rgba(59, 130, 246, 0.3)" : "rgba(239, 68, 68, 0.3)";

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
