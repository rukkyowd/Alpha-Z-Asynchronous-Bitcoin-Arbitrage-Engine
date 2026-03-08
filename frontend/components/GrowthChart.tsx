"use client";
import { useEffect, useRef } from "react";
// 1. Import LineSeries directly
import { createChart, LineSeries, LineStyle, ColorType } from "lightweight-charts";

export default function GrowthChart({ data }: { data: number[][] }) {
  const chartRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chartRef.current || !data || data.length === 0) return;

    const chart = createChart(chartRef.current, {
      layout: { 
        background: { type: ColorType.Solid, color: "transparent" }, 
        textColor: "#71717a" 
      },
      grid: { 
        vertLines: { visible: false }, 
        horzLines: { color: "rgba(255,255,255,0.03)" } 
      },
      timeScale: { visible: false },
      height: 150,
    });

    // 2. Use the new v5 addSeries API
    data.forEach((path, i) => {
      const series = chart.addSeries(LineSeries, { 
        color: i === 0 ? "#3b82f6" : "#27272a", 
        lineWidth: i === 0 ? 2 : 1,
        lineStyle: i === 0 ? LineStyle.Solid : LineStyle.Dotted 
      });
      
      // Map the array of numbers to the required { time, value } format
      series.setData(path.map((val, idx) => ({ 
        time: idx as any, 
        value: val 
      })));
    });

    return () => {
      chart.remove();
    };
  }, [data]);

  return <div ref={chartRef} className="w-full h-full" />;
}