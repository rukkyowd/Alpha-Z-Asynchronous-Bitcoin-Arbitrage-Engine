"use client";
import { useEffect, useRef } from "react";
import { createChart, CandlestickSeries, LineSeries, ColorType } from "lightweight-charts";
import { getHttpUrl } from "../config"; // Adjust path if your config file is located elsewhere

export default function PriceChart({ candle, vwap }: { candle: any; vwap: number }) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);
  const candlestickSeriesRef = useRef<any>(null);
  const vwapSeriesRef = useRef<any>(null);
  
  // LOCK: Prevents live ticks from breaking the chart before history loads
  const isHistoryLoaded = useRef(false);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // 1. Initialize Chart
    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#71717a" },
      grid: { vertLines: { color: "#18181b" }, horzLines: { color: "#18181b" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "#27272a" },
      rightPriceScale: { borderColor: "#27272a" },
      height: 350,
    });

    // 2. Add Series
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#22c55e", downColor: "#ef4444", 
      borderVisible: false, wickUpColor: "#22c55e", wickDownColor: "#ef4444" 
    });
    
    const vwapSeries = chart.addSeries(LineSeries, {
      color: "#3b82f6",
      lineWidth: 2,
      lineStyle: 2,
      priceLineVisible: false,
      lastValueVisible: true,
    });

    chartRef.current = chart;
    candlestickSeriesRef.current = candleSeries;
    vwapSeriesRef.current = vwapSeries;

    // 3. THE BOOT SEQUENCE: Load History
    const loadHistory = async () => {
      try {
        // Now dynamically uses your current IP (Localhost or Tailscale)
        const res = await fetch(`${getHttpUrl()}/api/history`);
        const data = await res.json();
      
        if (data.history && data.history.length > 0) {
          const sanitizedHistory = data.history.sort((a: any, b: any) => a.time - b.time);
          candlestickSeriesRef.current.setData(sanitizedHistory);
          
          // Unlock the chart to accept live ticks
          isHistoryLoaded.current = true;
        }
      } catch (e) {
        console.error("Failed to load chart history", e);
      }
    };
    loadHistory();

    const handleResize = () => chart.applyOptions({ width: chartContainerRef.current?.clientWidth });
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, []);

  // 4. Handle Live Updates safely
  useEffect(() => {
    // Drop live ticks into the void if history hasn't been painted yet
    if (!isHistoryLoaded.current) return;

    if (candlestickSeriesRef.current && candle?.time) {
      candlestickSeriesRef.current.update(candle);
      
      if (vwap && vwapSeriesRef.current) {
        vwapSeriesRef.current.update({
          time: candle.time,
          value: vwap
        });
      }
    }
  }, [candle, vwap]);

  return <div ref={chartContainerRef} className="w-full h-full min-h-[350px]" />;
}