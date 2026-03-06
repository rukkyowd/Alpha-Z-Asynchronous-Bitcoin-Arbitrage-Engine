"use client";
import { useEffect, useRef } from "react";
import { createChart, CandlestickSeries, LineSeries, ColorType } from "lightweight-charts";

function getChartHeight(): number {
  if (typeof window !== "undefined" && window.innerWidth < 768) {
    return 260;
  }
  return 350;
}

export default function PriceChart({
  candle,
  history,
  vwap,
}: {
  candle: any;
  history?: any[];
  vwap: number;
}) {
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
      height: getChartHeight(),
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

    const handleResize = () =>
      chart.applyOptions({
        width: chartContainerRef.current?.clientWidth,
        height: getChartHeight(),
      });
    handleResize();
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, []);

  useEffect(() => {
    if (!candlestickSeriesRef.current || !Array.isArray(history) || history.length === 0) {
      return;
    }
    const sanitizedHistory = history
      .filter((bar) => bar && bar.time !== undefined)
      .map((bar) => ({
        time: Number(bar.time),
        open: Number(bar.open),
        high: Number(bar.high),
        low: Number(bar.low),
        close: Number(bar.close),
      }))
      .sort((a, b) => a.time - b.time);
    if (sanitizedHistory.length === 0) {
      return;
    }
    candlestickSeriesRef.current.setData(sanitizedHistory);
    isHistoryLoaded.current = true;
  }, [history]);

  // 4. Handle Live Updates safely
  useEffect(() => {
    if (!candlestickSeriesRef.current || !candle?.time) return;

    // Bootstrap with current candle if history has not arrived yet.
    if (!isHistoryLoaded.current) {
      candlestickSeriesRef.current.setData([candle]);
      isHistoryLoaded.current = true;
    } else {
      candlestickSeriesRef.current.update(candle);
    }

    if (vwap && vwapSeriesRef.current) {
      vwapSeriesRef.current.update({
        time: candle.time,
        value: vwap,
      });
    }
  }, [candle, vwap]);

  return (
    <div className="relative h-full w-full min-h-[260px] sm:min-h-[350px]">
      <div ref={chartContainerRef} className="h-full w-full min-h-[260px] sm:min-h-[350px]" />
    </div>
  );
}
