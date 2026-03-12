import { useEffect, useRef } from "react";
import { createChart, CandlestickSeries, AreaSeries, LineSeries, ColorType, LineType } from "lightweight-charts";
import type { PriceBar } from "./engine-types";
import SnakePriceChart from "./SnakePriceChart";

function getChartHeight(): number {
  if (typeof window !== "undefined" && window.innerWidth < 768) {
    return 260;
  }
  return 350;
}

function normalizeBar(bar: Partial<PriceBar> | null | undefined): PriceBar | null {
  if (!bar) {
    return null;
  }
  let time = Number(bar.time);
  if (time > 10000000000) {
    time = Math.floor(time / 1000);
  }
  const open = Number(bar.open);
  const high = Number(bar.high);
  const low = Number(bar.low);
  const close = Number(bar.close);
  const volume = Number(bar.volume ?? 0);
  if (!Number.isFinite(time) || !Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) {
    return null;
  }
  return { time, open, high, low, close, volume };
}

function sanitizeHistoryBars(history: Array<Partial<PriceBar>>): PriceBar[] {
  const deduped = new Map<number, PriceBar>();

  history
    .map((bar) => normalizeBar(bar))
    .filter((bar): bar is PriceBar => Boolean(bar))
    .sort((a, b) => a.time - b.time)
    .forEach((bar) => {
      // Keep the latest copy for a timestamp so backfill/live overlap cannot break the chart.
      deduped.set(bar.time, bar);
    });

  return Array.from(deduped.values()).sort((a, b) => a.time - b.time);
}

type PriceChartProps = {
  candle: Partial<PriceBar> | null;
  history?: Array<Partial<PriceBar>>;
  vwap?: number;
  targetPrice?: number;
  type?: "candle" | "snake";
};

export default function PriceChart({
  candle,
  history,
  vwap,
  targetPrice,
  type = "candle",
}: PriceChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<any>(null);
  const candlestickSeriesRef = useRef<any>(null);
  const areaSeriesRef = useRef<any>(null);
  const vwapSeriesRef = useRef<any>(null);
  
  // LOCK: Prevents live ticks from breaking the chart before history loads
  const isHistoryLoaded = useRef(false);
  const lastAreaTimeRef = useRef<number>(0);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // 1. Initialize Chart
    const chart = createChart(chartContainerRef.current, {
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: "#71717a" },
      grid: { vertLines: { color: "rgba(255,255,255,0.03)" }, horzLines: { color: "rgba(255,255,255,0.03)" } },
      timeScale: { timeVisible: true, secondsVisible: false, borderColor: "rgba(255,255,255,0.06)" },
      rightPriceScale: { borderColor: "rgba(255,255,255,0.06)" },
      height: getChartHeight(),
      crosshair: {
        mode: 1, // Magnet crosshair
      },
    });

    // 2. Add Series
    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: "#34d399", downColor: "#f87171", 
      borderVisible: false, wickUpColor: "#34d399", wickDownColor: "#f87171",
      visible: type === "candle", // Toggle visibility
    });

    const areaSeries = chart.addSeries(AreaSeries, {
      lineColor: '#f59e0b', // Amber-500
      topColor: 'rgba(245, 158, 11, 0.4)',
      bottomColor: 'rgba(245, 158, 11, 0.0)',
      lineWidth: 2,
      lineType: LineType.WithSteps,
      priceLineVisible: true,
      lastValueVisible: true,
      crosshairMarkerVisible: true,
      visible: type === "snake", // Toggle visibility
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
    areaSeriesRef.current = areaSeries;
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
  }, []); // Note: re-init only on mount. We toggle visibility via props in another effect.

  // 3. Toggle visibility when `type` prop changes
  useEffect(() => {
    if (candlestickSeriesRef.current && areaSeriesRef.current) {
        candlestickSeriesRef.current.applyOptions({ visible: type === "candle" });
        areaSeriesRef.current.applyOptions({ visible: type === "snake" });
    }
  }, [type]);

  // 4. Load history
  useEffect(() => {
    if (!candlestickSeriesRef.current || !areaSeriesRef.current || !Array.isArray(history) || history.length === 0) {
      return;
    }
    const sanitizedHistory = sanitizeHistoryBars(history);
    if (sanitizedHistory.length === 0) {
      return;
    }
    candlestickSeriesRef.current.setData(sanitizedHistory);
    // Area series only takes {time, value} where value is close price
    areaSeriesRef.current.setData(sanitizedHistory.map(b => ({ time: b.time, value: b.close })));
    if (sanitizedHistory.length > 0) {
      lastAreaTimeRef.current = sanitizedHistory[sanitizedHistory.length - 1].time;
    }
    isHistoryLoaded.current = true;
  }, [history]);

  // 5. Handle Live Updates safely
  useEffect(() => {
    if (!candlestickSeriesRef.current || !areaSeriesRef.current) return;
    const nextCandle = normalizeBar(candle);
    if (!nextCandle) return;

    const areaTime = nextCandle.time;
    const areaPoint = { time: areaTime as any, value: nextCandle.close };

    // Bootstrap with current candle if history has not arrived yet.
    if (!isHistoryLoaded.current) {
      candlestickSeriesRef.current.setData([nextCandle]);
      areaSeriesRef.current.setData([areaPoint]);
      lastAreaTimeRef.current = areaTime;
      isHistoryLoaded.current = true;
    } else {
      candlestickSeriesRef.current.update(nextCandle);
      try {
        areaSeriesRef.current.update(areaPoint);
        lastAreaTimeRef.current = areaTime;
      } catch (e) {
        console.warn("Snake tick overlap, skipped", e);
      }
    }

    if (vwap && vwapSeriesRef.current) {
      vwapSeriesRef.current.update({
        time: nextCandle.time,
        value: vwap,
      });
    }
  }, [candle, vwap, type]);

  return (
    <div className="relative h-full w-full min-h-[260px] sm:min-h-[350px]">
      <div
        ref={chartContainerRef}
        className={`h-full w-full min-h-[260px] sm:min-h-[350px] transition-opacity duration-300 ${
          type === "snake" ? "pointer-events-none opacity-0" : "opacity-100"
        }`}
      />
      {type === "snake" ? <SnakePriceChart /> : null}
    </div>
  );
}
