'use client';

import React, { useEffect, useState } from 'react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

interface PricePoint {
  time: string;
  price: number;
}

export default function SnakePriceChart() {
  const [data, setData] = useState<PricePoint[]>([]);

  useEffect(() => {
    // Connect directly to the live Binance WebSocket for BTC/USDT trades
    const ws = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@trade');

    ws.onmessage = (event) => {
      const message = JSON.parse(event.data);
      const currentPrice = parseFloat(message.p);
      
      // Format time for the X-axis
      const timeString = new Date(message.T).toLocaleTimeString([], { 
        hour12: false, 
        hour: '2-digit', 
        minute: '2-digit', 
        second: '2-digit' 
      });

      setData((prevData) => {
        const newDataPoint = { time: timeString, price: currentPrice };
        const updatedData = [...prevData, newDataPoint];
        // Keep only the last 60 ticks to create the moving "snake" effect
        return updatedData.slice(-60);
      });
    };

    return () => {
      ws.close();
    };
  }, []);

  // Dynamically calculate the Y-axis bounds so the snake fills the container
  const prices = data.map(d => d.price);
  const minPrice = prices.length ? Math.min(...prices) : 0;
  const maxPrice = prices.length ? Math.max(...prices) : 0;
  const padding = (maxPrice - minPrice) * 0.2 || 10; 

  return (
    <div className="w-full h-64 bg-slate-900 rounded-xl p-4 border border-slate-800 shadow-lg">
      <div className="flex justify-between items-center mb-4">
        <h3 className="text-slate-300 text-sm font-bold tracking-wider uppercase">Live BTC/USDT</h3>
        <span className="relative flex h-3 w-3">
          <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
          <span className="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span>
        </span>
      </div>
      
      <div className="w-full h-48">
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={data} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
            <XAxis 
              dataKey="time" 
              stroke="#64748b" 
              fontSize={10} 
              tick={{ fill: '#64748b' }}
              tickLine={false}
              axisLine={false}
              minTickGap={20}
            />
            <YAxis 
              domain={[minPrice - padding, maxPrice + padding]} 
              stroke="#64748b" 
              fontSize={10}
              tickFormatter={(tick) => `$${tick.toLocaleString()}`}
              orientation="right"
              tickLine={false}
              axisLine={false}
              width={80}
            />
            <Tooltip 
              contentStyle={{ 
                backgroundColor: '#0f172a', 
                border: '1px solid #1e293b', 
                borderRadius: '8px', 
                color: '#f8fafc' 
              }}
              itemStyle={{ color: '#38bdf8', fontWeight: 'bold' }}
              labelStyle={{ color: '#94a3b8', marginBottom: '4px' }}
              formatter={(value: number) => [`$${value.toFixed(2)}`, 'Price']}
              isAnimationActive={false}
            />
            <Line 
              type="monotone" 
              dataKey="price" 
              stroke="#38bdf8" 
              strokeWidth={3} 
              dot={false} 
              // Turning off the Recharts animation here is crucial! 
              // It lets the array slicing handle the movement smoothly.
              isAnimationActive={false} 
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

