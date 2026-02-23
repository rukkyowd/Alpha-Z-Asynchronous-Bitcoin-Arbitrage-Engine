"use client";

import React from "react";
import { Sparkles, AlertCircle, TrendingDown, CheckCircle2 } from "lucide-react";
import { motion } from "framer-motion";

export default function AiInsights({ insights }: { insights: any[] }) {
  if (!insights || insights.length === 0) return (
    <div className="text-[10px] text-zinc-600 text-center py-6 border border-dashed border-zinc-800 rounded-xl">
      Gathering execution data for AI analysis...
    </div>
  );

  const getIcon = (type: string) => {
    switch (type) {
      case "warning": return <TrendingDown size={14} className="text-red-400" />;
      case "positive": return <CheckCircle2 size={14} className="text-green-400" />;
      default: return <Sparkles size={14} className="text-blue-400" />;
    }
  };

  return (
    <div className="space-y-3">
      {insights.map((insight, i) => (
        <motion.div 
          key={i}
          initial={{ opacity: 0, x: -10 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: i * 0.1 }}
          className="p-3 bg-[#09090b] border border-zinc-800/50 rounded-xl flex items-start gap-3 group hover:border-zinc-700 transition-all"
        >
          <div className="mt-0.5">{getIcon(insight.type)}</div>
          <div className="text-[11px] leading-relaxed font-medium text-zinc-300">
            {insight.text}
          </div>
        </motion.div>
      ))}
    </div>
  );
}