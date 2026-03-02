"use client";

import React from "react";
import { Sparkles, AlertCircle, TrendingDown, TrendingUp, CheckCircle2, AlertTriangle, Zap, Target } from "lucide-react";
import { motion } from "framer-motion";

export default function AiInsights({ insights }: { insights: any[] }) {
  if (!insights || insights.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-10 border border-dashed border-zinc-800 rounded-xl">
        <Sparkles size={32} className="text-zinc-700 mb-3 animate-pulse" />
        <div className="text-xs text-zinc-600 text-center font-mono uppercase tracking-wide">
          Gathering execution data
          <div className="text-[10px] opacity-60 mt-1">Analysis pending first trades</div>
        </div>
      </div>
    );
  }

  const getIcon = (type: string) => {
    switch (type) {
      case "warning": 
        return <AlertTriangle size={16} className="text-amber-400" />;
      case "positive": 
        return <CheckCircle2 size={16} className="text-emerald-400" />;
      case "critical":
        return <AlertCircle size={16} className="text-red-400" />;
      case "opportunity":
        return <Target size={16} className="text-blue-400" />;
      case "performance":
        return <TrendingUp size={16} className="text-green-400" />;
      default: 
        return <Sparkles size={16} className="text-violet-400" />;
    }
  };

  const getColor = (type: string) => {
    switch (type) {
      case "warning": 
        return "bg-amber-500/10 border-amber-500/30 hover:border-amber-500/50";
      case "positive": 
        return "bg-emerald-500/10 border-emerald-500/30 hover:border-emerald-500/50";
      case "critical":
        return "bg-red-500/10 border-red-500/30 hover:border-red-500/50";
      case "opportunity":
        return "bg-blue-500/10 border-blue-500/30 hover:border-blue-500/50";
      case "performance":
        return "bg-green-500/10 border-green-500/30 hover:border-green-500/50";
      default: 
        return "bg-violet-500/10 border-violet-500/30 hover:border-violet-500/50";
    }
  };

  return (
    <div className="space-y-3">
      {insights.map((insight, i) => (
        <motion.div 
          key={i}
          initial={{ opacity: 0, x: -20, scale: 0.95 }}
          animate={{ opacity: 1, x: 0, scale: 1 }}
          transition={{ 
            delay: i * 0.1,
            type: "spring",
            stiffness: 300,
            damping: 20
          }}
          className={`relative p-4 border rounded-xl flex items-start gap-3 group transition-all ${getColor(insight.type)}`}
        >
          {/* Glow effect on hover */}
          <div className="absolute inset-0 bg-gradient-to-r from-transparent via-white/5 to-transparent opacity-0 group-hover:opacity-100 transition-opacity rounded-xl" />
          
          {/* Icon */}
          <div className="relative z-10 mt-0.5 flex-shrink-0">
            {getIcon(insight.type)}
          </div>
          
          {/* Content */}
          <div className="relative z-10 flex-1">
            <div className="text-[11px] leading-relaxed font-medium text-zinc-200">
              {insight.text}
            </div>
            
            {/* Optional timestamp */}
            {insight.timestamp && (
              <div className="text-[9px] text-zinc-600 font-mono mt-1">
                {insight.timestamp}
              </div>
            )}
          </div>

          {/* Priority badge if high importance */}
          {insight.priority === "high" && (
            <div className="relative z-10 px-2 py-0.5 bg-red-500/20 border border-red-500/30 rounded text-[9px] font-bold text-red-400 uppercase tracking-wide">
              High
            </div>
          )}
        </motion.div>
      ))}

      {/* Summary stats if available */}
      {insights.length > 5 && (
        <motion.div 
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: insights.length * 0.1 + 0.2 }}
          className="pt-3 border-t border-zinc-800 flex items-center justify-between text-[10px] text-zinc-600 font-mono"
        >
          <span>Total Insights: {insights.length}</span>
          <span className="flex items-center gap-2">
            <Zap size={12} className="text-blue-400" />
            AI Analysis Active
          </span>
        </motion.div>
      )}
    </div>
  );
}
