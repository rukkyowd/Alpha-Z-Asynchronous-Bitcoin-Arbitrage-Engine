"use client";

import React, { useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { TrendingUp, TrendingDown, AlertTriangle, Zap, Target } from "lucide-react";

type FlowEvent = {
  id: string | number;
  type: "entry" | "exit" | "sl" | "tp" | "circuit" | "info";
  text: string;
  ts?: number;
};

type OrderFlowStripProps = {
  events: FlowEvent[];
  maxVisible?: number;
};

const typeConfig: Record<FlowEvent["type"], { icon: React.ReactNode; color: string; bg: string }> = {
  entry:   { icon: <TrendingUp size={10} />,     color: "text-emerald-400",  bg: "bg-emerald-500/15 border-emerald-500/30" },
  exit:    { icon: <TrendingDown size={10} />,    color: "text-blue-400",     bg: "bg-blue-500/15 border-blue-500/30" },
  sl:      { icon: <AlertTriangle size={10} />,   color: "text-red-400",      bg: "bg-red-500/15 border-red-500/30" },
  tp:      { icon: <Target size={10} />,          color: "text-amber-400",    bg: "bg-amber-500/15 border-amber-500/30" },
  circuit: { icon: <Zap size={10} />,             color: "text-fuchsia-400",  bg: "bg-fuchsia-500/15 border-fuchsia-500/30" },
  info:    { icon: <Zap size={10} />,             color: "text-zinc-400",     bg: "bg-zinc-700/15 border-zinc-700/30" },
};

function classifyLog(log: string): FlowEvent["type"] {
  if (log.includes("[BET] BET PLACED")) return "entry";
  if (log.includes("[SL HIT]") || log.includes("STOP_LOSS_CONFIRMED")) return "sl";
  if (log.includes("[TP HIT]")) return "tp";
  if (log.includes("[EARLY EXIT]") || log.includes("[RESOLVE]")) return "exit";
  if (log.includes("[CIRCUIT]") || log.includes("[KILL SWITCH]")) return "circuit";
  return "info";
}

export function logsToFlowEvents(logs: string[]): FlowEvent[] {
  return logs
    .filter((l) => {
      const t = classifyLog(l);
      return t !== "info";
    })
    .map((l, i) => ({
      id: `${i}-${l.slice(0, 20)}`,
      type: classifyLog(l),
      text: l.replace(/^\[[\w\s]+\]\s*/, "").slice(0, 60),
      ts: Date.now() - (logs.length - i) * 1000,
    }));
}

export default function OrderFlowStrip({ events, maxVisible = 20 }: OrderFlowStripProps) {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollLeft = scrollRef.current.scrollWidth;
    }
  }, [events]);

  const visible = events.slice(-maxVisible);

  if (visible.length === 0) {
    return (
      <div className="flex h-10 items-center justify-center rounded-lg border border-dashed border-[var(--az-border)]">
        <span className="section-label">No flow events yet</span>
      </div>
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="section-label">Order Flow</span>
        <span className="data-mono text-[10px] text-[var(--az-text-dim)]">{events.length} events</span>
      </div>
      <div
        ref={scrollRef}
        className="flex gap-1.5 overflow-x-auto pb-1 scrollbar-none"
        style={{ scrollBehavior: "smooth" }}
      >
        <AnimatePresence initial={false}>
          {visible.map((ev) => {
            const cfg = typeConfig[ev.type];
            return (
              <motion.div
                key={ev.id}
                initial={{ opacity: 0, scale: 0.8, x: 20 }}
                animate={{ opacity: 1, scale: 1, x: 0 }}
                exit={{ opacity: 0, scale: 0.8 }}
                transition={{ type: "spring", stiffness: 400, damping: 25 }}
                className={`flex flex-shrink-0 items-center gap-1 rounded-md border px-2 py-1 text-[9px] font-bold ${cfg.bg} ${cfg.color} cursor-default`}
                title={ev.text}
              >
                {cfg.icon}
                <span className="max-w-[80px] truncate">{ev.text}</span>
              </motion.div>
            );
          })}
        </AnimatePresence>
      </div>
    </div>
  );
}
