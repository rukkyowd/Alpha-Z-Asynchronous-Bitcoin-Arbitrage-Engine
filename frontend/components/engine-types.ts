export type EngineDirection = "UP" | "DOWN" | "SKIP" | "UNKNOWN";

export interface PriceBar {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface MarketCandleSnapshot {
  timestamp?: string;
  time?: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume?: number;
}

export interface TechnicalContextSnapshot {
  timestamp?: string;
  price: number;
  strike_price: number;
  vwap: number;
  vwap_distance: number;
  price_vs_vwap_pct: number;
  ema_9: number;
  ema_21: number;
  ema_spread_pct: number;
  rsi_14: number;
  cvd_total: number;
  cvd_candle_delta: number;
  cvd_1min_delta: number;
  current_volume: number;
  vol_sma_20: number;
  parkinson_volatility: number;
  garman_klass_volatility: number;
  realized_volatility: number;
  adaptive_atr_floor: number;
  adaptive_cvd_threshold: number;
  market_regime: string;
  bayesian_logit: number;
  bayesian_probability: number;
  expected_move_sigma: number;
  expected_move_t: number;
}

export interface EngineMarketSnapshot {
  target_slug?: string;
  market_family_prefix?: string;
  live_price: number;
  live_candle?: Partial<MarketCandleSnapshot> | null;
  candle_history?: Array<Partial<MarketCandleSnapshot>>;
  last_closed_kline_ms?: number;
  last_agg_trade_ms?: number;
  cvd_total?: number;
  cvd_snapshot_at_candle_open?: number;
  last_cvd_1min?: number;
  vwap_cum_pv?: number;
  vwap_cum_vol?: number;
  vwap_date?: string;
  latest_context?: Partial<TechnicalContextSnapshot> | null;
}

export interface EnginePosition {
  slug: string;
  decision: EngineDirection | string;
  token_id?: string;
  strike: number;
  bet_size_usd: number;
  bought_price: number;
  status?: string;
  score?: number;
  bonus_score?: number;
  entry_time?: string;
  mark_price: number;
  current_token_price: number;
  live_underlying_price: number;
  entry_underlying_price: number;
  tp_delta: number;
  sl_delta: number;
  tp_token_price: number;
  sl_token_price: number;
  seconds_remaining: number;
  sl_disabled?: boolean;
  sl_breach_count?: number;
  tp_gate_logged?: boolean;
  tp_armed?: boolean;
  tp_peak_delta?: number;
  tp_lock_floor_delta?: number;
  signals?: string[];
  notes?: string[];
  ml_features?: Record<string, unknown>;
}

export interface EdgeTrackerSnapshot {
  slug: string;
  direction: string;
  up_math_prob: number;
  down_math_prob: number;
  up_poly_prob: number;
  down_poly_prob: number;
  up_public_prob?: number;
  down_public_prob?: number;
  up_edge: number;
  down_edge: number;
  best_edge: number;
  best_ev_pct: number;
}

export interface SignalAlignmentSnapshot {
  direction: string;
  score: number;
  max_score: number;
  vwap: boolean;
  rsi: boolean;
  volume: boolean;
  cvd: boolean;
}

export interface ExecutionTimingSnapshot {
  signal_generation_ms: number;
  ai_inference_ms: number;
  clob_request_ms: number;
  confirmation_ms: number;
  total_ms: number;
  updated_at: number;
}

export interface CvdGaugeSnapshot {
  delta: number;
  one_min_delta: number;
  threshold: number;
  divergence: string;
  divergence_strength: number;
}

export interface AdaptiveThresholdSnapshot {
  atr_min: number;
  cvd_threshold: number;
  volatility_lookback?: number;
}

export interface AiInteractionSnapshot {
  prompt: string;
  response: string;
  timestamp: string;
}

export interface EngineTelemetrySnapshot {
  edge_snapshot?: Partial<EdgeTrackerSnapshot>;
  signal_alignment?: Partial<SignalAlignmentSnapshot>;
  cvd_snapshot?: Partial<CvdGaugeSnapshot>;
  execution_timing?: Partial<ExecutionTimingSnapshot>;
  adaptive_thresholds?: Partial<AdaptiveThresholdSnapshot>;
  last_ai_interaction?: Partial<AiInteractionSnapshot>;
  context?: Partial<TechnicalContextSnapshot> | null;
}

export interface EngineRuntimeSnapshot {
  total_wins?: number;
  total_losses?: number;
  ai_call_count?: number;
  ai_consecutive_failures?: number;
  ai_circuit_open_until?: number;
  ai_call_in_flight?: string;
  last_ai_response_ms?: number;
  ai_response_ema_ms?: number;
  kill_switch_enabled?: boolean;
  simulated_balance?: number;
  latest_rejected_reason?: string;
  paper_trading?: boolean;
  logs?: string[];
}

export interface DrawdownGuardSnapshot {
  bankroll?: number;
  current_balance?: number;
  daily_pnl?: number;
  max_trade_pct?: number;
  max_daily_loss_pct?: number;
  trades_this_hour?: number;
  regime?: string;
  text?: string;
  drawdown_used?: number;
  drawdown_room_left?: number;
  max_bet_cap?: number;
}

export interface EngineWsPayload {
  market?: Partial<EngineMarketSnapshot>;
  positions?: EnginePosition[] | Record<string, EnginePosition>;
  telemetry?: Partial<EngineTelemetrySnapshot>;
  runtime?: Partial<EngineRuntimeSnapshot>;
  drawdown_guard?: Partial<DrawdownGuardSnapshot>;
  engine_health?: Record<string, unknown>;
  engine_control?: Record<string, unknown>;
  portfolio?: Record<string, unknown>;
}

export interface DashboardQuantSnapshot {
  bayesian_probability: number;
  garman_klass_volatility: number;
  cvd_candle_delta: number;
  expected_move_sigma: number;
}

export interface DashboardLiveData {
  market: EngineMarketSnapshot;
  positions: Record<string, EnginePosition>;
  telemetry: EngineTelemetrySnapshot;
  runtime: EngineRuntimeSnapshot;
  drawdown_guard: DrawdownGuardSnapshot;
  price: number;
  vwap: number;
  history: PriceBar[];
  active_trades: Record<string, EnginePosition>;
  candle: PriceBar | null;
  is_paper: boolean;
  logs: string[];
  regime: string;
  atr: number;
  seconds_remaining: number;
  ai_log: AiInteractionSnapshot;
  strategy: {
    edge_tracker: EdgeTrackerSnapshot;
    signal_alignment: SignalAlignmentSnapshot;
    execution_latency: ExecutionTimingSnapshot;
    cvd_gauge: CvdGaugeSnapshot;
    adaptive_thresholds: AdaptiveThresholdSnapshot;
    system_locks: {
      locks: string[];
      ai_circuit_open: boolean;
      ai_circuit_remaining_secs: number;
      ai_failures: number;
      ai_in_flight: boolean;
    };
    drawdown_guard: DrawdownGuardSnapshot;
  };
  quant: DashboardQuantSnapshot;
}

export const DEFAULT_DASHBOARD_LIVE_DATA: DashboardLiveData = {
  market: {
    live_price: 0,
    live_candle: null,
    candle_history: [],
  },
  positions: {},
  telemetry: {
    edge_snapshot: {
      slug: "",
      direction: "UNKNOWN",
      up_math_prob: 0,
      down_math_prob: 0,
      up_poly_prob: 0,
      down_poly_prob: 0,
      up_public_prob: 0,
      down_public_prob: 0,
      up_edge: 0,
      down_edge: 0,
      best_edge: 0,
      best_ev_pct: 0,
    },
    signal_alignment: {
      direction: "UNKNOWN",
      score: 0,
      max_score: 4,
      vwap: false,
      rsi: false,
      volume: false,
      cvd: false,
    },
    execution_timing: {
      signal_generation_ms: 0,
      ai_inference_ms: 0,
      clob_request_ms: 0,
      confirmation_ms: 0,
      total_ms: 0,
      updated_at: 0,
    },
    cvd_snapshot: {
      delta: 0,
      one_min_delta: 0,
      threshold: 0,
      divergence: "NONE",
      divergence_strength: 0,
    },
    adaptive_thresholds: {
      atr_min: 0,
      cvd_threshold: 0,
      volatility_lookback: 0,
    },
    last_ai_interaction: {
      prompt: "No AI calls yet.",
      response: "N/A",
      timestamp: "",
    },
    context: null,
  },
  runtime: {
    total_wins: 0,
    total_losses: 0,
    ai_call_count: 0,
    ai_consecutive_failures: 0,
    ai_circuit_open_until: 0,
    ai_call_in_flight: "",
    last_ai_response_ms: 0,
    ai_response_ema_ms: 0,
    kill_switch_enabled: false,
    simulated_balance: 0,
    latest_rejected_reason: "",
    paper_trading: true,
    logs: [],
  },
  drawdown_guard: {
    bankroll: 0,
    current_balance: 0,
    daily_pnl: 0,
    max_trade_pct: 0.05,
    max_daily_loss_pct: 0.2,
    trades_this_hour: 0,
    regime: "NORMAL",
    text: "",
    drawdown_used: 0,
    drawdown_room_left: 0,
    max_bet_cap: 0,
  },
  price: 0,
  vwap: 0,
  history: [],
  active_trades: {},
  candle: null,
  is_paper: true,
  logs: [],
  regime: "UNKNOWN",
  atr: 0,
  seconds_remaining: 0,
  ai_log: {
    prompt: "No AI calls yet.",
    response: "N/A",
    timestamp: "",
  },
  strategy: {
    edge_tracker: {
      slug: "",
      direction: "UNKNOWN",
      up_math_prob: 0,
      down_math_prob: 0,
      up_poly_prob: 0,
      down_poly_prob: 0,
      up_public_prob: 0,
      down_public_prob: 0,
      up_edge: 0,
      down_edge: 0,
      best_edge: 0,
      best_ev_pct: 0,
    },
    signal_alignment: {
      direction: "UNKNOWN",
      score: 0,
      max_score: 4,
      vwap: false,
      rsi: false,
      volume: false,
      cvd: false,
    },
    execution_latency: {
      signal_generation_ms: 0,
      ai_inference_ms: 0,
      clob_request_ms: 0,
      confirmation_ms: 0,
      total_ms: 0,
      updated_at: 0,
    },
    cvd_gauge: {
      delta: 0,
      one_min_delta: 0,
      threshold: 0,
      divergence: "NONE",
      divergence_strength: 0,
    },
    adaptive_thresholds: {
      atr_min: 0,
      cvd_threshold: 0,
      volatility_lookback: 0,
    },
    system_locks: {
      locks: [],
      ai_circuit_open: false,
      ai_circuit_remaining_secs: 0,
      ai_failures: 0,
      ai_in_flight: false,
    },
    drawdown_guard: {
      bankroll: 0,
      current_balance: 0,
      daily_pnl: 0,
      max_trade_pct: 0.05,
      max_daily_loss_pct: 0.2,
      trades_this_hour: 0,
      regime: "NORMAL",
      text: "",
      drawdown_used: 0,
      drawdown_room_left: 0,
      max_bet_cap: 0,
    },
  },
  quant: {
    bayesian_probability: 0,
    garman_klass_volatility: 0,
    cvd_candle_delta: 0,
    expected_move_sigma: 0,
  },
};
