import os
import sqlite3
import json
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# ============================================================
# ALPHA-Z QUANTITATIVE ANALYSIS TOOL
# ============================================================

# Dynamically get the directory where this script is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB_PATH = os.path.join(BASE_DIR, "alpha_z_history.db")

def load_data(db_path=DEFAULT_DB_PATH):
    """Connects to the SQLite DB and returns a cleaned DataFrame."""
    try:
        conn = sqlite3.connect(db_path)
        # Pull all trades
        df = pd.read_sql_query("SELECT * FROM trades", conn)
        conn.close()
        
        if df.empty:
            print("(!) Database is empty. Go place some trades first!")
            return None
            
        # Convert timestamp to datetime objects for time-series analysis
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        print(f"(X) Error loading database: {e}")
        return None

def run_edge_attribution(db_path=DEFAULT_DB_PATH):
    """Analyzes the exact win rate and PnL impact of the Local AI vs System."""
    query = """
    SELECT 
        CASE 
            WHEN trigger_reason LIKE '%AI confirmed%' THEN 'AI-Confirmed Trades'
            WHEN trigger_reason LIKE '%AI vetoed%' THEN 'AI-Vetoed (Skipped)'
            ELSE 'System-Only Trades (High Conviction)'
        END AS category,
        COUNT(*) as total_executed,
        SUM(CASE WHEN result LIKE '%WIN%' THEN 1 ELSE 0 END) as wins,
        SUM(CASE WHEN result LIKE '%LOSS%' THEN 1 ELSE 0 END) as losses,
        ROUND(SUM(CASE WHEN result LIKE '%WIN%' THEN 1.0 ELSE 0.0 END) / COUNT(*) * 100, 2) as win_rate_pct,
        ROUND(SUM(pnl_impact), 2) as net_pnl
    FROM trades
    WHERE result LIKE '%WIN%' OR result LIKE '%LOSS%'
    GROUP BY category
    ORDER BY total_executed DESC;
    """
    try:
        with sqlite3.connect(db_path) as conn:
            df = pd.read_sql_query(query, conn)
            
            print("\n" + "="*80)
            print(" ALPHA Z: AI EDGE ATTRIBUTION REPORT")
            print("="*80)
            
            if df.empty:
                print(" Not enough resolved trades for AI attribution yet.")
            else:
                # Format the dataframe for cleaner terminal output
                df['win_rate_pct'] = df['win_rate_pct'].apply(lambda x: f"{x}%")
                df['net_pnl'] = df['net_pnl'].apply(lambda x: f"${x:+.2f}")
                df.columns = ['Trade Category', 'Total Trades', 'Wins', 'Losses', 'Win Rate', 'Net PnL']
                print(df.to_string(index=False))
            
    except sqlite3.OperationalError:
        print("\n[!] Could not run AI attribution. (Have you run the DB migration to add 'trigger_reason'?)")

def run_performance_report(df):
    """Generates high-level metrics and Market Regime breakdown."""
    # Filter out TIEs and Dust to see core performance
    core_trades = df[~df['result'].str.contains("DUST|TIE", na=False)].copy()
    
    total_trades = len(core_trades)
    wins = len(core_trades[core_trades['result'] == 'WIN'])
    total_pnl = df['pnl_impact'].sum()
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0

    print("\n" + "="*80)
    print(" ALPHA-Z CORE PERFORMANCE REPORT")
    print("="*80)
    print(f" Net PnL            : ${total_pnl:+.2f}")
    print(f" Core Win Rate      : {win_rate:.2f}% ({wins}/{total_trades})")
    
    # --- REGIME ANALYSIS ---
    print("-" * 80)
    print(" EARLY EXIT & REGIME BREAKDOWN")
    print("-" * 80)
    
    regimes = ['SIGNAL_REVERSAL', 'TAKE_PROFIT', 'STOP_LOSS']
    found_any = False
    
    for r in regimes:
        subset = df[df['local_calc_outcome'].str.contains(r, na=False)]
        if not subset.empty:
            found_any = True
            r_wins = len(subset[subset['result'] == 'WIN'])
            r_wr = (r_wins / len(subset)) * 100
            r_pnl = subset['pnl_impact'].sum()
            print(f" {r:16} | WR: {r_wr:5.1f}% | PnL: ${r_pnl:+.2f} | Trades: {len(subset)}")
            
    if not found_any:
        print(" No early exits (Take Profit/Stop Loss) triggered yet.")

    print("="*80)
    
def analyze_mismatches(df):
    """Analyzes discrepancy between local Binance calc and Poly official."""
    mismatches = df[df['match_status'].str.contains("MISMATCH", na=False)]
    count = len(mismatches)
    
    if count > 0:
        print(f"\n ALERT: Detected {count} Resolution Mismatches")
        print("This usually indicates high volatility during the expiry window.")
    else:
        print("\n Payout Integrity: Local calc matched Poly Official 100%.")
    print("="*80 + "\n")

def _parse_metadata_blob(raw_value):
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, str) and raw_value.strip():
        try:
            parsed = json.loads(raw_value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}

def run_probability_calibration_report(df):
    """Computes calibration diagnostics with Model vs Market Brier benchmarking."""
    if 'metadata_json' not in df.columns:
        print("\n Probability Calibration: metadata_json column missing.")
        print("="*80)
        return

    resolved = df[df['result'].isin(['WIN', 'LOSS'])].copy()
    if resolved.empty:
        print("\n Probability Calibration: no resolved trades yet.")
        print("="*80)
        return

    metadata = resolved['metadata_json'].apply(_parse_metadata_blob)
    resolved['predicted_win_prob_pct'] = metadata.apply(lambda item: float(item.get('predicted_win_prob_pct', np.nan)))
    resolved['fair_market_probability_pct'] = metadata.apply(lambda item: float(item.get('fair_market_probability_pct', np.nan)))
    resolved['raw_market_probability_pct'] = metadata.apply(lambda item: float(item.get('raw_market_probability_pct', np.nan)))
    resolved['base_up_probability_pct'] = metadata.apply(lambda item: float(item.get('base_up_probability_pct', np.nan)))
    resolved['indicator_logit_shift'] = metadata.apply(lambda item: float(item.get('indicator_logit_shift', np.nan)))
    resolved['calibrated_prob_pct'] = metadata.apply(lambda item: float(item.get('calibrated_prob_pct', np.nan)))
    resolved['outcome_numeric'] = resolved['result'].apply(lambda value: 1.0 if value == 'WIN' else 0.0)

    calibration = resolved.dropna(subset=['predicted_win_prob_pct']).copy()
    if calibration.empty:
        print("\n Probability Calibration: no stored prediction probabilities yet.")
        print("="*80)
        return

    # --- Model Brier Score ---
    calibration['model_prob'] = calibration['predicted_win_prob_pct'] / 100.0
    calibration['model_brier'] = (calibration['model_prob'] - calibration['outcome_numeric']) ** 2
    model_brier = calibration['model_brier'].mean()

    # --- Market Brier Score (benchmark) ---
    market_cal = calibration.dropna(subset=['fair_market_probability_pct']).copy()
    if not market_cal.empty:
        market_cal['market_prob'] = market_cal['fair_market_probability_pct'] / 100.0
        market_cal['market_brier'] = (market_cal['market_prob'] - market_cal['outcome_numeric']) ** 2
        market_brier = market_cal['market_brier'].mean()
        brier_advantage = market_brier - model_brier
    else:
        market_brier = np.nan
        brier_advantage = np.nan

    # --- Rolling Brier (last 50 trades) ---
    rolling_n = min(50, len(calibration))
    recent = calibration.tail(rolling_n)
    rolling_model_brier = recent['model_brier'].mean()
    rolling_market_brier = np.nan
    if not market_cal.empty:
        recent_market = market_cal.tail(rolling_n)
        if not recent_market.empty:
            rolling_market_brier = (recent_market['market_prob'] - recent_market['outcome_numeric']).pow(2).mean()

    avg_pred = calibration['predicted_win_prob_pct'].mean()
    realized_wr = calibration['outcome_numeric'].mean() * 100.0
    avg_edge = (calibration['predicted_win_prob_pct'] - calibration['fair_market_probability_pct']).mean()

    print("\n" + "="*80)
    print(" PROBABILITY CALIBRATION & BRIER BENCHMARK REPORT")
    print("="*80)

    print("\n  BRIER SCORES (lower is better, 0.25 = coin flip)")
    print("-" * 80)
    print(f"  Alpha-Z Model (all)    : {model_brier:.4f}")
    if not np.isnan(market_brier):
        print(f"  Polymarket Crowd (all) : {market_brier:.4f}")
        sign = "+" if brier_advantage > 0 else ""
        verdict = "MODEL WINS" if brier_advantage > 0 else "MARKET WINS"
        print(f"  Advantage              : {sign}{brier_advantage:.4f}  [{verdict}]")
    print(f"\n  Rolling (last {rolling_n}):")
    print(f"    Model  : {rolling_model_brier:.4f}")
    if not np.isnan(rolling_market_brier):
        print(f"    Market : {rolling_market_brier:.4f}")
    print("-" * 80)
    print(f"  Avg Predicted Win  : {avg_pred:.2f}%")
    print(f"  Realized Win Rate  : {realized_wr:.2f}%")
    print(f"  Avg Edge vs Fair   : {avg_edge:+.2f}%")
    print(f"  Total Resolved     : {len(calibration)}")

    # --- Per-bucket calibration with market benchmark ---
    bins = [0, 40, 50, 60, 70, 80, 100]
    calibration['bucket'] = pd.cut(calibration['predicted_win_prob_pct'], bins=bins, right=False, include_lowest=True)
    bucket_rows = []
    for bucket, group in calibration.groupby('bucket', observed=False):
        if group.empty:
            continue
        mkt_pred = group['fair_market_probability_pct'].mean()
        bucket_rows.append({
            'Bucket': str(bucket),
            'N': len(group),
            'Model %': round(group['predicted_win_prob_pct'].mean(), 1),
            'Mkt %': round(mkt_pred, 1) if not np.isnan(mkt_pred) else '-',
            'Real %': round(group['outcome_numeric'].mean() * 100.0, 1),
            'Brier': round(group['model_brier'].mean(), 4),
        })

    print("\n  CALIBRATION BY PREDICTION BUCKET")
    print("-" * 80)
    if bucket_rows:
        print(pd.DataFrame(bucket_rows).to_string(index=False))
    else:
        print("  Not enough bucketed trades yet.")

    # --- Edge magnitude analysis ---
    edge_cal = calibration.dropna(subset=['fair_market_probability_pct']).copy()
    if len(edge_cal) >= 5:
        edge_cal['edge_pct'] = edge_cal['predicted_win_prob_pct'] - edge_cal['fair_market_probability_pct']
        edge_bins = [0, 3, 6, 10, 15, 100]
        edge_labels = ['0-3%', '3-6%', '6-10%', '10-15%', '15%+']
        edge_cal['edge_bucket'] = pd.cut(
            edge_cal['edge_pct'].abs(), bins=edge_bins, labels=edge_labels,
            right=False, include_lowest=True,
        )
        edge_rows = []
        for ebucket, egroup in edge_cal.groupby('edge_bucket', observed=False):
            if egroup.empty:
                continue
            avg_pnl = round(egroup['pnl_impact'].mean(), 2) if 'pnl_impact' in egroup.columns else '-'
            edge_rows.append({
                'Edge': str(ebucket),
                'N': len(egroup),
                'Win %': round(egroup['outcome_numeric'].mean() * 100.0, 1),
                'Avg PnL': avg_pnl,
                'Brier': round(egroup['model_brier'].mean(), 4),
            })
        if edge_rows:
            print("\n  PERFORMANCE BY EDGE MAGNITUDE")
            print("-" * 80)
            print(pd.DataFrame(edge_rows).to_string(index=False))

    print("=" * 80)


def plot_equity_curve(df):
    """Visualizes account growth over time."""
    df = df.sort_values('timestamp')
    df['cumulative_pnl'] = df['pnl_impact'].cumsum()
    
    # Create the plot
    plt.style.use('dark_background')
    plt.figure(figsize=(10, 5))
    
    # Determine color based on overall profitability
    is_profitable = df['cumulative_pnl'].iloc[-1] >= 0 if not df.empty else True
    line_color = '#00ff41' if is_profitable else '#ff003c'
    
    plt.plot(df['timestamp'], df['cumulative_pnl'], label='Cumulative PnL', color=line_color, linewidth=2)
    plt.fill_between(df['timestamp'], df['cumulative_pnl'], color=line_color, alpha=0.1)
    
    plt.title('Alpha-Z Equity Curve', fontsize=14, pad=15)
    plt.xlabel('Time', fontsize=10)
    plt.ylabel('USDC Profit/Loss', fontsize=10)
    
    # Format grid and legend
    plt.grid(True, alpha=0.2, linestyle='--')
    plt.axhline(y=0, color='white', alpha=0.3, linestyle='-')
    plt.legend()
    plt.tight_layout()
    
    plt.show()

if __name__ == "__main__":
    # 1. Load the data
    trades_df = load_data()
    
    if trades_df is not None:
        # 2. Run AI Attribution (from previous step)
        run_edge_attribution()
        
        # 3. Run Core Performance & Exits
        run_performance_report(trades_df)
        
        # 4. Check Resolution Integrity
        analyze_mismatches(trades_df)

        # 5. Probability calibration
        run_probability_calibration_report(trades_df)
        
        # 6. Visual Output
        if sys.stdin.isatty():
            try:
                see_plot = input("View Equity Curve Plot? (y/n): ").strip().lower()
            except EOFError:
                see_plot = "n"
            if see_plot == 'y':
                plot_equity_curve(trades_df)
