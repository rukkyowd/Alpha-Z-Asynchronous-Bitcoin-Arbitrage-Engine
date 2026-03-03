import os
import sqlite3
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
            WHEN trigger_reason LIKE '%AI confirmed%' THEN '🤖 AI-Confirmed Trades'
            WHEN trigger_reason LIKE '%AI vetoed%' THEN '🛑 AI-Vetoed (Skipped)'
            ELSE '⚙️ System-Only Trades (High Conviction)'
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
            print(" 📊 ALPHA Z: AI EDGE ATTRIBUTION REPORT")
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
    print(" 📈 ALPHA-Z CORE PERFORMANCE REPORT")
    print("="*80)
    print(f" Net PnL            : ${total_pnl:+.2f}")
    print(f" Core Win Rate      : {win_rate:.2f}% ({wins}/{total_trades})")
    
    # --- REGIME ANALYSIS ---
    print("-" * 80)
    print(" 🎯 EARLY EXIT & REGIME BREAKDOWN")
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
        print(f"\n⚠️  ALERT: Detected {count} Resolution Mismatches")
        print("This usually indicates high volatility during the expiry window.")
    else:
        print("\n✅ Payout Integrity: Local calc matched Poly Official 100%.")
    print("="*80 + "\n")

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
        
        # 5. Visual Output
        see_plot = input("View Equity Curve Plot? (y/n): ").strip().lower()
        if see_plot == 'y':
            plot_equity_curve(trades_df)