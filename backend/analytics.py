import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# ============================================================
# ALPHA-Z QUANTITATIVE ANALYSIS TOOL
# ============================================================

def load_data(db_path="alpha_z_history.db"):
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

def run_performance_report(df):
    """Generates high-level metrics and Market Regime breakdown."""
    # Filter out TIEs and Dust to see core performance
    core_trades = df[~df['result'].str.contains("DUST|TIE", na=False)].copy()
    
    total_trades = len(core_trades)
    wins = len(core_trades[core_trades['result'] == 'WIN'])
    total_pnl = df['pnl_impact'].sum()
    win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0

    print("\n" + "="*45)
    print("      ALPHA-Z QUANT PERFORMANCE REPORT")
    print("="*45)
    print(f"Net PnL            : ${total_pnl:+.2f}")
    print(f"Core Win Rate      : {win_rate:.2f}% ({wins}/{total_trades})")
    
    # --- REGIME ANALYSIS ---
    # We parse the 'local_calc_outcome' or 'reason' if you logged regime there, 
    # but based on your core.py, the AI prompt uses 'regime'. 
    # Let's check the 'local_calc_outcome' which often stores 'SIGNAL_REVERSAL' etc.
    
    print("-" * 45)
    print("      MARKET REGIME BREAKDOWN")
    print("-" * 45)
    
    # We'll group by the result of our technical context
    # Note: If you haven't explicitly saved 'TRENDING'/'RANGING' to a column yet, 
    # this script will scan your 'match_status' and 'local_calc_outcome' for keywords.
    
    regimes = ['SIGNAL_REVERSAL', 'TAKE_PROFIT', 'STOP_LOSS']
    for r in regimes:
        subset = df[df['local_calc_outcome'].str.contains(r, na=False)]
        if not subset.empty:
            r_wins = len(subset[subset['result'] == 'WIN'])
            r_wr = (r_wins / len(subset)) * 100
            r_pnl = subset['pnl_impact'].sum()
            print(f"{r:16} | WR: {r_wr:5.1f}% | PnL: ${r_pnl:+.2f}")

    print("="*45)
    
def analyze_mismatches(df):
    """Analyzes discrepancy between local Binance calc and Poly official."""
    mismatches = df[df['match_status'].str.contains("MISMATCH", na=False)]
    count = len(mismatches)
    
    if count > 0:
        print(f"⚠️  ALERT: Detected {count} Resolution Mismatches")
        print("This usually indicates high volatility during the 5m expiry window.")
    else:
        print("✅ Payout Integrity: Local calc matched Poly Official 100%.")

def plot_equity_curve(df):
    """Visualizes account growth over time."""
    df = df.sort_values('timestamp')
    df['cumulative_pnl'] = df['pnl_impact'].cumsum()
    
    plt.figure(figsize=(10, 5))
    plt.plot(df['timestamp'], df['cumulative_pnl'], label='Cumulative PnL', color='#00ff41')
    plt.fill_between(df['timestamp'], df['cumulative_pnl'], color='#00ff41', alpha=0.1)
    
    plt.title('Alpha-Z Equity Curve')
    plt.xlabel('Time')
    plt.ylabel('USDC Profit/Loss')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.show()

if __name__ == "__main__":
    trades_df = load_data()
    
    if trades_df is not None:
        run_performance_report(trades_df)
        analyze_mismatches(trades_df)
        
        # Ask user if they want to see the graph
        see_plot = input("\nView Equity Curve Plot? (y/n): ").lower()
        if see_plot == 'y':
            plot_equity_curve(trades_df)