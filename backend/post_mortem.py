import pandas as pd
import os
import time
from datetime import datetime

def clear_screen():
    # Clears the terminal screen (works on Windows, Mac, and Linux)
    os.system('cls' if os.name == 'nt' else 'clear')

def run_post_mortem_loop():
    csv_path = "ai_trade_history.csv"
    last_mtime = 0
    
    print(f"[*] Starting live post-mortem monitor. Waiting for updates to {csv_path}...")
    
    while True:
        try:
            if not os.path.exists(csv_path):
                time.sleep(5)
                continue

            # Check the last modified time of the CSV
            current_mtime = os.path.getmtime(csv_path)
            
            # Only read and print if the file has actually changed
            if current_mtime > last_mtime:
                df = pd.read_csv(csv_path)
                
                if not df.empty:
                    clear_screen()
                    
                    print(f"{'='*60}")
                    print(f"  ALPHA-Z LIVE POST-MORTEM DASHBOARD | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"{'='*60}")

                    # 1. IDENTIFY ANOMALIES safely
                    if 'Match Status' in df.columns:
                        mismatches = df[df['Match Status'].str.contains('MISMATCH', na=False)]
                    else:
                        print("\n[!] 'Match Status' column missing from CSV (old log version). Skipping mismatch analysis.")
                        mismatches = pd.DataFrame()
                    
                    # Calculate the Price Gap safely
                    if 'Final Price' in df.columns and 'Strike Price' in df.columns:
                        df['Price Gap'] = (df['Final Price'] - df['Strike Price']).abs()
                        high_slippage = df[df['Price Gap'] > 15.0] # Flag gaps > $15 on BTC

                        # 2. SLIPPAGE & GAP ANALYSIS
                        print(f"\n[STAT] Average settlement gap: ${df['Price Gap'].mean():.2f}")
                        if not high_slippage.empty:
                            print(f"[!] Warning: {len(high_slippage)} trades settled with high slippage (> $15).")
                    else:
                        print("\n[!] Price columns missing. Skipping slippage analysis.")

                    # 3. MISMATCH ANALYSIS (GHOST WINS)
                    if not mismatches.empty:
                        print(f"\n[!] GHOST WINS DETECTED ({len(mismatches)} total):")
                        # Only show the last 5 so the terminal doesn't get flooded
                        for _, row in mismatches.tail(5).iterrows():
                            market_slug = row.get('Market Slug', 'Unknown')
                            local_calc = row.get('Local Calc', 'N/A')
                            actual_outcome = row.get('Actual Outcome', 'N/A')
                            gap = row.get('Price Gap', 0)
                            final = row.get('Final Price', 0)
                            print(f"    - {market_slug}: Local predicted {local_calc}, but Poly settled {actual_outcome}")
                            print(f"      Gap: ${gap:.2f} | Final: ${final:.2f}")
                    elif 'Match Status' in df.columns:
                        print("\n[OK] Local calculations match all Polymarket settlements.")

                    # 4. WIN RATE BY AI DECISION
                    print("\n[AI PERFORMANCE]")
                    if 'AI Decision' in df.columns and 'Result' in df.columns:
                        ai_perf = df.groupby('AI Decision')['Result'].value_counts(normalize=True).unstack().fillna(0)
                        if 'WIN' in ai_perf.columns:
                            for decision, rate in ai_perf['WIN'].items():
                                print(f"    - {decision} Call Win Rate: {rate*100:.1f}%")
                    else:
                        print("    - Not enough data to calculate AI performance.")

                    # 5. OVERALL STATS
                    if 'Result' in df.columns:
                        total_trades = len(df)
                        total_wins = len(df[df['Result'] == 'WIN'])
                        win_rate = (total_wins / total_trades) * 100 if total_trades > 0 else 0
                        print(f"\n[OVERALL]")
                        print(f"    - Total Trades Executed: {total_trades}")
                        print(f"    - Global Win Rate: {win_rate:.1f}%")

                    print(f"\n{'='*60}")
                    print("[*] Monitoring for new trades... (Press Ctrl+C to exit)")
                
                # Update the timestamp marker
                last_mtime = current_mtime
                
            # Rest for 5 seconds before checking the file again
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\n[*] Exiting live monitor.")
            break
        except Exception as e:
            print(f"\n[!] Error updating monitor: {e}")
            time.sleep(5)

if __name__ == "__main__":
    run_post_mortem_loop()