#!/usr/bin/env python3
"""
Complete Intraday Trail Backtest with CSV Export
All trades are closed by market close (3:30 PM) on the same day
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Install required packages
try:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'openpyxl', '--quiet'])
    print("✅ Required packages installed")
except:
    print("⚠️ Package installation may have failed, continuing anyway...")

class TrailBacktest:
    def __init__(self, capital=1000000, stop_loss_pct=0.5, profit_target_pct=1.0):
        """Initialize backtest parameters"""
        self.initial_capital = capital
        self.current_capital = capital
        self.stop_loss_pct = stop_loss_pct / 100
        self.profit_target_pct = profit_target_pct / 100
        
        self.trades = []
        self.trade_summary = {
            'total_trades': 0,
            'winning_trades': 0,
            'losing_trades': 0,
            'total_pnl': 0,
            'max_drawdown': 0,
            'win_rate': 0
        }
        
    def load_data(self):
        """Load predictions and OHLC data"""
        print("Loading data...")
        
        try:
            # Load Excel predictions
            self.calls_predictions = pd.read_excel('option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Calls')
            self.puts_predictions = pd.read_excel('option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Puts')
            print(f"Loaded {len(self.calls_predictions)} calls predictions")
            print(f"Loaded {len(self.puts_predictions)} puts predictions")
        except Exception as e:
            print(f"Error loading Excel file: {e}")
            return False
            
        try:
            # Load OHLC data
            self.calls_ohlc = pd.read_csv('calls_ohlc_data_20250716_161308.csv')
            self.puts_ohlc = pd.read_csv('puts_ohlc_data_20250716_161308.csv')
            
            # Convert timestamp to datetime
            self.calls_ohlc['timestamp'] = pd.to_datetime(self.calls_ohlc['timestamp'])
            self.puts_ohlc['timestamp'] = pd.to_datetime(self.puts_ohlc['timestamp'])
            
            print(f"Loaded {len(self.calls_ohlc)} calls OHLC records")
            print(f"Loaded {len(self.puts_ohlc)} puts OHLC records")
            
        except Exception as e:
            print(f"Error loading OHLC data: {e}")
            return False
            
        return True
    
    def find_matching_ohlc(self, stock_symbol, prediction_time, ohlc_data, tolerance_minutes=5):
        """Find OHLC data matching the stock and time from predictions"""
        stock_data = ohlc_data[ohlc_data['stock'] == stock_symbol].copy()
        
        if len(stock_data) == 0:
            return None
            
        stock_data['time_diff'] = abs(stock_data['timestamp'] - prediction_time)
        closest_match = stock_data[stock_data['time_diff'] <= timedelta(minutes=tolerance_minutes)]
        
        if len(closest_match) == 0:
            return None
            
        return closest_match.loc[closest_match['time_diff'].idxmin()]
    
    def simulate_trade(self, entry_price, trade_type, stock_symbol, entry_time, ohlc_data):
        """Simulate an intraday trade with stop loss and profit target"""
        if trade_type == 'long':
            stop_loss_price = entry_price * (1 - self.stop_loss_pct)
            profit_target_price = entry_price * (1 + self.profit_target_pct)
        else:  # short
            stop_loss_price = entry_price * (1 + self.stop_loss_pct)
            profit_target_price = entry_price * (1 - self.profit_target_pct)
        
        # Calculate end of trading day (3:30 PM on the same day)
        entry_date = entry_time.date()
        market_close_time = pd.Timestamp(entry_date).tz_localize('Asia/Kolkata').replace(hour=15, minute=30)
        
        # Reject trades entered too close to market close (within 30 minutes)
        time_to_close = market_close_time - entry_time
        if time_to_close.total_seconds() < 1800:  # 30 minutes = 1800 seconds
            return None
        
        # Get subsequent price data after entry but within the same trading day
        stock_data = ohlc_data[
            (ohlc_data['stock'] == stock_symbol) & 
            (ohlc_data['timestamp'] > entry_time) &
            (ohlc_data['timestamp'] <= market_close_time)
        ].sort_values('timestamp')
        
        if len(stock_data) == 0:
            return None
            
        # Simulate price movement
        for _, row in stock_data.iterrows():
            high_price = row['high']
            low_price = row['low']
            current_time = row['timestamp']
            
            exit_price = None
            exit_reason = None
            
            if trade_type == 'long':
                if low_price <= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_reason = 'stop_loss'
                elif high_price >= profit_target_price:
                    exit_price = profit_target_price
                    exit_reason = 'profit_target'
            else:  # short
                if high_price >= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_reason = 'stop_loss'
                elif low_price <= profit_target_price:
                    exit_price = profit_target_price
                    exit_reason = 'profit_target'
            
            if exit_price is not None:
                if trade_type == 'long':
                    pnl_pct = (exit_price - entry_price) / entry_price
                else:  # short
                    pnl_pct = (entry_price - exit_price) / entry_price
                
                return {
                    'entry_time': entry_time,
                    'exit_time': current_time,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'trade_type': trade_type,
                    'exit_reason': exit_reason,
                    'pnl_pct': pnl_pct,
                    'duration': current_time - entry_time
                }
        
        # Exit at market close if no stop loss or profit target hit (intraday constraint)
        if len(stock_data) > 0:
            last_row = stock_data.iloc[-1]
            exit_price = last_row['close']
            exit_time = last_row['timestamp']
            
            # Check if we're at market close time
            if exit_time.time() >= pd.Timestamp('15:30').time():
                exit_reason = 'market_close'
            else:
                exit_reason = 'end_of_data'
            
            if trade_type == 'long':
                pnl_pct = (exit_price - entry_price) / entry_price
            else:  # short
                pnl_pct = (entry_price - exit_price) / entry_price
            
            return {
                'entry_time': entry_time,
                'exit_time': exit_time,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'trade_type': trade_type,
                'exit_reason': exit_reason,
                'pnl_pct': pnl_pct,
                'duration': exit_time - entry_time
            }
        
        # If no data available for the same day, consider trade invalid
        return None
    
    def run_backtest(self):
        """Run the complete backtest"""
        print("\n" + "="*60)
        print("STARTING TRAIL BACKTEST")
        print("="*60)
        
        if not self.load_data():
            print("Failed to load data. Exiting.")
            return False
        
        print(f"\nBacktest Parameters:")
        print(f"Initial Capital: ₹{self.initial_capital:,.0f}")
        print(f"Stop Loss: {self.stop_loss_pct*100:.1f}%")
        print(f"Profit Target: {self.profit_target_pct*100:.1f}%")
        print(f"Trading Style: INTRADAY (All trades closed by 3:30 PM)")
        
        # Process Calls predictions (Long trades)
        print(f"\nProcessing {len(self.calls_predictions)} CALLS predictions...")
        
        for idx, prediction in self.calls_predictions.iterrows():
            stock_symbol = prediction.get('Stock', None)
            
            try:
                date_str = str(prediction.get('Date', ''))
                time_str = str(prediction.get('Time', ''))
                if date_str and time_str and date_str != 'nan' and time_str != 'nan':
                    datetime_str = f"{date_str} {time_str}"
                    prediction_time = pd.to_datetime(datetime_str)
                    prediction_time = prediction_time.tz_localize('Asia/Kolkata')
                else:
                    prediction_time = None
            except:
                prediction_time = None
            
            if stock_symbol is None or prediction_time is None:
                continue
                
            matching_ohlc = self.find_matching_ohlc(stock_symbol, prediction_time, self.calls_ohlc)
            
            if matching_ohlc is not None:
                entry_price = matching_ohlc['close']
                
                trade_result = self.simulate_trade(
                    entry_price, 'long', stock_symbol, 
                    matching_ohlc['timestamp'], self.calls_ohlc
                )
                
                if trade_result is not None:
                    trade_result['stock'] = stock_symbol
                    trade_result['prediction_source'] = 'calls'
                    self.trades.append(trade_result)
        
        # Process Puts predictions (Short trades)
        print(f"Processing {len(self.puts_predictions)} PUTS predictions...")
        
        for idx, prediction in self.puts_predictions.iterrows():
            stock_symbol = prediction.get('Stock', None)
            
            try:
                date_str = str(prediction.get('Date', ''))
                time_str = str(prediction.get('Time', ''))
                if date_str and time_str and date_str != 'nan' and time_str != 'nan':
                    datetime_str = f"{date_str} {time_str}"
                    prediction_time = pd.to_datetime(datetime_str)
                    prediction_time = prediction_time.tz_localize('Asia/Kolkata')
                else:
                    prediction_time = None
            except:
                prediction_time = None
            
            if stock_symbol is None or prediction_time is None:
                continue
                
            matching_ohlc = self.find_matching_ohlc(stock_symbol, prediction_time, self.puts_ohlc)
            
            if matching_ohlc is not None:
                entry_price = matching_ohlc['close']
                
                trade_result = self.simulate_trade(
                    entry_price, 'short', stock_symbol, 
                    matching_ohlc['timestamp'], self.puts_ohlc
                )
                
                if trade_result is not None:
                    trade_result['stock'] = stock_symbol
                    trade_result['prediction_source'] = 'puts'
                    self.trades.append(trade_result)
        
        # Calculate results
        self.calculate_results()
        self.display_results()
        return True
    
    def calculate_results(self):
        """Calculate backtest summary statistics"""
        if len(self.trades) == 0:
            print("No trades executed!")
            return
        
        trades_df = pd.DataFrame(self.trades)
        
        self.trade_summary['total_trades'] = len(trades_df)
        self.trade_summary['winning_trades'] = len(trades_df[trades_df['pnl_pct'] > 0])
        self.trade_summary['losing_trades'] = len(trades_df[trades_df['pnl_pct'] < 0])
        self.trade_summary['win_rate'] = self.trade_summary['winning_trades'] / self.trade_summary['total_trades'] * 100
        
        trades_df['pnl_amount'] = trades_df['pnl_pct'] * self.initial_capital
        self.trade_summary['total_pnl'] = trades_df['pnl_amount'].sum()
        
        trades_df['cumulative_pnl'] = trades_df['pnl_amount'].cumsum()
        trades_df['running_capital'] = self.initial_capital + trades_df['cumulative_pnl']
        
        running_max = trades_df['running_capital'].expanding().max()
        drawdown = (trades_df['running_capital'] - running_max) / running_max * 100
        self.trade_summary['max_drawdown'] = drawdown.min()
        
        self.current_capital = self.initial_capital + self.trade_summary['total_pnl']
        self.processed_trades = trades_df
    
    def display_results(self):
        """Display comprehensive backtest results"""
        print("\n" + "="*60)
        print("BACKTEST RESULTS SUMMARY")
        print("="*60)
        
        print(f"\n📊 TRADING PERFORMANCE:")
        print(f"   Total Trades Executed: {self.trade_summary['total_trades']}")
        print(f"   Winning Trades: {self.trade_summary['winning_trades']}")
        print(f"   Losing Trades: {self.trade_summary['losing_trades']}")
        print(f"   Win Rate: {self.trade_summary['win_rate']:.1f}%")
        
        print(f"\n💰 FINANCIAL PERFORMANCE:")
        print(f"   Initial Capital: ₹{self.initial_capital:,.0f}")
        print(f"   Final Capital: ₹{self.current_capital:,.0f}")
        print(f"   Total P&L: ₹{self.trade_summary['total_pnl']:,.0f}")
        print(f"   Total Return: {(self.trade_summary['total_pnl']/self.initial_capital)*100:.2f}%")
        print(f"   Maximum Drawdown: {self.trade_summary['max_drawdown']:.2f}%")
    
    def export_to_csv(self, filename_prefix=None):
        """Export all backtest results to CSV files"""
        
        if filename_prefix is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_prefix = f"backtest_results_{timestamp}"
        
        print(f"\n📊 Exporting detailed results to CSV files with prefix: {filename_prefix}")
        
        if len(self.trades) == 0:
            print("No trades to export!")
            return
        
        try:
            # All Trades Detail
            trades_df = self.processed_trades.copy()
            
            # Convert timezone-aware timestamps to timezone-naive
            trades_df['entry_time'] = trades_df['entry_time'].dt.tz_localize(None)
            trades_df['exit_time'] = trades_df['exit_time'].dt.tz_localize(None)
            
            # Format for readability
            trades_export = trades_df.copy()
            trades_export['entry_price'] = trades_export['entry_price'].round(2)
            trades_export['exit_price'] = trades_export['exit_price'].round(2)
            trades_export['pnl_pct'] = (trades_export['pnl_pct'] * 100).round(2)
            trades_export['pnl_amount'] = trades_export['pnl_amount'].round(0)
            
            # Export All Trades
            trades_filename = f"{filename_prefix}_all_trades.csv"
            trades_export.to_csv(trades_filename, index=False)
            
            # Summary Statistics
            summary_data = {
                'Metric': [
                    'Initial Capital (₹)',
                    'Final Capital (₹)',
                    'Total P&L (₹)',
                    'Total Return (%)',
                    'Total Trades',
                    'Winning Trades',
                    'Losing Trades',
                    'Win Rate (%)',
                    'Maximum Drawdown (%)',
                    'Average Trade P&L (₹)',
                    'Best Trade P&L (₹)',
                    'Worst Trade P&L (₹)'
                ],
                'Value': [
                    f"{self.initial_capital:,.0f}",
                    f"{self.current_capital:,.0f}",
                    f"{self.trade_summary['total_pnl']:,.0f}",
                    f"{(self.trade_summary['total_pnl']/self.initial_capital)*100:.2f}",
                    self.trade_summary['total_trades'],
                    self.trade_summary['winning_trades'],
                    self.trade_summary['losing_trades'],
                    f"{self.trade_summary['win_rate']:.1f}",
                    f"{self.trade_summary['max_drawdown']:.2f}",
                    f"{trades_df['pnl_amount'].mean():,.0f}",
                    f"{trades_df['pnl_amount'].max():,.0f}",
                    f"{trades_df['pnl_amount'].min():,.0f}"
                ]
            }
            
            # Export Summary
            summary_filename = f"{filename_prefix}_summary.csv"
            pd.DataFrame(summary_data).to_csv(summary_filename, index=False)
            
            # Stock Performance
            stock_perf = trades_df.groupby('stock').agg({
                'pnl_amount': ['count', 'sum', 'mean'],
                'pnl_pct': lambda x: (x > 0).sum()
            }).round(2)
            
            stock_perf.columns = ['Total_Trades', 'Total_PnL', 'Avg_PnL', 'Wins']
            stock_perf['Win_Rate_Pct'] = (stock_perf['Wins'] / stock_perf['Total_Trades'] * 100).round(1)
            
            # Export Stock Performance
            stock_filename = f"{filename_prefix}_by_stock.csv"
            stock_perf.to_csv(stock_filename)
            
            # Export Original Predictions
            if hasattr(self, 'calls_predictions'):
                calls_filename = f"{filename_prefix}_original_calls.csv"
                self.calls_predictions.to_csv(calls_filename, index=False)
                
            if hasattr(self, 'puts_predictions'):
                puts_filename = f"{filename_prefix}_original_puts.csv"
                self.puts_predictions.to_csv(puts_filename, index=False)
            
            print(f"✅ CSV export completed successfully!")
            print(f"📂 Files created:")
            print(f"   • {trades_filename} - All trade details")
            print(f"   • {summary_filename} - Performance summary")
            print(f"   • {stock_filename} - Performance by stock")
            if hasattr(self, 'calls_predictions'):
                print(f"   • {calls_filename} - Original calls predictions")
            if hasattr(self, 'puts_predictions'):
                print(f"   • {puts_filename} - Original puts predictions")
            return True
            
        except Exception as e:
            print(f"❌ CSV export failed: {e}")
            return False

def main():
    """Main function to run the intraday backtest and export"""
    print("Intraday Trail Backtest Analysis with CSV Export")
    print("===============================================")
    
    # Initialize backtest
    backtest = TrailBacktest(
        capital=1000000,      # 10L capital
        stop_loss_pct=0.5,    # 0.5% stop loss
        profit_target_pct=1.0 # 1% profit target
    )
    
    # Run the backtest
    if backtest.run_backtest():
        # Export to CSV
        backtest.export_to_csv()
        print("\n🎉 Complete analysis finished successfully!")
    else:
        print("❌ Backtest failed to complete")

if __name__ == "__main__":
    main() 