#!/usr/bin/env python3
"""
Realistic Intraday Trail Backtest with Proper Position Sizing
Ensures position sizes never exceed available capital
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class RealisticTrailBacktest:
    def __init__(self, capital=1000000, stop_loss_pct=0.5, profit_target_pct=1.0):
        """
        Initialize realistic backtest with proper position sizing
        
        Position allocation strategy:
        - Grade A: 2% of total capital per trade (higher allocation)
        - Grade B: 1.5% of total capital per trade (medium allocation)
        - Grade C: 1% of total capital per trade (standard allocation)
        """
        self.initial_capital = capital
        self.current_capital = capital
        self.stop_loss_pct = stop_loss_pct / 100
        self.profit_target_pct = profit_target_pct / 100
        
        # Realistic position sizing as percentage of total capital
        self.grade_allocation = {
            'A': 0.02,   # 2% of total capital (₹20k per trade if ₹10L total)
            'B': 0.015,  # 1.5% of total capital (₹15k per trade if ₹10L total)
            'C': 0.01,   # 1% of total capital (₹10k per trade if ₹10L total)
            'D': 0.01    # 1% of total capital (₹10k per trade if ₹10L total)
        }
        
        self.trades = []
        self.trade_summary = {}
        
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
    
    def calculate_position_size(self, prediction, current_capital):
        """Calculate realistic position size based on prediction grade"""
        grade = prediction.get('Grade', 'C')
        allocation_pct = self.grade_allocation.get(grade, 0.01)
        
        # Position size = allocation percentage × current capital
        position_size = current_capital * allocation_pct
        
        return position_size
    
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
    
    def simulate_trade(self, entry_price, trade_type, stock_symbol, entry_time, ohlc_data, position_size):
        """Simulate an intraday trade with realistic position sizing"""
        
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
            
        # Simulate price movement tick by tick
        for _, row in stock_data.iterrows():
            high_price = row['high']
            low_price = row['low']
            current_time = row['timestamp']
            
            exit_price = None
            exit_reason = None
            
            if trade_type == 'long':
                # Check if stop loss hit (price went below SL)
                if low_price <= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_reason = 'stop_loss'
                # Check if profit target hit (price went above target)
                elif high_price >= profit_target_price:
                    exit_price = profit_target_price
                    exit_reason = 'profit_target'
            else:  # short
                # Check if stop loss hit (price went above SL)
                if high_price >= stop_loss_price:
                    exit_price = stop_loss_price
                    exit_reason = 'stop_loss'
                # Check if profit target hit (price went below target)
                elif low_price <= profit_target_price:
                    exit_price = profit_target_price
                    exit_reason = 'profit_target'
            
            if exit_price is not None:
                # Calculate PnL with realistic position sizing
                if trade_type == 'long':
                    pnl_pct = (exit_price - entry_price) / entry_price
                else:  # short
                    pnl_pct = (entry_price - exit_price) / entry_price
                
                pnl_amount = pnl_pct * position_size
                
                return {
                    'entry_time': entry_time,
                    'exit_time': current_time,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'trade_type': trade_type,
                    'exit_reason': exit_reason,
                    'pnl_pct': pnl_pct,
                    'pnl_amount': pnl_amount,
                    'position_size': position_size,
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
            
            pnl_amount = pnl_pct * position_size
            
            return {
                'entry_time': entry_time,
                'exit_time': exit_time,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'trade_type': trade_type,
                'exit_reason': exit_reason,
                'pnl_pct': pnl_pct,
                'pnl_amount': pnl_amount,
                'position_size': position_size,
                'duration': exit_time - entry_time
            }
        
        # If no data available for the same day, consider trade invalid
        return None
    
    def run_backtest(self):
        """Run the complete realistic backtest with proper capital management"""
        print("\n" + "="*60)
        print("REALISTIC INTRADAY TRAIL BACKTEST")
        print("="*60)
        
        if not self.load_data():
            print("Failed to load data. Exiting.")
            return False
        
        print(f"\nRealistic Backtest Parameters:")
        print(f"Initial Capital: ₹{self.initial_capital:,.0f}")
        print(f"Stop Loss: {self.stop_loss_pct*100:.1f}%")
        print(f"Profit Target: {self.profit_target_pct*100:.1f}%")
        print(f"Trading Style: INTRADAY (All trades closed by 3:30 PM)")
        print(f"Position Sizing: GRADE-BASED ALLOCATION (% of capital)")
        print(f"  • Grade A: {self.grade_allocation['A']*100:.1f}% per trade (₹{self.initial_capital*self.grade_allocation['A']:,.0f})")
        print(f"  • Grade B: {self.grade_allocation['B']*100:.1f}% per trade (₹{self.initial_capital*self.grade_allocation['B']:,.0f})")
        print(f"  • Grade C: {self.grade_allocation['C']*100:.1f}% per trade (₹{self.initial_capital*self.grade_allocation['C']:,.0f})")
        
        # Track running capital for realistic position sizing
        running_capital = self.initial_capital
        
        # Combine all predictions with timestamps for chronological processing
        all_predictions = []
        
        # Process Calls predictions
        for idx, prediction in self.calls_predictions.iterrows():
            stock_symbol = prediction.get('Stock', None)
            
            try:
                date_str = str(prediction.get('Date', ''))
                time_str = str(prediction.get('Time', ''))
                if date_str and time_str and date_str != 'nan' and time_str != 'nan':
                    datetime_str = f"{date_str} {time_str}"
                    prediction_time = pd.to_datetime(datetime_str)
                    prediction_time = prediction_time.tz_localize('Asia/Kolkata')
                    
                    all_predictions.append({
                        'prediction': prediction,
                        'stock_symbol': stock_symbol,
                        'prediction_time': prediction_time,
                        'trade_type': 'long',
                        'source': 'calls',
                        'ohlc_data': self.calls_ohlc
                    })
            except:
                continue
        
        # Process Puts predictions
        for idx, prediction in self.puts_predictions.iterrows():
            stock_symbol = prediction.get('Stock', None)
            
            try:
                date_str = str(prediction.get('Date', ''))
                time_str = str(prediction.get('Time', ''))
                if date_str and time_str and date_str != 'nan' and time_str != 'nan':
                    datetime_str = f"{date_str} {time_str}"
                    prediction_time = pd.to_datetime(datetime_str)
                    prediction_time = prediction_time.tz_localize('Asia/Kolkata')
                    
                    all_predictions.append({
                        'prediction': prediction,
                        'stock_symbol': stock_symbol,
                        'prediction_time': prediction_time,
                        'trade_type': 'short',
                        'source': 'puts',
                        'ohlc_data': self.puts_ohlc
                    })
            except:
                continue
        
        # Sort predictions chronologically
        all_predictions.sort(key=lambda x: x['prediction_time'])
        
        print(f"\nProcessing {len(all_predictions)} predictions chronologically...")
        
        # Process predictions in chronological order
        for pred_data in all_predictions:
            prediction = pred_data['prediction']
            stock_symbol = pred_data['stock_symbol']
            prediction_time = pred_data['prediction_time']
            trade_type = pred_data['trade_type']
            source = pred_data['source']
            ohlc_data = pred_data['ohlc_data']
            
            if stock_symbol is None:
                continue
                
            matching_ohlc = self.find_matching_ohlc(stock_symbol, prediction_time, ohlc_data)
            
            if matching_ohlc is not None:
                entry_price = matching_ohlc['close']
                
                # Calculate position size based on current capital (not initial capital)
                position_size = self.calculate_position_size(prediction, running_capital)
                
                # Check if we have enough capital for this trade
                if position_size > running_capital * 0.1:  # Don't risk more than 10% on single trade
                    position_size = running_capital * 0.1
                
                trade_result = self.simulate_trade(
                    entry_price, trade_type, stock_symbol, 
                    matching_ohlc['timestamp'], ohlc_data, position_size
                )
                
                if trade_result is not None:
                    trade_result['stock'] = stock_symbol
                    trade_result['prediction_source'] = source
                    trade_result['grade'] = prediction.get('Grade', 'Unknown')
                    
                    # Update running capital
                    running_capital += trade_result['pnl_amount']
                    trade_result['running_capital'] = running_capital
                    
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
        
        # Basic statistics
        self.trade_summary['total_trades'] = len(trades_df)
        self.trade_summary['winning_trades'] = len(trades_df[trades_df['pnl_pct'] > 0])
        self.trade_summary['losing_trades'] = len(trades_df[trades_df['pnl_pct'] < 0])
        self.trade_summary['win_rate'] = self.trade_summary['winning_trades'] / self.trade_summary['total_trades'] * 100
        
        # PnL calculations
        self.trade_summary['total_pnl'] = trades_df['pnl_amount'].sum()
        
        # Final capital from last trade
        self.current_capital = trades_df['running_capital'].iloc[-1] if len(trades_df) > 0 else self.initial_capital
        
        # Max drawdown calculation
        trades_df['cumulative_pnl'] = trades_df['pnl_amount'].cumsum()
        trades_df['portfolio_value'] = self.initial_capital + trades_df['cumulative_pnl']
        
        running_max = trades_df['portfolio_value'].expanding().max()
        drawdown = (trades_df['portfolio_value'] - running_max) / running_max * 100
        self.trade_summary['max_drawdown'] = drawdown.min()
        
        # Store processed trades
        self.processed_trades = trades_df
    
    def display_results(self):
        """Display comprehensive backtest results"""
        print("\n" + "="*60)
        print("REALISTIC BACKTEST RESULTS SUMMARY")
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
        
        total_return_pct = (self.trade_summary['total_pnl']/self.initial_capital)*100
        print(f"   Total Return: {total_return_pct:.2f}%")
        print(f"   Maximum Drawdown: {self.trade_summary['max_drawdown']:.2f}%")
        
        # Target achievement check
        if total_return_pct >= 40:
            print(f"   🎯 TARGET ACHIEVED! ({total_return_pct:.2f}% ≥ 40%)")
        else:
            print(f"   📊 Current Performance: {total_return_pct:.2f}% (Target: 40%)")
        
        if len(self.trades) > 0:
            trades_df = self.processed_trades
            
            # Grade-wise performance analysis
            print(f"\n📈 REALISTIC GRADE-WISE PERFORMANCE:")
            grade_analysis = trades_df.groupby('grade').agg({
                'pnl_amount': ['count', 'sum', 'mean'],
                'pnl_pct': lambda x: (x > 0).sum(),
                'position_size': 'mean'
            }).round(2)
            
            for grade in ['A', 'B', 'C', 'D']:
                if grade in grade_analysis.index:
                    count = grade_analysis.loc[grade, ('pnl_amount', 'count')]
                    total_pnl = grade_analysis.loc[grade, ('pnl_amount', 'sum')]
                    wins = grade_analysis.loc[grade, ('pnl_pct', '<lambda>')]
                    avg_position = grade_analysis.loc[grade, ('position_size', 'mean')]
                    win_rate = (wins / count) * 100 if count > 0 else 0
                    allocation_pct = self.grade_allocation.get(grade, 0.01) * 100
                    
                    print(f"   Grade {grade}: {count} trades, ₹{total_pnl:,.0f} P&L, {win_rate:.1f}% win rate")
                    print(f"            Avg position: ₹{avg_position:,.0f} ({allocation_pct:.1f}% of capital)")
        
        print(f"\n💡 POSITION SIZING EXPLANATION:")
        print(f"   Unlike the previous unrealistic results, this backtest uses")
        print(f"   proper position sizing where each trade uses only a small")
        print(f"   percentage of available capital, ensuring we never exceed")
        print(f"   our actual capital limits.")

def main():
    """Main function to run the realistic backtest"""
    print("Realistic Intraday Trail Backtest with Proper Position Sizing")
    print("============================================================")
    print("🎯 Goal: Achieve realistic returns with proper capital management")
    
    # Initialize realistic backtest
    backtest = RealisticTrailBacktest(
        capital=1000000,      # 10L capital
        stop_loss_pct=0.5,    # 0.5% stop loss
        profit_target_pct=1.0 # 1% profit target
    )
    
    # Run the backtest
    if backtest.run_backtest():
        print("\n🎉 Realistic analysis completed!")
        print("This shows what returns are actually achievable with ₹10L capital.")
    else:
        print("❌ Backtest failed to complete")

if __name__ == "__main__":
    main() 