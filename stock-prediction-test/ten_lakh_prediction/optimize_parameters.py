#!/usr/bin/env python3
"""
Parameter Optimization for Intraday Backtest
Test different combinations to achieve 40% target returns
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import itertools
import warnings
warnings.filterwarnings('ignore')

# Import the main backtest class
import sys
sys.path.append('.')

class OptimizedBacktest:
    def __init__(self, capital=1000000):
        self.initial_capital = capital
        self.optimization_results = []
        
    def load_data(self):
        """Load predictions and OHLC data"""
        try:
            # Load Excel predictions
            self.calls_predictions = pd.read_excel('option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Calls')
            self.puts_predictions = pd.read_excel('option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Puts')
            
            # Load OHLC data
            self.calls_ohlc = pd.read_csv('calls_ohlc_data_20250716_161308.csv')
            self.puts_ohlc = pd.read_csv('puts_ohlc_data_20250716_161308.csv')
            
            # Convert timestamp to datetime
            self.calls_ohlc['timestamp'] = pd.to_datetime(self.calls_ohlc['timestamp'])
            self.puts_ohlc['timestamp'] = pd.to_datetime(self.puts_ohlc['timestamp'])
            
            print(f"✅ Data loaded successfully")
            print(f"   Calls predictions: {len(self.calls_predictions)}")
            print(f"   Puts predictions: {len(self.puts_predictions)}")
            return True
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False
    
    def filter_predictions(self, predictions, grade_filter=None, tn_ratio_min=None, time_filter=None):
        """Filter predictions based on quality criteria"""
        filtered = predictions.copy()
        
        if grade_filter:
            filtered = filtered[filtered['Grade'].isin(grade_filter)]
            
        if tn_ratio_min:
            filtered = filtered[filtered['TN_Ratio'] >= tn_ratio_min]
            
        if time_filter:
            # Convert time to check against filter
            filtered['Time'] = pd.to_datetime(filtered['Time'], format='%H:%M:%S').dt.time
            start_time, end_time = time_filter
            filtered = filtered[
                (filtered['Time'] >= start_time) & 
                (filtered['Time'] <= end_time)
            ]
        
        return filtered
    
    def calculate_position_size(self, prediction, base_capital, sizing_method='fixed'):
        """Calculate position size based on different methods"""
        if sizing_method == 'fixed':
            return base_capital
        elif sizing_method == 'grade_based':
            grade = prediction.get('Grade', 'C')
            multipliers = {'A': 2.0, 'B': 1.5, 'C': 1.0}
            return base_capital * multipliers.get(grade, 1.0)
        elif sizing_method == 'tn_ratio_based':
            tn_ratio = prediction.get('TN_Ratio', 50)
            if tn_ratio >= 70:
                return base_capital * 1.8
            elif tn_ratio >= 60:
                return base_capital * 1.4
            else:
                return base_capital
        
        return base_capital
    
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
    
    def simulate_trade(self, entry_price, trade_type, stock_symbol, entry_time, ohlc_data, 
                      stop_loss_pct, profit_target_pct, position_size):
        """Simulate an intraday trade with custom parameters"""
        
        if trade_type == 'long':
            stop_loss_price = entry_price * (1 - stop_loss_pct)
            profit_target_price = entry_price * (1 + profit_target_pct)
        else:  # short
            stop_loss_price = entry_price * (1 + stop_loss_pct)
            profit_target_price = entry_price * (1 - profit_target_pct)
        
        # Calculate end of trading day
        entry_date = entry_time.date()
        market_close_time = pd.Timestamp(entry_date).tz_localize('Asia/Kolkata').replace(hour=15, minute=30)
        
        # Reject trades too close to market close
        time_to_close = market_close_time - entry_time
        if time_to_close.total_seconds() < 1800:  # 30 minutes
            return None
        
        # Get subsequent price data
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
        
        # Exit at market close
        if len(stock_data) > 0:
            last_row = stock_data.iloc[-1]
            exit_price = last_row['close']
            
            if trade_type == 'long':
                pnl_pct = (exit_price - entry_price) / entry_price
            else:  # short
                pnl_pct = (entry_price - exit_price) / entry_price
            
            pnl_amount = pnl_pct * position_size
            
            return {
                'entry_time': entry_time,
                'exit_time': last_row['timestamp'],
                'entry_price': entry_price,
                'exit_price': exit_price,
                'trade_type': trade_type,
                'exit_reason': 'market_close',
                'pnl_pct': pnl_pct,
                'pnl_amount': pnl_amount,
                'position_size': position_size,
                'duration': last_row['timestamp'] - entry_time
            }
        
        return None
    
    def run_scenario(self, scenario_params):
        """Run backtest with specific scenario parameters"""
        
        stop_loss_pct = scenario_params['stop_loss_pct'] / 100
        profit_target_pct = scenario_params['profit_target_pct'] / 100
        grade_filter = scenario_params.get('grade_filter')
        tn_ratio_min = scenario_params.get('tn_ratio_min')
        time_filter = scenario_params.get('time_filter')
        sizing_method = scenario_params.get('sizing_method', 'fixed')
        
        trades = []
        
        # Filter predictions
        filtered_calls = self.filter_predictions(
            self.calls_predictions, grade_filter, tn_ratio_min, time_filter
        )
        filtered_puts = self.filter_predictions(
            self.puts_predictions, grade_filter, tn_ratio_min, time_filter
        )
        
        # Process calls (long trades)
        for idx, prediction in filtered_calls.iterrows():
            stock_symbol = prediction.get('Stock', None)
            
            try:
                date_str = str(prediction.get('Date', ''))
                time_str = str(prediction.get('Time', ''))
                if date_str and time_str and date_str != 'nan' and time_str != 'nan':
                    datetime_str = f"{date_str} {time_str}"
                    prediction_time = pd.to_datetime(datetime_str)
                    prediction_time = prediction_time.tz_localize('Asia/Kolkata')
                else:
                    continue
            except:
                continue
            
            if stock_symbol is None:
                continue
                
            matching_ohlc = self.find_matching_ohlc(stock_symbol, prediction_time, self.calls_ohlc)
            
            if matching_ohlc is not None:
                entry_price = matching_ohlc['close']
                position_size = self.calculate_position_size(
                    prediction, self.initial_capital, sizing_method
                )
                
                trade_result = self.simulate_trade(
                    entry_price, 'long', stock_symbol, 
                    matching_ohlc['timestamp'], self.calls_ohlc,
                    stop_loss_pct, profit_target_pct, position_size
                )
                
                if trade_result is not None:
                    trade_result['stock'] = stock_symbol
                    trade_result['prediction_source'] = 'calls'
                    trade_result['grade'] = prediction.get('Grade', 'Unknown')
                    trades.append(trade_result)
        
        # Process puts (short trades)
        for idx, prediction in filtered_puts.iterrows():
            stock_symbol = prediction.get('Stock', None)
            
            try:
                date_str = str(prediction.get('Date', ''))
                time_str = str(prediction.get('Time', ''))
                if date_str and time_str and date_str != 'nan' and time_str != 'nan':
                    datetime_str = f"{date_str} {time_str}"
                    prediction_time = pd.to_datetime(datetime_str)
                    prediction_time = prediction_time.tz_localize('Asia/Kolkata')
                else:
                    continue
            except:
                continue
            
            if stock_symbol is None:
                continue
                
            matching_ohlc = self.find_matching_ohlc(stock_symbol, prediction_time, self.puts_ohlc)
            
            if matching_ohlc is not None:
                entry_price = matching_ohlc['close']
                position_size = self.calculate_position_size(
                    prediction, self.initial_capital, sizing_method
                )
                
                trade_result = self.simulate_trade(
                    entry_price, 'short', stock_symbol, 
                    matching_ohlc['timestamp'], self.puts_ohlc,
                    stop_loss_pct, profit_target_pct, position_size
                )
                
                if trade_result is not None:
                    trade_result['stock'] = stock_symbol
                    trade_result['prediction_source'] = 'puts'
                    trade_result['grade'] = prediction.get('Grade', 'Unknown')
                    trades.append(trade_result)
        
        # Calculate results
        if len(trades) == 0:
            return None
            
        trades_df = pd.DataFrame(trades)
        
        total_pnl = trades_df['pnl_amount'].sum()
        total_return_pct = (total_pnl / self.initial_capital) * 100
        win_rate = (len(trades_df[trades_df['pnl_pct'] > 0]) / len(trades_df)) * 100
        
        # Calculate max drawdown
        trades_df['cumulative_pnl'] = trades_df['pnl_amount'].cumsum()
        trades_df['running_capital'] = self.initial_capital + trades_df['cumulative_pnl']
        running_max = trades_df['running_capital'].expanding().max()
        drawdown = (trades_df['running_capital'] - running_max) / running_max * 100
        max_drawdown = drawdown.min()
        
        return {
            'scenario_params': scenario_params,
            'total_trades': len(trades_df),
            'total_return_pct': total_return_pct,
            'total_pnl': total_pnl,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown,
            'avg_trade_pnl': trades_df['pnl_amount'].mean(),
            'best_trade': trades_df['pnl_amount'].max(),
            'worst_trade': trades_df['pnl_amount'].min(),
            'trades_data': trades_df
        }
    
    def optimize_parameters(self):
        """Test multiple parameter combinations to find optimal settings"""
        
        print("🔍 Starting Parameter Optimization...")
        print("Target: 40%+ returns")
        print("="*50)
        
        # Define parameter ranges to test
        scenarios = [
            # Baseline (current)
            {
                'name': 'Baseline',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.0,
                'sizing_method': 'fixed'
            },
            
            # Risk-Reward Optimizations
            {
                'name': 'Higher Target 1.5%',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.5,
                'sizing_method': 'fixed'
            },
            {
                'name': 'Tighter SL + Higher Target',
                'stop_loss_pct': 0.3,
                'profit_target_pct': 1.2,
                'sizing_method': 'fixed'
            },
            {
                'name': 'Aggressive 1:4 Ratio',
                'stop_loss_pct': 0.4,
                'profit_target_pct': 1.6,
                'sizing_method': 'fixed'
            },
            
            # Quality Filters
            {
                'name': 'Grade A Only',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.0,
                'grade_filter': ['A'],
                'sizing_method': 'fixed'
            },
            {
                'name': 'Grade A+B Only',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.0,
                'grade_filter': ['A', 'B'],
                'sizing_method': 'fixed'
            },
            {
                'name': 'High TN Ratio (70+)',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.0,
                'tn_ratio_min': 70,
                'sizing_method': 'fixed'
            },
            
            # Position Sizing
            {
                'name': 'Grade-Based Sizing',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.0,
                'sizing_method': 'grade_based'
            },
            {
                'name': 'TN Ratio-Based Sizing',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.0,
                'sizing_method': 'tn_ratio_based'
            },
            
            # Combined Optimizations
            {
                'name': 'Grade A + Higher Target',
                'stop_loss_pct': 0.4,
                'profit_target_pct': 1.4,
                'grade_filter': ['A'],
                'sizing_method': 'fixed'
            },
            {
                'name': 'Grade A + Dynamic Sizing',
                'stop_loss_pct': 0.5,
                'profit_target_pct': 1.2,
                'grade_filter': ['A'],
                'sizing_method': 'grade_based'
            },
            {
                'name': 'Best Combo Candidate',
                'stop_loss_pct': 0.3,
                'profit_target_pct': 1.5,
                'grade_filter': ['A', 'B'],
                'sizing_method': 'grade_based'
            }
        ]
        
        results = []
        
        for scenario in scenarios:
            print(f"\n🧪 Testing: {scenario['name']}...")
            
            result = self.run_scenario(scenario)
            if result is not None:
                results.append(result)
                
                print(f"   Trades: {result['total_trades']}")
                print(f"   Return: {result['total_return_pct']:.2f}%")
                print(f"   Win Rate: {result['win_rate']:.1f}%")
                print(f"   Max DD: {result['max_drawdown']:.2f}%")
                
                # Highlight if target achieved
                if result['total_return_pct'] >= 40:
                    print(f"   🎯 TARGET ACHIEVED! 🎯")
            else:
                print(f"   ❌ No valid trades")
        
        # Sort by return
        results.sort(key=lambda x: x['total_return_pct'], reverse=True)
        
        return results
    
    def export_optimization_results(self, results, filename=None):
        """Export optimization results to CSV"""
        
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"parameter_optimization_{timestamp}.csv"
        
        # Create summary dataframe
        summary_data = []
        for result in results:
            params = result['scenario_params']
            
            row = {
                'Scenario_Name': params.get('name', 'Unknown'),
                'Stop_Loss_%': params.get('stop_loss_pct', 0),
                'Profit_Target_%': params.get('profit_target_pct', 0),
                'Grade_Filter': str(params.get('grade_filter', 'All')),
                'TN_Ratio_Min': params.get('tn_ratio_min', 'None'),
                'Sizing_Method': params.get('sizing_method', 'fixed'),
                'Total_Trades': result['total_trades'],
                'Total_Return_%': round(result['total_return_pct'], 2),
                'Total_PnL_Rs': round(result['total_pnl'], 0),
                'Win_Rate_%': round(result['win_rate'], 1),
                'Max_Drawdown_%': round(result['max_drawdown'], 2),
                'Avg_Trade_PnL': round(result['avg_trade_pnl'], 0),
                'Best_Trade': round(result['best_trade'], 0),
                'Worst_Trade': round(result['worst_trade'], 0),
                'Target_40%_Achieved': 'YES' if result['total_return_pct'] >= 40 else 'NO'
            }
            summary_data.append(row)
        
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(filename, index=False)
        
        print(f"\n📊 Optimization results exported to: {filename}")
        return filename

def main():
    """Main optimization function"""
    print("Parameter Optimization for 40% Target Returns")
    print("============================================")
    
    optimizer = OptimizedBacktest(capital=1000000)
    
    if not optimizer.load_data():
        return
    
    # Run optimization
    results = optimizer.optimize_parameters()
    
    # Export results
    if results:
        optimizer.export_optimization_results(results)
        
        print("\n" + "="*60)
        print("🏆 TOP 5 OPTIMIZATION RESULTS")
        print("="*60)
        
        for i, result in enumerate(results[:5], 1):
            params = result['scenario_params']
            print(f"\n#{i}. {params.get('name', 'Unknown')}")
            print(f"    Return: {result['total_return_pct']:.2f}% | Trades: {result['total_trades']}")
            print(f"    Win Rate: {result['win_rate']:.1f}% | Max DD: {result['max_drawdown']:.2f}%")
            print(f"    SL: {params.get('stop_loss_pct', 0)}% | Target: {params.get('profit_target_pct', 0)}%")
            
            if result['total_return_pct'] >= 40:
                print(f"    🎯 ACHIEVES 40% TARGET!")
    
    else:
        print("❌ No results generated")

if __name__ == "__main__":
    main() 