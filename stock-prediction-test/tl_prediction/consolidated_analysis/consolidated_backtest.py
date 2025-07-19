#!/usr/bin/env python3
"""
Consolidated Dynamic Capital Backtest for Both Calls and Puts
- Processes both calls (long) and puts (short) trades chronologically
- Dynamic capital management across all trades
- Comprehensive analysis of complete trading strategy
"""

import pandas as pd
import numpy as np
from datetime import datetime

def load_all_data():
    """Load both calls and puts OHLC data and predictions"""
    
    # Load OHLC data
    calls_data = pd.read_csv('../calls_ohlc_data_20250716_161308.csv')
    calls_data['timestamp'] = pd.to_datetime(calls_data['timestamp'])
    calls_data['trade_type'] = 'LONG'  # Calls are long positions
    
    puts_data = pd.read_csv('../puts_ohlc_data_20250716_161308.csv')
    puts_data['timestamp'] = pd.to_datetime(puts_data['timestamp'])
    puts_data['trade_type'] = 'SHORT'  # Puts are short positions
    
    # Load predictions from Excel
    calls_predictions = pd.read_excel('../option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Calls')
    puts_predictions = pd.read_excel('../option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Puts')
    
    # Add trade type to predictions
    calls_predictions['trade_type'] = 'LONG'
    puts_predictions['trade_type'] = 'SHORT'
    
    # Create entry timestamps with timezone
    for df in [calls_predictions, puts_predictions]:
        df['entry_timestamp'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        df['entry_timestamp'] = df['entry_timestamp'].dt.tz_localize('Asia/Kolkata')
    
    # Combine predictions and sort chronologically
    all_predictions = pd.concat([calls_predictions, puts_predictions], ignore_index=True)
    all_predictions = all_predictions.sort_values(['Date', 'Time']).reset_index(drop=True)
    
    return calls_data, puts_data, all_predictions

def calculate_position_size(current_capital, stock_price, grade):
    """Calculate position size based on grade and current available capital"""
    
    grade_allocation = {
        'A': 0.4,  # 40% for A grade
        'B': 0.3,  # 30% for B grade  
        'C': 0.2,  # 20% for C grade
        'D': 0.1,  # 10% for D grade
    }
    
    allocated_capital = current_capital * grade_allocation.get(grade, 0.05)
    shares = int(allocated_capital / stock_price)
    actual_capital_used = shares * stock_price
    
    return shares, actual_capital_used

def execute_trade(stock_data, entry_time, stock, grade, current_capital, trade_type, stop_loss_pct=0.5, profit_target_pct=1.0):
    """Execute either a LONG (calls) or SHORT (puts) trade"""
    
    # Find the entry price at or after the prediction entry time
    entry_data = stock_data[stock_data['timestamp'] >= entry_time]
    
    if entry_data.empty:
        entry_row = stock_data.iloc[-1]
        entry_price = entry_row['close']
        actual_entry_time = entry_row['timestamp']
    else:
        entry_row = entry_data.iloc[0]
        entry_price = entry_row['open']
        actual_entry_time = entry_row['timestamp']
    
    # Calculate position
    shares, capital_used = calculate_position_size(current_capital, entry_price, grade)
    
    if shares == 0:
        return None
    
    # Calculate stop loss and profit target based on trade type
    if trade_type == 'LONG':  # Calls - profit when price goes UP
        stop_loss_price = entry_price * (1 - stop_loss_pct / 100)
        profit_target_price = entry_price * (1 + profit_target_pct / 100)
    else:  # SHORT - puts - profit when price goes DOWN
        stop_loss_price = entry_price * (1 + stop_loss_pct / 100)
        profit_target_price = entry_price * (1 - profit_target_pct / 100)
    
    # Track the position through the day
    exit_time = None
    exit_price = None
    exit_reason = "Market Close"
    
    remaining_data = stock_data[stock_data['timestamp'] >= actual_entry_time]
    
    for _, row in remaining_data.iterrows():
        current_time = row['timestamp']
        low_price = row['low']
        high_price = row['high']
        
        if trade_type == 'LONG':
            # LONG: Stop loss when price goes DOWN, profit when price goes UP
            if low_price <= stop_loss_price:
                exit_time = current_time
                exit_price = stop_loss_price
                exit_reason = "Stop Loss"
                break
            if high_price >= profit_target_price:
                exit_time = current_time
                exit_price = profit_target_price
                exit_reason = "Take Profit"
                break
        else:  # SHORT
            # SHORT: Stop loss when price goes UP, profit when price goes DOWN
            if high_price >= stop_loss_price:
                exit_time = current_time
                exit_price = stop_loss_price
                exit_reason = "Stop Loss"
                break
            if low_price <= profit_target_price:
                exit_time = current_time
                exit_price = profit_target_price
                exit_reason = "Take Profit"
                break
    
    # If no exit triggered, exit at market close
    if exit_time is None:
        exit_time = remaining_data.iloc[-1]['timestamp']
        exit_price = remaining_data.iloc[-1]['close']
    
    # Calculate P&L based on trade type
    if trade_type == 'LONG':
        pnl_per_share = exit_price - entry_price  # Long: profit when exit > entry
    else:  # SHORT
        pnl_per_share = entry_price - exit_price  # Short: profit when exit < entry
    
    total_pnl = pnl_per_share * shares
    pnl_pct = (pnl_per_share / entry_price) * 100
    
    # New capital after this trade
    new_capital = current_capital + total_pnl
    
    trade_result = {
        'stock': stock,
        'grade': grade,
        'trade_type': trade_type,
        'predicted_entry_time': entry_time,
        'actual_entry_time': actual_entry_time,
        'entry_price': entry_price,
        'exit_time': exit_time,
        'exit_price': exit_price,
        'exit_reason': exit_reason,
        'shares': shares,
        'capital_before': current_capital,
        'capital_used': capital_used,
        'pnl_amount': total_pnl,
        'pnl_pct': pnl_pct,
        'capital_after': new_capital,
        'stop_loss_price': stop_loss_price,
        'profit_target_price': profit_target_price
    }
    
    return trade_result

def run_consolidated_backtest(calls_data, puts_data, all_predictions, initial_capital=1000000):
    """Run consolidated backtest with both calls and puts"""
    
    trades = []
    current_capital = initial_capital
    
    print(f"Starting CONSOLIDATED backtest with ₹{initial_capital:,.0f}")
    print(f"Processing {len(all_predictions)} total predictions (calls + puts) chronologically...")
    
    for idx, pred_row in all_predictions.iterrows():
        stock = pred_row['Stock']
        grade = pred_row['Grade']
        entry_time = pred_row['entry_timestamp']
        date = pred_row['Date']
        trade_type = pred_row['trade_type']
        
        # Select appropriate OHLC data based on trade type
        if trade_type == 'LONG':
            stock_data = calls_data[
                (calls_data['stock'] == stock) & 
                (calls_data['date'] == date)
            ].sort_values('timestamp')
        else:  # SHORT
            stock_data = puts_data[
                (puts_data['stock'] == stock) & 
                (puts_data['date'] == date)
            ].sort_values('timestamp')
        
        if stock_data.empty:
            continue
        
        # Execute the trade
        trade_result = execute_trade(stock_data, entry_time, stock, grade, current_capital, trade_type)
        
        if trade_result is None:
            continue
        
        # Update capital for next trade
        current_capital = trade_result['capital_after']
        
        # Add additional prediction data
        trade_result.update({
            'date': date,
            'bullish_count': pred_row['Bullish_Count'],
            'bearish_count': pred_row['Bearish_Count'],
            'tn_ratio': pred_row['TN_Ratio'],
            'trade_number': len(trades) + 1
        })
        
        trades.append(trade_result)
        
        # Print progress every 100 trades
        if len(trades) % 100 == 0:
            print(f"Completed {len(trades)} trades ({trade_type}), Capital: ₹{current_capital:,.0f}")
    
    print(f"\nConsolidated backtest completed!")
    print(f"Final Capital: ₹{current_capital:,.0f}")
    print(f"Total Return: {((current_capital - initial_capital) / initial_capital * 100):,.2f}%")
    
    return pd.DataFrame(trades)

def analyze_consolidated_performance(trades_df):
    """Comprehensive analysis of both calls and puts performance"""
    
    if trades_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Overall performance
    initial_capital = trades_df.iloc[0]['capital_before']
    final_capital = trades_df.iloc[-1]['capital_after']
    total_return = ((final_capital - initial_capital) / initial_capital) * 100
    
    # Performance by trade type
    trade_type_analysis = []
    for trade_type in ['LONG', 'SHORT']:
        type_trades = trades_df[trades_df['trade_type'] == trade_type]
        if not type_trades.empty:
            total_pnl = type_trades['pnl_amount'].sum()
            avg_pnl = type_trades['pnl_amount'].mean()
            win_rate = (len(type_trades[type_trades['pnl_amount'] > 0]) / len(type_trades)) * 100
            
            trade_type_analysis.append({
                'trade_type': trade_type,
                'total_trades': len(type_trades),
                'win_rate_pct': win_rate,
                'total_pnl': total_pnl,
                'avg_pnl_per_trade': avg_pnl
            })
    
    # Performance by grade (across both types)
    grade_analysis = []
    for grade in trades_df['grade'].unique():
        grade_trades = trades_df[trades_df['grade'] == grade]
        
        total_trades = len(grade_trades)
        winning_trades = len(grade_trades[grade_trades['pnl_amount'] > 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        total_pnl = grade_trades['pnl_amount'].sum()
        avg_pnl = grade_trades['pnl_amount'].mean()
        
        # Count by trade type within grade
        long_trades = len(grade_trades[grade_trades['trade_type'] == 'LONG'])
        short_trades = len(grade_trades[grade_trades['trade_type'] == 'SHORT'])
        
        analysis = {
            'grade': grade,
            'total_trades': total_trades,
            'long_trades': long_trades,
            'short_trades': short_trades,
            'winning_trades': winning_trades,
            'win_rate_pct': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl_per_trade': avg_pnl
        }
        
        grade_analysis.append(analysis)
    
    grade_df = pd.DataFrame(grade_analysis)
    trade_type_df = pd.DataFrame(trade_type_analysis)
    
    # Print comprehensive summary
    print("\n" + "="*70)
    print("CONSOLIDATED BACKTEST RESULTS (CALLS + PUTS)")
    print("="*70)
    print(f"Initial Capital: ₹{initial_capital:,.0f}")
    print(f"Final Capital: ₹{final_capital:,.0f}")
    print(f"Total Return: {total_return:.2f}%")
    print(f"Total Trades: {len(trades_df)}")
    
    print(f"\nPERFORMANCE BY TRADE TYPE:")
    for _, row in trade_type_df.iterrows():
        strategy = "CALLS" if row['trade_type'] == 'LONG' else "PUTS"
        print(f"  {strategy}: {row['total_trades']} trades, {row['win_rate_pct']:.1f}% win rate, ₹{row['total_pnl']:,.0f} P&L")
    
    print(f"\nPERFORMANCE BY GRADE:")
    for _, row in grade_df.iterrows():
        print(f"  Grade {row['grade']}: {row['total_trades']} trades ({row['long_trades']} calls + {row['short_trades']} puts), {row['win_rate_pct']:.1f}% win rate, ₹{row['total_pnl']:,.0f} P&L")
    
    return grade_df, trade_type_df

def main():
    """Main execution function"""
    
    print("Loading all data...")
    calls_data, puts_data, all_predictions = load_all_data()
    
    print(f"Loaded {len(calls_data)} calls OHLC records")
    print(f"Loaded {len(puts_data)} puts OHLC records")
    print(f"Loaded {len(all_predictions)} total predictions")
    
    calls_count = len(all_predictions[all_predictions['trade_type'] == 'LONG'])
    puts_count = len(all_predictions[all_predictions['trade_type'] == 'SHORT'])
    print(f"  - {calls_count} calls predictions")
    print(f"  - {puts_count} puts predictions")
    
    # Run consolidated backtest
    trades_df = run_consolidated_backtest(calls_data, puts_data, all_predictions)
    
    # Analyze performance
    grade_performance, trade_type_performance = analyze_consolidated_performance(trades_df)
    
    # Save results
    trades_df.to_csv('consolidated_trades.csv', index=False)
    grade_performance.to_csv('consolidated_grade_performance.csv', index=False)
    trade_type_performance.to_csv('consolidated_trade_type_performance.csv', index=False)
    
    print(f"\nResults saved:")
    print(f"  All Trades: consolidated_trades.csv")
    print(f"  Grade Analysis: consolidated_grade_performance.csv")
    print(f"  Trade Type Analysis: consolidated_trade_type_performance.csv")
    
    return trades_df, grade_performance, trade_type_performance

if __name__ == "__main__":
    main() 