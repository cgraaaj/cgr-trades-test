#!/usr/bin/env python3
"""
Dynamic Capital Backtest for Calls Options
- Capital grows with profits and shrinks with losses
- Each trade uses updated capital from previous results
- More realistic simulation of actual trading
"""

import pandas as pd
import numpy as np
from datetime import datetime

def load_data():
    """Load the calls OHLC data and predictions Excel file"""
    
    # Load OHLC data
    calls_data = pd.read_csv('../calls_ohlc_data_20250716_161308.csv')
    calls_data['timestamp'] = pd.to_datetime(calls_data['timestamp'])
    
    # Load predictions from Excel
    predictions = pd.read_excel('../option_predictions_calls_puts_20250716_161302.xlsx', sheet_name='Calls')
    
    # Create entry timestamp from Date and Time columns with timezone
    predictions['entry_timestamp'] = pd.to_datetime(predictions['Date'].astype(str) + ' ' + predictions['Time'].astype(str))
    predictions['entry_timestamp'] = predictions['entry_timestamp'].dt.tz_localize('Asia/Kolkata')
    
    return calls_data, predictions

def calculate_position_size(current_capital, stock_price, grade):
    """Calculate position size based on grade and current available capital"""
    
    # Grade-based allocation percentages
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

def execute_trade(stock_data, entry_time, stock, grade, current_capital, stop_loss_pct=0.5, profit_target_pct=1.0):
    """Execute a single trade and return the result"""
    
    # Find the entry price at or after the prediction entry time
    entry_data = stock_data[stock_data['timestamp'] >= entry_time]
    
    if entry_data.empty:
        # If no data after entry time, use the last available price
        entry_row = stock_data.iloc[-1]
        entry_price = entry_row['close']
        actual_entry_time = entry_row['timestamp']
    else:
        # Use the first available price at or after the predicted entry time
        entry_row = entry_data.iloc[0]
        entry_price = entry_row['open']
        actual_entry_time = entry_row['timestamp']
    
    # Calculate position
    shares, capital_used = calculate_position_size(current_capital, entry_price, grade)
    
    if shares == 0:
        return None
        
    # Calculate stop loss and profit target prices
    stop_loss_price = entry_price * (1 - stop_loss_pct / 100)
    profit_target_price = entry_price * (1 + profit_target_pct / 100)
    
    # Track the position through the day (from entry time onwards)
    exit_time = None
    exit_price = None
    exit_reason = "Market Close"
    
    # Only check data from actual entry time onwards
    remaining_data = stock_data[stock_data['timestamp'] >= actual_entry_time]
    
    for _, row in remaining_data.iterrows():
        current_time = row['timestamp']
        low_price = row['low']
        high_price = row['high']
        
        # Check for stop loss hit
        if low_price <= stop_loss_price:
            exit_time = current_time
            exit_price = stop_loss_price
            exit_reason = "Stop Loss"
            break
            
        # Check for profit target hit
        if high_price >= profit_target_price:
            exit_time = current_time
            exit_price = profit_target_price
            exit_reason = "Take Profit"
            break
    
    # If no exit triggered, exit at market close
    if exit_time is None:
        exit_time = remaining_data.iloc[-1]['timestamp']
        exit_price = remaining_data.iloc[-1]['close']
        
    # Calculate P&L
    pnl_per_share = exit_price - entry_price
    total_pnl = pnl_per_share * shares
    pnl_pct = (pnl_per_share / entry_price) * 100
    
    # New capital after this trade
    new_capital = current_capital + total_pnl
    
    trade_result = {
        'stock': stock,
        'grade': grade,
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

def run_dynamic_backtest(calls_data, predictions, initial_capital=1000000):
    """Run backtest with dynamic capital management"""
    
    trades = []
    current_capital = initial_capital
    
    # Sort predictions by date and time for chronological processing
    predictions_sorted = predictions.sort_values(['Date', 'Time']).reset_index(drop=True)
    
    print(f"Starting backtest with ₹{initial_capital:,.0f}")
    print(f"Processing {len(predictions_sorted)} predictions chronologically...")
    
    for idx, pred_row in predictions_sorted.iterrows():
        stock = pred_row['Stock']
        grade = pred_row['Grade']
        entry_time = pred_row['entry_timestamp']
        date = pred_row['Date']
        
        # Find OHLC data for this stock on this date
        stock_data = calls_data[
            (calls_data['stock'] == stock) & 
            (calls_data['date'] == date)
        ].sort_values('timestamp')
        
        if stock_data.empty:
            continue
        
        # Execute the trade
        trade_result = execute_trade(stock_data, entry_time, stock, grade, current_capital)
        
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
        
        # Print progress every 50 trades
        if len(trades) % 50 == 0:
            print(f"Completed {len(trades)} trades, Capital: ₹{current_capital:,.0f}")
    
    print(f"\nBacktest completed!")
    print(f"Final Capital: ₹{current_capital:,.0f}")
    print(f"Total Return: {((current_capital - initial_capital) / initial_capital * 100):,.2f}%")
    
    return pd.DataFrame(trades)

def analyze_performance(trades_df):
    """Analyze performance metrics"""
    
    if trades_df.empty:
        return pd.DataFrame()
    
    # Overall performance
    initial_capital = trades_df.iloc[0]['capital_before']
    final_capital = trades_df.iloc[-1]['capital_after']
    total_return = ((final_capital - initial_capital) / initial_capital) * 100
    
    # Grade-wise analysis
    grade_analysis = []
    
    for grade in trades_df['grade'].unique():
        grade_trades = trades_df[trades_df['grade'] == grade]
        
        total_trades = len(grade_trades)
        winning_trades = len(grade_trades[grade_trades['pnl_amount'] > 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        total_pnl = grade_trades['pnl_amount'].sum()
        avg_pnl = grade_trades['pnl_amount'].mean()
        
        # Count exit reasons
        stop_loss_count = len(grade_trades[grade_trades['exit_reason'] == 'Stop Loss'])
        take_profit_count = len(grade_trades[grade_trades['exit_reason'] == 'Take Profit'])
        market_close_count = len(grade_trades[grade_trades['exit_reason'] == 'Market Close'])
        
        analysis = {
            'grade': grade,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'win_rate_pct': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl_per_trade': avg_pnl,
            'stop_loss_exits': stop_loss_count,
            'take_profit_exits': take_profit_count,
            'market_close_exits': market_close_count
        }
        
        grade_analysis.append(analysis)
    
    grade_df = pd.DataFrame(grade_analysis)
    
    # Print summary
    print("\n" + "="*60)
    print("DYNAMIC CAPITAL BACKTEST RESULTS")
    print("="*60)
    print(f"Initial Capital: ₹{initial_capital:,.0f}")
    print(f"Final Capital: ₹{final_capital:,.0f}")
    print(f"Total Return: {total_return:.2f}%")
    print(f"Total Trades: {len(trades_df)}")
    
    print(f"\nGRADE PERFORMANCE:")
    for _, row in grade_df.iterrows():
        print(f"  Grade {row['grade']}: {row['total_trades']} trades, {row['win_rate_pct']:.1f}% win rate, ₹{row['total_pnl']:,.0f} P&L")
    
    return grade_df

def main():
    """Main execution function"""
    
    print("Loading data...")
    calls_data, predictions = load_data()
    
    print(f"Loaded {len(calls_data)} OHLC records")
    print(f"Loaded {len(predictions)} predictions")
    
    # Run dynamic backtest
    trades_df = run_dynamic_backtest(calls_data, predictions)
    
    # Analyze performance
    grade_performance = analyze_performance(trades_df)
    
    # Save results
    trades_df.to_csv('dynamic_backtest_trades.csv', index=False)
    grade_performance.to_csv('dynamic_grade_performance.csv', index=False)
    
    print(f"\nResults saved:")
    print(f"  Trades: dynamic_backtest_trades.csv")
    print(f"  Grade Analysis: dynamic_grade_performance.csv")
    
    return trades_df, grade_performance

if __name__ == "__main__":
    main() 