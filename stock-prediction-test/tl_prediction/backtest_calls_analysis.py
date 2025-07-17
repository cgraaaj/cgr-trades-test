#!/usr/bin/env python3
"""
Backtest script for calls options trading analysis
Parameters:
- Stop Loss: 0.5%
- Profit Target: 1%
- Capital: 10 Lakh (1,000,000 INR)
- Intraday trading only
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

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

def calculate_position_size(capital, stock_price, grade):
    """Calculate position size based on grade and available capital"""
    
    # Grade-based allocation (adjust as needed)
    grade_allocation = {
        'A': 0.4,  # 40% for A grade
        'B': 0.3,  # 30% for B grade  
        'C': 0.2,  # 20% for C grade
    }
    
    allocated_capital = capital * grade_allocation.get(grade, 0.1)
    shares = int(allocated_capital / stock_price)
    
    return shares, allocated_capital

def run_backtest(calls_data, predictions, capital=1000000, stop_loss_pct=0.5, profit_target_pct=1.0):
    """Run the backtest with specified parameters"""
    
    trades = []
    
    # Iterate through each prediction to find corresponding OHLC data
    for _, pred_row in predictions.iterrows():
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
            print(f"Warning: No OHLC data found for {stock} on {date}")
            continue
        
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
        shares, allocated_capital = calculate_position_size(capital, entry_price, grade)
        
        if shares == 0:
            continue
            
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
            close_price = row['close']
            
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
        
        trade = {
            'stock': stock,
            'grade': grade,
            'date': date,
            'predicted_entry_time': entry_time,
            'actual_entry_time': actual_entry_time,
            'entry_price': entry_price,
            'exit_time': exit_time,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'shares': shares,
            'allocated_capital': allocated_capital,
            'pnl_amount': total_pnl,
            'pnl_pct': pnl_pct,
            'stop_loss_price': stop_loss_price,
            'profit_target_price': profit_target_price,
            'bullish_count': pred_row['Bullish_Count'],
            'bearish_count': pred_row['Bearish_Count'],
            'tn_ratio': pred_row['TN_Ratio']
        }
        
        trades.append(trade)
    
    return pd.DataFrame(trades)

def analyze_performance_by_grade(trades_df):
    """Analyze performance metrics by grade"""
    
    grade_analysis = []
    
    for grade in trades_df['grade'].unique():
        grade_trades = trades_df[trades_df['grade'] == grade]
        
        total_trades = len(grade_trades)
        winning_trades = len(grade_trades[grade_trades['pnl_amount'] > 0])
        losing_trades = len(grade_trades[grade_trades['pnl_amount'] < 0])
        
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        total_pnl = grade_trades['pnl_amount'].sum()
        avg_pnl_per_trade = grade_trades['pnl_amount'].mean()
        
        max_profit = grade_trades['pnl_amount'].max()
        max_loss = grade_trades['pnl_amount'].min()
        
        # Count exit reasons
        stop_loss_count = len(grade_trades[grade_trades['exit_reason'] == 'Stop Loss'])
        take_profit_count = len(grade_trades[grade_trades['exit_reason'] == 'Take Profit'])
        market_close_count = len(grade_trades[grade_trades['exit_reason'] == 'Market Close'])
        
        analysis = {
            'grade': grade,
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate_pct': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl_per_trade': avg_pnl_per_trade,
            'max_profit': max_profit,
            'max_loss': max_loss,
            'stop_loss_exits': stop_loss_count,
            'take_profit_exits': take_profit_count,
            'market_close_exits': market_close_count
        }
        
        grade_analysis.append(analysis)
    
    return pd.DataFrame(grade_analysis)

def main():
    """Main execution function"""
    
    print("Loading data...")
    calls_data, predictions = load_data()
    
    print(f"Loaded {len(calls_data)} OHLC records")
    print(f"Date range: {calls_data['date'].min()} to {calls_data['date'].max()}")
    print(f"Stocks: {calls_data['stock'].nunique()}")
    print(f"Grades: {sorted(calls_data['grade'].unique())}")
    
    print("\nRunning backtest...")
    trades_df = run_backtest(calls_data, predictions)
    
    print(f"Completed {len(trades_df)} trades")
    
    # Analyze performance by grade
    print("\nAnalyzing performance by grade...")
    grade_performance = analyze_performance_by_grade(trades_df)
    
    # Display results
    print("\n" + "="*60)
    print("PERFORMANCE BY GRADE")
    print("="*60)
    
    for _, row in grade_performance.iterrows():
        print(f"\nGRADE {row['grade']}:")
        print(f"  Total Trades: {row['total_trades']}")
        print(f"  Win Rate: {row['win_rate_pct']:.2f}%")
        print(f"  Total P&L: ₹{row['total_pnl']:,.2f}")
        print(f"  Avg P&L per Trade: ₹{row['avg_pnl_per_trade']:,.2f}")
        print(f"  Max Profit: ₹{row['max_profit']:,.2f}")
        print(f"  Max Loss: ₹{row['max_loss']:,.2f}")
        print(f"  Stop Loss Exits: {row['stop_loss_exits']}")
        print(f"  Take Profit Exits: {row['take_profit_exits']}")
        print(f"  Market Close Exits: {row['market_close_exits']}")
    
    # Find best performing grade
    best_grade = grade_performance.loc[grade_performance['total_pnl'].idxmax(), 'grade']
    best_pnl = grade_performance.loc[grade_performance['total_pnl'].idxmax(), 'total_pnl']
    
    print(f"\n" + "="*60)
    print(f"BEST PERFORMING GRADE: {best_grade} with Total P&L: ₹{best_pnl:,.2f}")
    print("="*60)
    
    # Save results (clean files without timestamps)
    trades_filename = 'backtest_trades_final.csv'
    grade_analysis_filename = 'grade_performance_final.csv'
    
    trades_df.to_csv(trades_filename, index=False)
    grade_performance.to_csv(grade_analysis_filename, index=False)
    
    print(f"\nResults saved:")
    print(f"  All trades: {trades_filename}")
    print(f"  Grade analysis: {grade_analysis_filename}")
    
    return trades_df, grade_performance

if __name__ == "__main__":
    main() 