#!/usr/bin/env python3
"""
Realistic Concurrent Trading Backtest
- Handles multiple concurrent positions
- Capital tied up in ongoing trades
- Real-time chronological processing
- Available capital changes as trades open/close
- Logs all output to file
- Configurable entry delay from prediction time
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict
import sys
import os

# CONFIGURATION: Entry delay from prediction time (in minutes)
# Example: If prediction is at 10:15 and PREDICTION_ENTRY_DELAY_MINUTES = 15,
# the trade will be executed at 10:30
PREDICTION_ENTRY_DELAY_MINUTES = 0

class Logger:
    """Logger class to write output to both console and file"""
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding='utf-8')
        
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()  # Ensure immediate write to file
        
    def flush(self):
        self.terminal.flush()
        self.log.flush()
        
    def close(self):
        self.log.close()

class Position:
    """Represents an active trading position"""
    def __init__(self, trade_id, stock, grade, trade_type, entry_time, entry_price, 
                 shares, capital_used, stop_loss_price, profit_target_price, pred_data):
        self.trade_id = trade_id
        self.stock = stock
        self.grade = grade
        self.trade_type = trade_type
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.shares = shares
        self.capital_used = capital_used
        self.stop_loss_price = stop_loss_price
        self.profit_target_price = profit_target_price
        self.pred_data = pred_data
        self.is_active = True

class ConcurrentBacktest:
    """Realistic backtest with concurrent position management"""
    
    def __init__(self, initial_capital=1000000):
        self.initial_capital = initial_capital
        self.total_capital = initial_capital
        self.available_capital = initial_capital
        self.tied_up_capital = 0
        
        self.active_positions = {}  # trade_id -> Position
        self.completed_trades = []
        self.capital_history = []
        self.daily_profits = []  # Track daily P&L
        self.trade_counter = 0

    def calculate_position_size(self, stock_price, grade):
        """Calculate position size based on grade and available capital"""
        grade_allocation = {
            'A': 0.4, 'B': 0.3, 'C': 0.2, 'D': 0.1
        }
        
        allocated_capital = self.available_capital * grade_allocation.get(grade, 0.05)
        shares = int(allocated_capital / stock_price)
        actual_capital_used = shares * stock_price
        
        return shares, actual_capital_used

    def can_open_position(self, capital_needed):
        """Check if we have enough available capital for a new position"""
        return self.available_capital >= capital_needed

    def open_position(self, stock, grade, trade_type, entry_time, entry_price, pred_data):
        """Open a new position if capital is available"""
        
        shares, capital_used = self.calculate_position_size(entry_price, grade)
        
        if shares == 0 or not self.can_open_position(capital_used):
            return None  # Cannot open position
        
        # Calculate stop loss and profit target
        if trade_type == 'LONG':
            stop_loss_price = entry_price * 0.995  # 0.5% below
            profit_target_price = entry_price * 1.01  # 1% above
        else:  # SHORT
            stop_loss_price = entry_price * 1.005  # 0.5% above
            profit_target_price = entry_price * 0.99  # 1% below

        self.trade_counter += 1
        position = Position(
            trade_id=self.trade_counter,
            stock=stock,
            grade=grade,
            trade_type=trade_type,
            entry_time=entry_time,
            entry_price=entry_price,
            shares=shares,
            capital_used=capital_used,
            stop_loss_price=stop_loss_price,
            profit_target_price=profit_target_price,
            pred_data=pred_data
        )
        
        # Update capital allocation
        self.available_capital -= capital_used
        self.tied_up_capital += capital_used
        self.active_positions[self.trade_counter] = position
        
        return position

    def check_exit_conditions(self, position, current_time, low_price, high_price, close_price):
        """Check if position should be closed based on price action"""
        
        if position.trade_type == 'LONG':
            # LONG: Stop loss when price goes down, profit when price goes up
            if low_price <= position.stop_loss_price:
                return position.stop_loss_price, "Stop Loss"
            elif high_price >= position.profit_target_price:
                return position.profit_target_price, "Take Profit"
        else:  # SHORT
            # SHORT: Stop loss when price goes up, profit when price goes down
            if high_price >= position.stop_loss_price:
                return position.stop_loss_price, "Stop Loss"
            elif low_price <= position.profit_target_price:
                return position.profit_target_price, "Take Profit"
        
        return None, None

    def close_position(self, position, exit_time, exit_price, exit_reason):
        """Close a position and update capital"""
        
        # Calculate P&L
        if position.trade_type == 'LONG':
            pnl_per_share = exit_price - position.entry_price
        else:  # SHORT
            pnl_per_share = position.entry_price - exit_price
        
        total_pnl = pnl_per_share * position.shares
        pnl_pct = (pnl_per_share / position.entry_price) * 100
        
        # Update capital
        returned_capital = position.capital_used + total_pnl
        self.available_capital += returned_capital
        self.tied_up_capital -= position.capital_used
        self.total_capital += total_pnl
        
        # Record completed trade
        trade_record = {
            'trade_id': position.trade_id,
            'stock': position.stock,
            'grade': position.grade,
            'trade_type': position.trade_type,
            'entry_time': position.entry_time,
            'exit_time': exit_time,
            'entry_price': position.entry_price,
            'exit_price': exit_price,
            'exit_reason': exit_reason,
            'shares': position.shares,
            'capital_used': position.capital_used,
            'pnl_amount': total_pnl,
            'pnl_pct': pnl_pct,
            'total_capital_after': self.total_capital,
            'available_capital_after': self.available_capital,
            'tied_up_capital': self.tied_up_capital,
            **position.pred_data
        }
        
        self.completed_trades.append(trade_record)
        
        # Remove from active positions
        del self.active_positions[position.trade_id]
        
        return trade_record

    def close_all_positions_at_market_close(self, market_close_time, close_prices):
        """Close all remaining positions at market close"""
        
        positions_to_close = list(self.active_positions.values())
        
        for position in positions_to_close:
            # Find closing price for this stock
            close_price = close_prices.get(position.stock, position.entry_price)
            self.close_position(position, market_close_time, close_price, "Market Close")

    def close_all_positions_for_day(self, date, final_prices):
        """Close all positions by 2:45 PM - AVOID MARKET CLOSE VOLATILITY"""
        
        # Get all positions opened on this date
        positions_to_close = [
            pos for pos in self.active_positions.values() 
            if pos.entry_time.date() == date
        ]
        
        # Create exit time for this date (2:45 PM IST - avoid market close volatility)
        exit_time = pd.Timestamp(f"{date} 14:45:00").tz_localize('Asia/Kolkata')
        
        for position in positions_to_close:
            close_price = final_prices.get(position.stock, position.entry_price)
            self.close_position(position, exit_time, close_price, "Avoid Close Volatility")

    def record_daily_profit(self, date, start_capital, end_capital):
        """Record daily profit/loss for tracking"""
        daily_pnl = end_capital - start_capital
        daily_pnl_pct = (daily_pnl / start_capital) * 100 if start_capital > 0 else 0
        
        self.daily_profits.append({
            'date': date,
            'start_capital': start_capital,
            'end_capital': end_capital,
            'daily_pnl': daily_pnl,
            'daily_pnl_pct': daily_pnl_pct
        })
        
        return daily_pnl, daily_pnl_pct

def load_all_data():
    """Load all data and prepare for chronological processing"""
    
    # Load OHLC data
    calls_data = pd.read_csv('../calls_ohlc_data_20250716_161308.csv')
    calls_data['timestamp'] = pd.to_datetime(calls_data['timestamp'])
    calls_data['trade_type'] = 'LONG'
    
    puts_data = pd.read_csv('../puts_ohlc_data_20250716_161308.csv')
    puts_data['timestamp'] = pd.to_datetime(puts_data['timestamp'])
    puts_data['trade_type'] = 'SHORT'
    
    # Combine OHLC data
    all_ohlc = pd.concat([calls_data, puts_data], ignore_index=True)
    all_ohlc = all_ohlc.sort_values('timestamp').reset_index(drop=True)
    
    # Load predictions
    calls_predictions = pd.read_excel('../option_predictions_calls_puts_20250718_002907.xlsx', sheet_name='Calls')
    puts_predictions = pd.read_excel('../option_predictions_calls_puts_20250718_002907.xlsx', sheet_name='Puts')
    
    calls_predictions['trade_type'] = 'LONG'
    puts_predictions['trade_type'] = 'SHORT'
    
    # Create entry timestamps
    for df in [calls_predictions, puts_predictions]:
        df['entry_timestamp'] = pd.to_datetime(df['Date'].astype(str) + ' ' + df['Time'].astype(str))
        df['entry_timestamp'] = df['entry_timestamp'].dt.tz_localize('Asia/Kolkata')
    
    # Combine and sort predictions
    all_predictions = pd.concat([calls_predictions, puts_predictions], ignore_index=True)
    all_predictions = all_predictions.sort_values('entry_timestamp').reset_index(drop=True)
    
    return all_ohlc, all_predictions

def run_realistic_backtest():
    """Run realistic INTRADAY backtest with concurrent positions"""
    
    print("Loading data for realistic concurrent INTRADAY backtest...")
    all_ohlc, all_predictions = load_all_data()
    
    print(f"Loaded {len(all_ohlc)} OHLC records")
    print(f"Loaded {len(all_predictions)} predictions")
    print("INTRADAY ONLY: All positions close by 2:45 PM (avoid market close volatility)")
    print(f"ENTRY DELAY: Trades executed {PREDICTION_ENTRY_DELAY_MINUTES} minutes after prediction time")
    
    backtest = ConcurrentBacktest()
    
    # Group data by date for intraday processing
    all_ohlc['date_only'] = all_ohlc['timestamp'].dt.date
    all_predictions['date_only'] = all_predictions['entry_timestamp'].dt.date
    
    # Get all unique trading dates
    trading_dates = sorted(set(all_ohlc['date_only']))
    
    print(f"Processing {len(trading_dates)} trading days...")
    
    for date_idx, current_date in enumerate(trading_dates):
        print(f"\n--- TRADING DAY {date_idx + 1}: {current_date} ---")
        
        # Record start-of-day capital for daily P&L tracking
        start_of_day_capital = backtest.total_capital
        
        # Get data for this trading day only
        day_ohlc = all_ohlc[all_ohlc['date_only'] == current_date].copy()
        day_predictions = all_predictions[all_predictions['date_only'] == current_date].copy()
        
        if day_ohlc.empty:
            continue
        
        # Group OHLC data by timestamp for this day
        ohlc_by_time = defaultdict(list)
        for _, row in day_ohlc.iterrows():
            ohlc_by_time[row['timestamp']].append(row)
        
        # Process all timestamps for this day chronologically
        day_timestamps = sorted(set(day_ohlc['timestamp']))
        
        for current_time in day_timestamps:
            # Check for new trade signals at this timestamp
            # Look for predictions made PREDICTION_ENTRY_DELAY_MINUTES ago
            prediction_time = current_time - timedelta(minutes=PREDICTION_ENTRY_DELAY_MINUTES)
            new_signals = day_predictions[day_predictions['entry_timestamp'] == prediction_time]
            
            for _, signal in new_signals.iterrows():
                # Find matching OHLC data for this signal
                matching_ohlc = None
                for ohlc_row in ohlc_by_time[current_time]:
                    if (ohlc_row['stock'] == signal['Stock'] and 
                        ohlc_row['date'] == signal['Date'] and
                        ohlc_row['trade_type'] == signal['trade_type']):
                        matching_ohlc = ohlc_row
                        break
                
                if matching_ohlc is not None:
                    # Try to open new position
                    pred_data = {
                        'date': signal['Date'],
                        'bullish_count': signal['Bullish_Count'],
                        'bearish_count': signal['Bearish_Count'],
                        'tn_ratio': signal['TN_Ratio']
                    }
                    
                    position = backtest.open_position(
                        stock=signal['Stock'],
                        grade=signal['Grade'], 
                        trade_type=signal['trade_type'],
                        entry_time=current_time,
                        entry_price=matching_ohlc['open'],
                        pred_data=pred_data
                    )
                    
                    if position:
                        print(f"  {current_time.strftime('%H:%M')} Opened {position.trade_type} #{position.trade_id}: {position.stock} @ ₹{position.entry_price:.2f} (pred: {prediction_time.strftime('%H:%M')})")
            
            # Check exit conditions for all active positions opened TODAY
            positions_to_close = []
            
            for position in list(backtest.active_positions.values()):
                # Only check positions opened today
                if position.entry_time.date() != current_date:
                    continue
                
                # Find OHLC data for this position at current time
                matching_ohlc = None
                for ohlc_row in ohlc_by_time[current_time]:
                    if (ohlc_row['stock'] == position.stock and
                        ohlc_row['trade_type'] == position.trade_type and
                        ohlc_row['timestamp'] >= position.entry_time):
                        matching_ohlc = ohlc_row
                        break
                
                if matching_ohlc is not None:
                    exit_price, exit_reason = backtest.check_exit_conditions(
                        position, current_time, 
                        matching_ohlc['low'], matching_ohlc['high'], matching_ohlc['close']
                    )
                    
                    if exit_price is not None:
                        positions_to_close.append((position, exit_price, exit_reason))
            
            # Close positions that hit exit conditions
            for position, exit_price, exit_reason in positions_to_close:
                backtest.close_position(position, current_time, exit_price, exit_reason)
                print(f"  {current_time.strftime('%H:%M')} Closed {position.trade_type} #{position.trade_id}: {position.stock} @ ₹{exit_price:.2f} ({exit_reason})")
            
            # Auto-close positions at 2:45 PM to avoid market close volatility
            if current_time.hour == 14 and current_time.minute >= 45:
                auto_close_positions = [
                    pos for pos in backtest.active_positions.values() 
                    if pos.entry_time.date() == current_date
                ]
                
                for position in auto_close_positions:
                    # Find current price for auto-close
                    close_price = None
                    for ohlc_row in ohlc_by_time[current_time]:
                        if (ohlc_row['stock'] == position.stock and
                            ohlc_row['trade_type'] == position.trade_type):
                            close_price = ohlc_row['close']
                            break
                    
                    if close_price is not None:
                        backtest.close_position(position, current_time, close_price, "Avoid Close Volatility")
                        print(f"  {current_time.strftime('%H:%M')} Auto-closed {position.trade_type} #{position.trade_id}: {position.stock} @ ₹{close_price:.2f} (Avoid Close Volatility)")
        
        # MANDATORY: Close all remaining positions opened today by 2:45 PM (avoid close volatility)
        day_end_positions = [
            pos for pos in backtest.active_positions.values() 
            if pos.entry_time.date() == current_date
        ]
        
        if day_end_positions:
            print(f"  14:45 POSITION EXIT: Closing {len(day_end_positions)} remaining positions (avoid market close volatility)")
            
            # Get prices at 2:45 PM or closest available time
            cutoff_time = pd.Timestamp(f"{current_date} 14:45:00").tz_localize('Asia/Kolkata')
            available_times = [t for t in day_timestamps if t <= cutoff_time]
            
            if available_times:
                exit_time = max(available_times)  # Latest time at or before 2:45 PM
                exit_ohlc = day_ohlc[day_ohlc['timestamp'] == exit_time]
            else:
                # Fallback to last available time if no data before 2:45 PM
                exit_ohlc = day_ohlc[day_ohlc['timestamp'] == day_timestamps[-1]]
            
            final_prices = {}
            for _, row in exit_ohlc.iterrows():
                final_prices[row['stock']] = row['close']
            
            backtest.close_all_positions_for_day(current_date, final_prices)
        
        # Print end-of-day summary with daily P&L
        day_trades = len([t for t in backtest.completed_trades if pd.to_datetime(t['date']).date() == current_date])
        
        # Calculate and record daily profit
        daily_pnl, daily_pnl_pct = backtest.record_daily_profit(
            current_date, start_of_day_capital, backtest.total_capital
        )
        
        # Enhanced end-of-day logging
        pnl_sign = "📈" if daily_pnl >= 0 else "📉"
        print(f"  End of day: {day_trades} trades completed")
        print(f"  {pnl_sign} Daily P&L: ₹{daily_pnl:,.0f} ({daily_pnl_pct:+.2f}%)")
        print(f"  💰 Total Capital: ₹{backtest.total_capital:,.0f} (Available: ₹{backtest.available_capital:,.0f})")
    
    print(f"\nRealistic INTRADAY backtest completed!")
    print(f"Total trades executed: {len(backtest.completed_trades)}")
    print(f"Final capital: ₹{backtest.total_capital:,.0f}")
    print(f"Total return: {((backtest.total_capital - backtest.initial_capital) / backtest.initial_capital * 100):.2f}%")
    
    return pd.DataFrame(backtest.completed_trades), backtest

def analyze_realistic_performance(trades_df, backtest):
    """Analyze the realistic concurrent trading performance (2:45 PM exit to avoid volatility)"""
    
    if trades_df.empty:
        return
    
    # Overall performance
    total_return = ((backtest.total_capital - backtest.initial_capital) / backtest.initial_capital) * 100
    
    # Performance by trade type
    long_trades = trades_df[trades_df['trade_type'] == 'LONG']
    short_trades = trades_df[trades_df['trade_type'] == 'SHORT']
    
    print("\n" + "="*70)
    print("REALISTIC CONCURRENT TRADING RESULTS")
    print("="*70)
    print(f"Initial Capital: ₹{backtest.initial_capital:,.0f}")
    print(f"Final Capital: ₹{backtest.total_capital:,.0f}")
    print(f"Total Return: {total_return:.2f}%")
    print(f"Total Trades: {len(trades_df)}")
    
    if not long_trades.empty:
        long_pnl = long_trades['pnl_amount'].sum()
        long_win_rate = (len(long_trades[long_trades['pnl_amount'] > 0]) / len(long_trades)) * 100
        print(f"\nLONG Trades (Calls): {len(long_trades)} trades, {long_win_rate:.1f}% win rate, ₹{long_pnl:,.0f} P&L")
    
    if not short_trades.empty:
        short_pnl = short_trades['pnl_amount'].sum()
        short_win_rate = (len(short_trades[short_trades['pnl_amount'] > 0]) / len(short_trades)) * 100
        print(f"SHORT Trades (Puts): {len(short_trades)} trades, {short_win_rate:.1f}% win rate, ₹{short_pnl:,.0f} P&L")
    
    # Grade analysis
    print(f"\nGRADE PERFORMANCE:")
    for grade in sorted(trades_df['grade'].unique()):
        grade_trades = trades_df[trades_df['grade'] == grade]
        grade_pnl = grade_trades['pnl_amount'].sum()
        grade_win_rate = (len(grade_trades[grade_trades['pnl_amount'] > 0]) / len(grade_trades)) * 100
        print(f"  Grade {grade}: {len(grade_trades)} trades, {grade_win_rate:.1f}% win rate, ₹{grade_pnl:,.0f} P&L")
    
    # Daily performance analysis
    if backtest.daily_profits:
        print(f"\nDAILY PERFORMANCE SUMMARY:")
        daily_df = pd.DataFrame(backtest.daily_profits)
        
        profitable_days = len(daily_df[daily_df['daily_pnl'] > 0])
        loss_days = len(daily_df[daily_df['daily_pnl'] < 0])
        breakeven_days = len(daily_df[daily_df['daily_pnl'] == 0])
        
        best_day = daily_df.loc[daily_df['daily_pnl'].idxmax()]
        worst_day = daily_df.loc[daily_df['daily_pnl'].idxmin()]
        
        avg_daily_pnl = daily_df['daily_pnl'].mean()
        avg_daily_pnl_pct = daily_df['daily_pnl_pct'].mean()
        
        print(f"  📊 Trading Days: {len(daily_df)} total")
        print(f"  📈 Profitable Days: {profitable_days} ({profitable_days/len(daily_df)*100:.1f}%)")
        print(f"  📉 Loss Days: {loss_days} ({loss_days/len(daily_df)*100:.1f}%)")
        if breakeven_days > 0:
            print(f"  ➖ Breakeven Days: {breakeven_days}")
        
        print(f"  💰 Average Daily P&L: ₹{avg_daily_pnl:,.0f} ({avg_daily_pnl_pct:+.2f}%)")
        print(f"  🏆 Best Day: {best_day['date']} → ₹{best_day['daily_pnl']:,.0f} ({best_day['daily_pnl_pct']:+.2f}%)")
        print(f"  💸 Worst Day: {worst_day['date']} → ₹{worst_day['daily_pnl']:,.0f} ({worst_day['daily_pnl_pct']:+.2f}%)")
        
        # Show last 5 days performance
        print(f"\n📅 LAST 5 TRADING DAYS:")
        last_5_days = daily_df.tail(5)
        for _, day in last_5_days.iterrows():
            pnl_sign = "📈" if day['daily_pnl'] >= 0 else "📉"
            print(f"  {pnl_sign} {day['date']}: ₹{day['daily_pnl']:,.0f} ({day['daily_pnl_pct']:+.2f}%)")

def main():
    """Main execution function with logging"""
    
    # Initialize logging with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f'realistic_backtest_log_{timestamp}.txt'
    
    # Set up dual output (console + file)
    logger = Logger(log_filename)
    sys.stdout = logger
    
    try:
        print(f"🚀 REALISTIC CONCURRENT TRADING BACKTEST")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📝 Logging to: {log_filename}")
        print("=" * 70)
        
        # Run realistic backtest
        trades_df, backtest = run_realistic_backtest()
        
        # Analyze results
        analyze_realistic_performance(trades_df, backtest)
        
        # Save results
        trades_df.to_csv('realistic_concurrent_trades.csv', index=False)
        
        # Save daily profits if available
        if backtest.daily_profits:
            daily_profits_df = pd.DataFrame(backtest.daily_profits)
            daily_profits_df.to_csv('daily_profits.csv', index=False)
        
        print(f"\nResults saved:")
        print(f"  Realistic Trades: realistic_concurrent_trades.csv")
        if backtest.daily_profits:
            print(f"  Daily Profits: daily_profits.csv")
        print(f"  Log File: {log_filename}")
        
        print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        return trades_df, backtest
        
    finally:
        # Restore original stdout and close log file
        sys.stdout = logger.terminal
        logger.close()
        print(f"✅ Backtest completed. Log saved to: {log_filename}")

if __name__ == "__main__":
    main() 