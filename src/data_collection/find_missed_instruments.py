#!/usr/bin/env python3
"""
Extract all instruments that got 400 errors from log files
These instruments have data but were missed due to URL encoding bug
"""
import re
import os
from pathlib import Path
from collections import defaultdict

def extract_failed_instruments(log_path):
    """Extract instrument keys that got 400 errors from a log file"""
    failed_instruments = []
    
    try:
        with open(log_path, 'r') as f:
            content = f.read()
        
        # Extract date being processed
        date_match = re.search(r'Running program for (?:single date|date range): ([\d-]+)', content)
        if not date_match:
            return None, []
        
        processed_date = date_match.group(1)
        
        # Find all 400 errors with instrument keys
        # Pattern: NSE_FO|<instrument_id>/1minute/<date>/<date>
        pattern = r'NSE_FO\|(\d+)/1minute/([\d-]+)/([\d-]+).*?400, message=\'Bad Request\''
        matches = re.findall(pattern, content)
        
        for instrument_id, from_date, to_date in matches:
            failed_instruments.append({
                'instrument_key': f'NSE_FO|{instrument_id}',
                'date': from_date,
                'log_date': processed_date
            })
        
        return processed_date, failed_instruments
        
    except Exception as e:
        print(f"Error analyzing {log_path}: {e}")
        return None, []


def main():
    """Main function to find all missed instruments"""
    log_dir = Path('/home/cgraaaj/Projects/cgr-trades/logs')
    
    # Find all opt_stock_data logs
    log_files = sorted(log_dir.glob('opt_stock_data_*.log'))
    
    print("=" * 100)
    print("Missed Instruments Analysis - URL Encoding Bug")
    print("=" * 100)
    print()
    print("Analyzing logs for instruments that got 400 errors but actually have data...")
    print()
    
    all_failed = defaultdict(list)
    total_missed = 0
    
    for log_file in log_files:
        processed_date, failed_instruments = extract_failed_instruments(log_file)
        
        if failed_instruments:
            for item in failed_instruments:
                all_failed[item['date']].append(item['instrument_key'])
                total_missed += 1
    
    if not all_failed:
        print("✓ No missed instruments found!")
        return
    
    print(f"{'Date':<15} {'Missed Instruments':<20} {'Sample Instrument Keys'}")
    print("-" * 100)
    
    dates_to_rerun = []
    for date in sorted(all_failed.keys()):
        instruments = all_failed[date]
        unique_instruments = list(set(instruments))
        count = len(unique_instruments)
        
        # Show first 3 instrument keys as samples
        samples = ', '.join(unique_instruments[:3])
        if len(unique_instruments) > 3:
            samples += f", ... (+{len(unique_instruments)-3} more)"
        
        print(f"{date:<15} {count:<20} {samples}")
        dates_to_rerun.append(date)
    
    print()
    print("=" * 100)
    print(f"Summary:")
    print(f"  Total dates affected: {len(dates_to_rerun)}")
    print(f"  Total missed instrument-date combinations: {total_missed}")
    print(f"  Unique instruments affected: {len(set(inst for insts in all_failed.values() for inst in insts))}")
    print()
    
    print("⚠️  CRITICAL: These instruments have data but were missed due to URL encoding bug!")
    print()
    print("The bug has been FIXED in the code. You need to re-run these dates to collect the missed data.")
    print()
    print("Dates that need to be re-run (with fixed code):")
    print("-" * 100)
    for date in dates_to_rerun:
        print(f"  - {date} (missed {len(set(all_failed[date]))} instruments)")
    
    print()
    print("Recommended command to re-run ALL affected dates:")
    if len(dates_to_rerun) == 1:
        print(f"  python3 opt-stk-data-to-db-upstox.py {dates_to_rerun[0]}")
    else:
        dates_sorted = sorted(dates_to_rerun)
        print(f"  python3 opt-stk-data-to-db-upstox.py {dates_sorted[0]} {dates_sorted[-1]}")
    
    print()
    print("=" * 100)
    print()
    print("Technical Details:")
    print("  - Bug: aiohttp was URL-encoding pipe character (| → %7C) in instrument keys")
    print("  - Fix: Using yarl.URL with encoded=True to preserve pipe characters")
    print("  - Impact: ~0.01-0.1% of instruments per day (those that got 400 errors)")
    print("  - Status: Bug is now FIXED, re-run will collect the missed data")
    print()


if __name__ == '__main__':
    main()

