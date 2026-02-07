#!/usr/bin/env python3
"""
Analyze log files to identify dates that need reprocessing
"""
import os
import re
from datetime import datetime
from pathlib import Path

def analyze_log_file(log_path):
    """Analyze a single log file and extract key metrics"""
    try:
        with open(log_path, 'r') as f:
            content = f.read()
        
        # Extract date being processed
        date_match = re.search(r'Running program for (?:single date|date range): ([\d-]+)', content)
        if not date_match:
            return None
        
        processed_date = date_match.group(1)
        
        # Check if completed successfully
        completed = 'Script completed successfully' in content
        
        # Extract success rate
        success_match = re.search(r'Successful: (\d+) \(([\d.]+)%\)', content)
        success_count = int(success_match.group(1)) if success_match else 0
        success_rate = float(success_match.group(2)) if success_match else 0.0
        
        # Extract total records
        records_match = re.search(r'Collected (\d+) total ticker records', content)
        total_records = int(records_match.group(1)) if records_match else 0
        
        # Check for database errors
        db_error = 'Database connection failed' in content or 'pg_filenode.map' in content
        
        # Count 400 errors
        error_400_count = len(re.findall(r'400, message=\'Bad Request\'', content))
        
        # Count unexpected errors
        unexpected_errors = len(re.findall(r'ERROR.*Unexpected error', content))
        
        return {
            'log_file': os.path.basename(log_path),
            'processed_date': processed_date,
            'completed': completed,
            'success_count': success_count,
            'success_rate': success_rate,
            'total_records': total_records,
            'db_error': db_error,
            'error_400_count': error_400_count,
            'unexpected_errors': unexpected_errors,
        }
    except Exception as e:
        print(f"Error analyzing {log_path}: {e}")
        return None


def main():
    """Main function to analyze all log files"""
    log_dir = Path('/home/cgraaaj/Projects/cgr-trades/logs')
    
    # Find all opt_stock_data logs
    log_files = sorted(log_dir.glob('opt_stock_data_*.log'))
    
    print("=" * 100)
    print("NSE Options Data Collection - Log Analysis")
    print("=" * 100)
    print()
    
    results = []
    for log_file in log_files:
        result = analyze_log_file(log_file)
        if result:
            results.append(result)
    
    # Sort by date
    results.sort(key=lambda x: x['processed_date'])
    
    # Display results
    print(f"{'Date':<12} {'Log File':<30} {'Status':<12} {'Success%':<10} {'Records':<12} {'Errors'}")
    print("-" * 100)
    
    needs_rerun = []
    
    for r in results:
        status = '✓ SUCCESS' if r['completed'] and not r['db_error'] else '✗ FAILED'
        
        # Determine if needs rerun
        if r['db_error']:
            status = '✗ DB ERROR'
            needs_rerun.append(r['processed_date'])
        elif not r['completed']:
            status = '✗ INCOMPLETE'
            needs_rerun.append(r['processed_date'])
        elif r['success_rate'] < 25:  # Less than 25% success rate
            status = '⚠ LOW SUCCESS'
            needs_rerun.append(r['processed_date'])
        elif r['unexpected_errors'] > 10:
            status = '⚠ MANY ERRORS'
        
        errors_str = f"400:{r['error_400_count']} Unexp:{r['unexpected_errors']}"
        
        print(f"{r['processed_date']:<12} {r['log_file']:<30} {status:<12} "
              f"{r['success_rate']:>6.1f}%    {r['total_records']:>10,}  {errors_str}")
    
    print()
    print("=" * 100)
    print("Summary:")
    print(f"  Total log files analyzed: {len(results)}")
    print(f"  Successful runs: {sum(1 for r in results if r['completed'] and not r['db_error'])}")
    print(f"  Failed/Incomplete runs: {len(needs_rerun)}")
    print(f"  Total records collected: {sum(r['total_records'] for r in results):,}")
    print()
    
    if needs_rerun:
        print("⚠ Dates that need to be re-run:")
        print("-" * 100)
        for date in sorted(set(needs_rerun)):
            print(f"  - {date}")
        print()
        print("Command to re-run:")
        if len(needs_rerun) == 1:
            print(f"  python3 opt-stk-data-to-db-upstox.py {needs_rerun[0]}")
        else:
            dates_sorted = sorted(set(needs_rerun))
            print(f"  python3 opt-stk-data-to-db-upstox.py {dates_sorted[0]} {dates_sorted[-1]}")
    else:
        print("✓ All dates processed successfully!")
    
    print("=" * 100)
    print()
    print("Notes:")
    print("  - 400 errors are NORMAL (instruments with no trading data)")
    print("  - Success rate of 30-35% is EXPECTED for F&O instruments")
    print("  - Only DB errors and incomplete runs need reprocessing")
    print()


if __name__ == '__main__':
    main()

