#!/usr/bin/env python3
"""
Script to extract instrument_key values from NSE.json for active stocks in the database.
Filters for NSE_EQ segment and EQ instrument_type only.
"""

import json
import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text

# Add parent directory to path to import db_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import db_config as config

def main():
    """Main function to extract instrument keys for active stocks."""
    try:
        print("🚀 Starting instrument key extraction...")
        
        # Database connection
        print("📊 Connecting to database...")
        engine = create_engine(config.DB_CONNECTION_STRING)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection verified")
        
        # Get active stocks
        print("📈 Fetching active stocks from database...")
        query = "SELECT * FROM options.stock WHERE is_active ORDER BY id ASC"
        with engine.connect() as conn:
            active_stocks = pd.read_sql(query, conn)
        
        active_stock_names = set(active_stocks['name'].tolist())
        print(f"✅ Found {len(active_stocks)} active stocks")
        
        # Load NSE data
        nse_file_path = "/home/cgraaaj/Projects/cgr-trades/data/raw/NSE.json"
        print(f"📋 Loading NSE data from: {nse_file_path}")
        
        with open(nse_file_path, 'r') as file:
            nse_data = json.load(file)
        print(f"✅ Loaded {len(nse_data)} instruments from NSE.json")
        
        # Extract matching instruments
        print("🔍 Filtering for NSE_EQ + EQ instruments matching active stocks...")
        
        matching_instruments = []
        for instrument in nse_data:
            # Filter criteria
            if (instrument.get('segment') == 'NSE_EQ' and 
                instrument.get('instrument_type') == 'EQ' and
                instrument.get('trading_symbol') in active_stock_names):
                
                matching_instruments.append({
                    'stock_name': instrument.get('trading_symbol'),
                    'instrument_key': instrument.get('instrument_key'),
                    'name': instrument.get('name'),
                    'exchange_token': instrument.get('exchange_token')
                })
        
        print(f"✅ Found {len(matching_instruments)} matching instruments")
        
        # Display results
        print("\n" + "="*60)
        print("📊 INSTRUMENT KEYS FOR ACTIVE STOCKS (NSE_EQ + EQ)")
        print("="*60)
        
        if matching_instruments:
            for i, instrument in enumerate(sorted(matching_instruments, key=lambda x: x['stock_name']), 1):
                print(f"{i:3d}. {instrument['stock_name']:<15} | {instrument['instrument_key']}")
        else:
            print("❌ No matching instruments found!")
            return
        
        # Summary statistics
        print("\n" + "="*60)
        print("📈 SUMMARY")
        print("="*60)
        print(f"Total active stocks in DB:      {len(active_stocks)}")
        print(f"Matching NSE_EQ+EQ instruments: {len(matching_instruments)}")
        
        # Show stocks without instruments
        stocks_with_instruments = {i['stock_name'] for i in matching_instruments}
        stocks_without_instruments = active_stock_names - stocks_with_instruments
        
        if stocks_without_instruments:
            print(f"Active stocks WITHOUT NSE_EQ+EQ: {len(stocks_without_instruments)}")
            for stock in sorted(stocks_without_instruments):
                print(f"  ❌ {stock}")
        
        print(f"\n✅ Instrument key extraction completed successfully!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

if __name__ == "__main__":
    main() 