#!/usr/bin/env python3
"""
Script to create a new Excel file with instrument keys added to the original data.
Creates separate "Calls" and "Puts" sheets in the same workbook.
"""

import json
import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime

# Add parent directory to path to import db_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import db_config as config

def get_instrument_keys_mapping():
    """Get mapping of stock names to instrument keys from database and NSE data."""
    try:
        print("📊 Connecting to database...")
        engine = create_engine(config.DB_CONNECTION_STRING)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("✅ Database connection verified")
        
        # Get active stocks from database
        print("📈 Fetching active stocks from database...")
        query = "SELECT * FROM options.stock WHERE is_active ORDER BY id ASC"
        with engine.connect() as conn:
            active_stocks = pd.read_sql(query, conn)
        
        active_stock_names = set(active_stocks['name'].tolist())
        print(f"✅ Found {len(active_stocks)} active stocks in database")
        
        # Load NSE data
        nse_file_path = "/home/cgraaaj/Projects/cgr-trades/python/NSE.json"
        print(f"📋 Loading NSE data from: {nse_file_path}")
        
        if not os.path.exists(nse_file_path):
            print(f"❌ NSE file not found: {nse_file_path}")
            return {}
        
        with open(nse_file_path, 'r') as file:
            nse_data = json.load(file)
        print(f"✅ Loaded {len(nse_data)} instruments from NSE.json")
        
        # Create mapping of stock name to instrument key
        print("🔍 Creating stock name to instrument key mapping...")
        
        stock_to_instrument_key = {}
        matched_count = 0
        
        for instrument in nse_data:
            # Filter criteria: NSE_EQ segment and EQ instrument_type
            if (instrument.get('segment') == 'NSE_EQ' and 
                instrument.get('instrument_type') == 'EQ' and
                instrument.get('trading_symbol') in active_stock_names):
                
                stock_name = instrument.get('trading_symbol')
                instrument_key = instrument.get('instrument_key')
                
                if stock_name and instrument_key:
                    stock_to_instrument_key[stock_name] = instrument_key
                    matched_count += 1
        
        print(f"✅ Created mapping for {matched_count} stocks")
        return stock_to_instrument_key
        
    except Exception as e:
        print(f"❌ Error creating instrument key mapping: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {}

def separate_calls_puts_data(df):
    """Separate data into calls and puts based on available criteria."""
    
    # Method 1: Check if there's already an option_type column
    if 'option_type' in df.columns:
        calls_df = df[df['option_type'].str.upper() == 'CE'].copy()
        puts_df = df[df['option_type'].str.upper() == 'PE'].copy()
        print(f"✅ Separated by option_type column: {len(calls_df)} calls, {len(puts_df)} puts")
        return calls_df, puts_df
    
    # Method 2: Check if there's a column indicating call/put
    option_indicators = ['Option_Type', 'Type', 'CE_PE', 'Call_Put']
    for col in option_indicators:
        if col in df.columns:
            calls_df = df[df[col].str.contains('C|Call', case=False, na=False)].copy()
            puts_df = df[df[col].str.contains('P|Put', case=False, na=False)].copy()
            print(f"✅ Separated by {col} column: {len(calls_df)} calls, {len(puts_df)} puts")
            return calls_df, puts_df
    
    # Method 3: If no clear separation, create logical separation based on data patterns
    # For trading data, we can separate based on bullish vs bearish indicators
    if 'Bullish_Count' in df.columns and 'Bearish_Count' in df.columns:
        # Consider rows with higher bullish count as "calls" and higher bearish as "puts"
        df['bullish_bias'] = df['Bullish_Count'] > df['Bearish_Count']
        calls_df = df[df['bullish_bias'] == True].copy()
        puts_df = df[df['bullish_bias'] == False].copy()
        
        # Remove the temporary column
        calls_df = calls_df.drop('bullish_bias', axis=1)
        puts_df = puts_df.drop('bullish_bias', axis=1)
        
        print(f"✅ Separated by bullish bias: {len(calls_df)} calls (bullish), {len(puts_df)} puts (bearish)")
        return calls_df, puts_df
    
    # Method 4: If no clear way to separate, duplicate data for both sheets
    print("⚠️ No clear way to separate calls/puts. Creating identical data for both sheets.")
    print("💡 You may want to manually edit the sheets or provide more specific separation criteria.")
    
    # Add option_type column to distinguish the sheets
    calls_df = df.copy()
    puts_df = df.copy()
    calls_df['sheet_type'] = 'Calls'
    puts_df['sheet_type'] = 'Puts'
    
    return calls_df, puts_df

def main():
    """Main function to create Excel file with separate Calls and Puts sheets."""
    try:
        print("🚀 Starting Excel file creation with separate Calls/Puts sheets...")
        
        # Read the original Excel file
        excel_file = "../option_predictions_optimized.xlsx"
        print(f"📊 Reading original Excel file: {excel_file}")
        
        if not os.path.exists(excel_file):
            print(f"❌ Excel file not found: {excel_file}")
            return
        
        # Check if Excel has multiple sheets
        xl_file = pd.ExcelFile(excel_file)
        sheet_names = xl_file.sheet_names
        print(f"📋 Found {len(sheet_names)} sheet(s): {sheet_names}")
        
        # Read data - if multiple sheets exist, try to identify calls/puts sheets
        if len(sheet_names) > 1:
            calls_sheet = None
            puts_sheet = None
            
            # Look for sheets with call/put indicators
            for sheet in sheet_names:
                sheet_lower = sheet.lower()
                if 'call' in sheet_lower or 'ce' in sheet_lower:
                    calls_sheet = sheet
                elif 'put' in sheet_lower or 'pe' in sheet_lower:
                    puts_sheet = sheet
            
            if calls_sheet and puts_sheet:
                print(f"✅ Found separate sheets: '{calls_sheet}' and '{puts_sheet}'")
                calls_df = pd.read_excel(excel_file, sheet_name=calls_sheet)
                puts_df = pd.read_excel(excel_file, sheet_name=puts_sheet)
            else:
                # Read first sheet and separate data
                print(f"📊 Reading first sheet '{sheet_names[0]}' and separating data...")
                original_df = pd.read_excel(excel_file, sheet_name=sheet_names[0])
                calls_df, puts_df = separate_calls_puts_data(original_df)
        else:
            # Single sheet - read and separate
            print("📊 Single sheet found, reading and separating data...")
            original_df = pd.read_excel(excel_file)
            calls_df, puts_df = separate_calls_puts_data(original_df)
        
        print(f"✅ Calls data: {len(calls_df)} rows")
        print(f"✅ Puts data: {len(puts_df)} rows")
        
        # Show sample of original data
        print(f"\n📊 Sample of calls data:")
        print(calls_df.head(3))
        print(f"\n📊 Sample of puts data:")
        print(puts_df.head(3))
        
        # Get instrument key mapping
        print("\n🔑 Fetching instrument keys...")
        stock_to_instrument_key = get_instrument_keys_mapping()
        
        if not stock_to_instrument_key:
            print("❌ No instrument key mapping available. Creating Excel without instrument keys.")
            # Add empty instrument_key column
            calls_df['instrument_key'] = None
            puts_df['instrument_key'] = None
        else:
            # Add instrument keys to both dataframes
            print("✅ Adding instrument keys to both calls and puts data...")
            
            # Add instrument_key column to calls
            calls_df['instrument_key'] = calls_df['Stock'].map(stock_to_instrument_key)
            calls_stocks_with_keys = calls_df['instrument_key'].notna().sum()
            calls_unique_stocks_with_keys = calls_df[calls_df['instrument_key'].notna()]['Stock'].nunique()
            
            # Add instrument_key column to puts  
            puts_df['instrument_key'] = puts_df['Stock'].map(stock_to_instrument_key)
            puts_stocks_with_keys = puts_df['instrument_key'].notna().sum()
            puts_unique_stocks_with_keys = puts_df[puts_df['instrument_key'].notna()]['Stock'].nunique()
            
            print(f"✅ Calls: Added instrument keys to {calls_stocks_with_keys}/{len(calls_df)} rows")
            print(f"✅ Puts: Added instrument keys to {puts_stocks_with_keys}/{len(puts_df)} rows")
        
        # Create output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"option_predictions_calls_puts_{timestamp}.xlsx"
        
        # Save both dataframes to separate sheets in the same workbook
        print(f"\n💾 Saving data to workbook with separate sheets: {output_file}")
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            calls_df.to_excel(writer, sheet_name='Calls', index=False)
            puts_df.to_excel(writer, sheet_name='Puts', index=False)
        
        print("✅ Successfully created workbook with separate Calls and Puts sheets!")
        
        # Show sample of enhanced data
        print(f"\n📊 Sample of enhanced Calls data:")
        sample_cols = ['Date', 'Time', 'Stock', 'Grade', 'instrument_key'] if 'Date' in calls_df.columns else calls_df.columns[:4].tolist() + ['instrument_key']
        if 'instrument_key' not in sample_cols:
            sample_cols.append('instrument_key')
        available_cols = [col for col in sample_cols if col in calls_df.columns]
        print(calls_df[available_cols].head(5))
        
        print(f"\n📊 Sample of enhanced Puts data:")
        available_cols = [col for col in sample_cols if col in puts_df.columns]
        print(puts_df[available_cols].head(5))
        
        # Summary statistics
        print("\n" + "="*80)
        print("📈 SUMMARY")
        print("="*80)
        print(f"Original Excel file:           {excel_file}")
        print(f"Output Excel file:             {output_file}")
        print(f"")
        print(f"CALLS SHEET:")
        print(f"  Total rows:                  {len(calls_df)}")
        print(f"  Unique stocks:               {calls_df['Stock'].nunique()}")
        print(f"  Rows with instrument keys:   {calls_df['instrument_key'].notna().sum()}")
        print(f"  Stocks with instrument keys: {calls_df[calls_df['instrument_key'].notna()]['Stock'].nunique()}")
        print(f"")
        print(f"PUTS SHEET:")
        print(f"  Total rows:                  {len(puts_df)}")
        print(f"  Unique stocks:               {puts_df['Stock'].nunique()}")
        print(f"  Rows with instrument keys:   {puts_df['instrument_key'].notna().sum()}")
        print(f"  Stocks with instrument keys: {puts_df[puts_df['instrument_key'].notna()]['Stock'].nunique()}")
        
        # Show column info
        print(f"\nCalls sheet columns: {list(calls_df.columns)}")
        print(f"Puts sheet columns:  {list(puts_df.columns)}")
        
        print(f"\n✅ Successfully created Excel workbook with separate Calls/Puts sheets!")
        print(f"📁 File saved as: {output_file}")
        print(f"📊 Use this file for your options trading analysis with instrument keys!")
        
        return output_file
        
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return None

if __name__ == "__main__":
    main() 