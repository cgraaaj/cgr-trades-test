#!/usr/bin/env python3
"""
Consolidated Options Data Processor
=====================================

This script combines the functionality of:
1. create_excel_with_instrument_keys.py - adds instrument keys and separates calls/puts
2. fetch_both_calls_puts_ohlc.py - fetches OHLC data from Upstox API

Input: Excel file from option_analyze_optimized.py
Output: Enhanced Excel file with calls/puts sheets + CSV files with OHLC data
"""

import json
import sys
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from datetime import datetime
import asyncio
import aiohttp
import logging
import urllib.parse
from typing import List, Dict, Optional, Tuple
import glob

# Add parent directory to path to import db_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import db_config as config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
API_BASE_URL = "https://api.upstox.com/v3/historical-candle"
DEFAULT_INTERVAL = "5"  # 5 minutes interval to match optimized analysis
MAX_CONCURRENT_REQUESTS = 5  # Control API concurrency
MAX_RETRIES = 3

class OptionsDataProcessor:
    """Consolidated options data processor with instrument keys and OHLC fetching"""
    
    def __init__(self, interval: str = DEFAULT_INTERVAL, max_concurrent: int = MAX_CONCURRENT_REQUESTS):
        self.interval = interval
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.stock_to_instrument_key = {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_instrument_keys_mapping(self) -> Dict[str, str]:
        """Get mapping of stock names to instrument keys from database and NSE data."""
        try:
            self.logger.info("📊 Connecting to database...")
            engine = create_engine(config.DB_CONNECTION_STRING)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("✅ Database connection verified")
            
            # Get active stocks from database
            self.logger.info("📈 Fetching active stocks from database...")
            query = "SELECT * FROM options.stock WHERE is_active ORDER BY id ASC"
            with engine.connect() as conn:
                active_stocks = pd.read_sql(query, conn)
            
            active_stock_names = set(active_stocks['name'].tolist())
            self.logger.info(f"✅ Found {len(active_stocks)} active stocks in database")
            
            # Load NSE data
            nse_file_path = "/home/cgraaaj/Projects/cgr-trades/python/NSE.json"
            self.logger.info(f"📋 Loading NSE data from: {nse_file_path}")
            
            if not os.path.exists(nse_file_path):
                self.logger.warning(f"❌ NSE file not found: {nse_file_path}")
                return {}
            
            with open(nse_file_path, 'r') as file:
                nse_data = json.load(file)
            self.logger.info(f"✅ Loaded {len(nse_data)} instruments from NSE.json")
            
            # Create mapping of stock name to instrument key
            self.logger.info("🔍 Creating stock name to instrument key mapping...")
            
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
            
            self.logger.info(f"✅ Created mapping for {matched_count} stocks")
            self.stock_to_instrument_key = stock_to_instrument_key
            return stock_to_instrument_key
            
        except Exception as e:
            self.logger.error(f"❌ Error creating instrument key mapping: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return {}
    
    def separate_calls_puts_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Separate data into calls and puts based on available criteria."""
        
        # Method 1: Check if there's already an option_type column
        if 'option_type' in df.columns:
            calls_df = df[df['option_type'].str.upper() == 'CE'].copy()
            puts_df = df[df['option_type'].str.upper() == 'PE'].copy()
            self.logger.info(f"✅ Separated by option_type column: {len(calls_df)} calls, {len(puts_df)} puts")
            return calls_df, puts_df
        
        # Method 2: Check if there's a column indicating call/put
        option_indicators = ['Option_Type', 'Type', 'CE_PE', 'Call_Put']
        for col in option_indicators:
            if col in df.columns:
                calls_df = df[df[col].str.contains('C|Call', case=False, na=False)].copy()
                puts_df = df[df[col].str.contains('P|Put', case=False, na=False)].copy()
                self.logger.info(f"✅ Separated by {col} column: {len(calls_df)} calls, {len(puts_df)} puts")
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
            
            self.logger.info(f"✅ Separated by bullish bias: {len(calls_df)} calls (bullish), {len(puts_df)} puts (bearish)")
            return calls_df, puts_df
        
        # Method 4: If no clear way to separate, duplicate data for both sheets
        self.logger.info("⚠️ No clear way to separate calls/puts. Creating identical data for both sheets.")
        self.logger.info("💡 You may want to manually edit the sheets or provide more specific separation criteria.")
        
        # Add option_type column to distinguish the sheets
        calls_df = df.copy()
        puts_df = df.copy()
        calls_df['sheet_type'] = 'Calls'
        puts_df['sheet_type'] = 'Puts'
        
        return calls_df, puts_df
    
    def find_latest_predictions_file(self) -> Optional[str]:
        """Find the latest option predictions Excel file."""
        try:
            # Look for files matching the pattern from option_analyze_optimized.py
            patterns = [
                "option_predictions_optimized_*.xlsx",
                "../option_predictions_optimized_*.xlsx",
                "option_predictions_optimized.xlsx",
                "../option_predictions_optimized.xlsx"
            ]
            
            excel_files = []
            for pattern in patterns:
                excel_files.extend(glob.glob(pattern))
            
            if not excel_files:
                self.logger.error("❌ No Excel file found with option predictions")
                self.logger.info("💡 Please run option_analyze_optimized.py first to generate the predictions")
                return None
            
            # Use the most recent file
            latest_file = max(excel_files, key=os.path.getctime)
            self.logger.info(f"📊 Found latest predictions file: {latest_file}")
            return latest_file
            
        except Exception as e:
            self.logger.error(f"❌ Error finding predictions file: {e}")
            return None
    
    def process_excel_file(self, excel_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process Excel file to add instrument keys and separate calls/puts."""
        try:
            self.logger.info(f"📊 Reading Excel file: {excel_file}")
            
            if not os.path.exists(excel_file):
                self.logger.error(f"❌ Excel file not found: {excel_file}")
                return pd.DataFrame(), pd.DataFrame()
            
            # Check if Excel has multiple sheets
            xl_file = pd.ExcelFile(excel_file)
            sheet_names = xl_file.sheet_names
            self.logger.info(f"📋 Found {len(sheet_names)} sheet(s): {sheet_names}")
            
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
                    self.logger.info(f"✅ Found separate sheets: '{calls_sheet}' and '{puts_sheet}'")
                    calls_df = pd.read_excel(excel_file, sheet_name=calls_sheet)
                    puts_df = pd.read_excel(excel_file, sheet_name=puts_sheet)
                else:
                    # Read first sheet and separate data
                    self.logger.info(f"📊 Reading first sheet '{sheet_names[0]}' and separating data...")
                    original_df = pd.read_excel(excel_file, sheet_name=sheet_names[0])
                    calls_df, puts_df = self.separate_calls_puts_data(original_df)
            else:
                # Single sheet - read and separate
                self.logger.info("📊 Single sheet found, reading and separating data...")
                original_df = pd.read_excel(excel_file)
                calls_df, puts_df = self.separate_calls_puts_data(original_df)
            
            self.logger.info(f"✅ Calls data: {len(calls_df)} rows")
            self.logger.info(f"✅ Puts data: {len(puts_df)} rows")
            
            # Get instrument key mapping if not already loaded
            if not self.stock_to_instrument_key:
                self.logger.info("🔑 Fetching instrument keys...")
                self.get_instrument_keys_mapping()
            
            if not self.stock_to_instrument_key:
                self.logger.warning("❌ No instrument key mapping available. Adding empty instrument_key column.")
                # Add empty instrument_key column
                calls_df['instrument_key'] = None
                puts_df['instrument_key'] = None
            else:
                # Add instrument keys to both dataframes
                self.logger.info("✅ Adding instrument keys to both calls and puts data...")
                
                # Add instrument_key column to calls
                calls_df['instrument_key'] = calls_df['Stock'].map(self.stock_to_instrument_key)
                calls_stocks_with_keys = calls_df['instrument_key'].notna().sum()
                
                # Add instrument_key column to puts  
                puts_df['instrument_key'] = puts_df['Stock'].map(self.stock_to_instrument_key)
                puts_stocks_with_keys = puts_df['instrument_key'].notna().sum()
                
                self.logger.info(f"✅ Calls: Added instrument keys to {calls_stocks_with_keys}/{len(calls_df)} rows")
                self.logger.info(f"✅ Puts: Added instrument keys to {puts_stocks_with_keys}/{len(puts_df)} rows")
            
            return calls_df, puts_df
            
        except Exception as e:
            self.logger.error(f"❌ Error processing Excel file: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame(), pd.DataFrame()
    
    def save_enhanced_excel(self, calls_df: pd.DataFrame, puts_df: pd.DataFrame) -> str:
        """Save enhanced data with instrument keys to Excel."""
        try:
            # Create output filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"option_predictions_calls_puts_{timestamp}.xlsx"
            
            # Save both dataframes to separate sheets in the same workbook
            self.logger.info(f"💾 Saving enhanced Excel file: {output_file}")
            
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                calls_df.to_excel(writer, sheet_name='Calls', index=False)
                puts_df.to_excel(writer, sheet_name='Puts', index=False)
                
                # Add summary sheet
                summary_data = {
                    "Option_Type": ["Calls", "Puts"],
                    "Total_Rows": [len(calls_df), len(puts_df)],
                    "Unique_Stocks": [calls_df['Stock'].nunique() if not calls_df.empty else 0, 
                                     puts_df['Stock'].nunique() if not puts_df.empty else 0],
                    "Rows_With_Keys": [calls_df['instrument_key'].notna().sum() if not calls_df.empty else 0,
                                      puts_df['instrument_key'].notna().sum() if not puts_df.empty else 0]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            self.logger.info("✅ Successfully created enhanced Excel file with calls/puts sheets!")
            return output_file
            
        except Exception as e:
            self.logger.error(f"❌ Error saving enhanced Excel: {e}")
            return ""
    
    def construct_api_url(self, instrument_key: str, date: str) -> str:
        """Construct the Upstox API URL for fetching historical data."""
        # URL encode the instrument key
        encoded_instrument_key = urllib.parse.quote(instrument_key, safe='')
        
        # Format: https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}/{from_date}/{to_date}
        url = f"{API_BASE_URL}/{encoded_instrument_key}/minutes/{self.interval}/{date}/{date}"
        
        return url
    
    async def fetch_ohlc_data_with_retries(self, session: aiohttp.ClientSession, url: str, stock_info: Dict) -> Optional[pd.DataFrame]:
        """Fetch OHLC data from Upstox API with retry logic."""
        async with self.semaphore:
            for attempt in range(MAX_RETRIES):
                try:
                    self.logger.debug(f"Fetching data from: {url} (attempt {attempt + 1})")
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.logger.debug(f"Successfully fetched data from: {url}")
                            return self.process_api_response(data, stock_info)
                        else:
                            self.logger.warning(f"HTTP {response.status} for {url}")
                            
                except Exception as e:
                    self.logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        
            self.logger.error(f"Failed to fetch data after {MAX_RETRIES} attempts: {url}")
            return None
    
    def process_api_response(self, api_data: Dict, stock_info: Dict) -> Optional[pd.DataFrame]:
        """Process the API response and create a DataFrame."""
        try:
            if api_data.get("status") != "success":
                self.logger.warning(f"API returned unsuccessful status for {stock_info['stock']}")
                return None
                
            candles_data = api_data.get("data", {}).get("candles", [])
            
            if not candles_data:
                self.logger.warning(f"No candle data found for {stock_info['stock']} on {stock_info['date']}")
                return None

            # Create DataFrame from candles data
            df = pd.DataFrame(
                candles_data,
                columns=[
                    "timestamp",
                    "open",
                    "high",
                    "low", 
                    "close",
                    "volume",
                    "open_interest"
                ]
            )
            
            if df.empty:
                self.logger.warning(f"Empty DataFrame for {stock_info['stock']} on {stock_info['date']}")
                return None
            
            # Add metadata
            df["stock"] = stock_info["stock"]
            df["date"] = stock_info["date"]
            df["instrument_key"] = stock_info["instrument_key"]
            df["grade"] = stock_info.get("grade", "N/A")
            df["tn_ratio"] = stock_info.get("tn_ratio", "N/A")
            df["sheet_type"] = stock_info["sheet_type"]
            
            # Convert timestamp to datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Reorder columns for better readability
            cols = ["timestamp", "stock", "date", "grade", "tn_ratio", "open", "high", "low", "close", "volume", "open_interest", "instrument_key", "sheet_type"]
            df = df[[col for col in cols if col in df.columns]]
            
            self.logger.info(f"Successfully processed {len(df)} candles for {stock_info['stock']} on {stock_info['date']} ({stock_info['sheet_type']})")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing API response for {stock_info['stock']}: {e}")
            return None
    
    def prepare_api_requests(self, df: pd.DataFrame, sheet_type: str) -> List[Dict]:
        """Prepare list of API requests from a DataFrame."""
        requests = []
        
        if df.empty:
            self.logger.warning(f"No data found in {sheet_type} sheet")
            return requests
        
        # Filter rows that have both instrument_key and valid date
        valid_rows = df.dropna(subset=['instrument_key', 'Date'])
        
        if valid_rows.empty:
            self.logger.warning(f"No valid rows found with both instrument_key and Date in {sheet_type} sheet")
            return requests
        
        self.logger.info(f"Found {len(valid_rows)} valid rows for {sheet_type} API requests")
        
        # Get unique combinations of Date, Stock, and instrument_key
        unique_combinations = valid_rows.groupby(['Date', 'Stock', 'instrument_key']).first().reset_index()
        
        self.logger.info(f"Found {len(unique_combinations)} unique stock-date combinations for {sheet_type}")
        
        for _, row in unique_combinations.iterrows():
            # Convert date to string format (YYYY-MM-DD)
            if isinstance(row['Date'], pd.Timestamp):
                date_str = row['Date'].strftime('%Y-%m-%d')
            else:
                # Try to parse the date if it's a string
                try:
                    date_obj = pd.to_datetime(row['Date'])
                    date_str = date_obj.strftime('%Y-%m-%d')
                except:
                    self.logger.warning(f"Could not parse date: {row['Date']} for stock {row['Stock']} in {sheet_type}")
                    continue
            
            stock_info = {
                "stock": row['Stock'],
                "date": date_str,
                "instrument_key": row['instrument_key'],
                "grade": row.get('Grade', 'N/A'),
                "tn_ratio": row.get('TN_Ratio', 'N/A'),
                "sheet_type": sheet_type
            }
            
            # Construct API URL
            api_url = self.construct_api_url(row['instrument_key'], date_str)
            
            requests.append({
                "url": api_url,
                "stock_info": stock_info
            })
            
            self.logger.debug(f"Prepared {sheet_type} request for {row['Stock']} on {date_str}: {api_url}")
        
        self.logger.info(f"Prepared {len(requests)} API requests for {sheet_type}")
        return requests
    
    async def fetch_all_ohlc_data(self, requests: List[Dict], sheet_type: str) -> List[pd.DataFrame]:
        """Fetch OHLC data for all requests concurrently."""
        self.logger.info(f"Starting to fetch {sheet_type} OHLC data for {len(requests)} requests...")
        
        if not requests:
            self.logger.warning(f"No requests to process for {sheet_type}")
            return []
        
        dataframes = []
        
        async with aiohttp.ClientSession() as session:
            # Create tasks for all API calls
            tasks = [
                self.fetch_ohlc_data_with_retries(session, req["url"], req["stock_info"])
                for req in requests
            ]
            
            # Execute all tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            successful_dfs = 0
            failed_requests = 0
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Exception for {sheet_type} request {i}: {result}")
                    failed_requests += 1
                elif result is not None:
                    dataframes.append(result)
                    successful_dfs += 1
                else:
                    failed_requests += 1
            
            self.logger.info(f"{sheet_type}: Successfully fetched data for {successful_dfs} requests")
            self.logger.info(f"{sheet_type}: Failed requests: {failed_requests}")
        
        return dataframes
    
    async def process_complete_pipeline(self, excel_file: Optional[str] = None) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """Run the complete pipeline: Excel processing + OHLC fetching."""
        try:
            self.logger.info("🚀 Starting complete options data processing pipeline...")
            
            # Find Excel file if not provided
            if not excel_file:
                excel_file = self.find_latest_predictions_file()
                if not excel_file:
                    return None, None, None
            
            # Process Excel file to add instrument keys and separate calls/puts
            calls_df, puts_df = self.process_excel_file(excel_file)
            
            if calls_df.empty and puts_df.empty:
                self.logger.error("❌ No data found in either calls or puts")
                return None, None, None
            
            # Save enhanced Excel file
            enhanced_excel_file = self.save_enhanced_excel(calls_df, puts_df)
            
            # Prepare timestamp for output files
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            calls_output_file = None
            puts_output_file = None
            
            # Process Calls data if available
            if not calls_df.empty:
                self.logger.info(f"\n📈 Processing Calls data:")
                self.logger.info(f"  Total rows: {len(calls_df)}")
                self.logger.info(f"  Unique stocks: {calls_df['Stock'].nunique()}")
                self.logger.info(f"  Stocks with instrument keys: {calls_df['instrument_key'].notna().sum()}")
                
                # Prepare and fetch Calls data
                calls_requests = self.prepare_api_requests(calls_df, "Calls")
                if calls_requests:
                    self.logger.info(f"\n🔄 Fetching Calls OHLC data...")
                    calls_dataframes = await self.fetch_all_ohlc_data(calls_requests, "Calls")
                    
                    if calls_dataframes:
                        calls_combined_df = pd.concat(calls_dataframes, ignore_index=True)
                        calls_output_file = f"calls_ohlc_data_{timestamp}.csv"
                        calls_combined_df.to_csv(calls_output_file, index=False)
                        self.logger.info(f"💾 Saved Calls data to: {calls_output_file}")
                        self.logger.info(f"✅ Calls: {len(calls_combined_df)} total candle records")
            
            # Process Puts data if available
            if not puts_df.empty:
                self.logger.info(f"\n📈 Processing Puts data:")
                self.logger.info(f"  Total rows: {len(puts_df)}")
                self.logger.info(f"  Unique stocks: {puts_df['Stock'].nunique()}")
                self.logger.info(f"  Stocks with instrument keys: {puts_df['instrument_key'].notna().sum()}")
                
                # Prepare and fetch Puts data
                puts_requests = self.prepare_api_requests(puts_df, "Puts")
                if puts_requests:
                    self.logger.info(f"\n🔄 Fetching Puts OHLC data...")
                    puts_dataframes = await self.fetch_all_ohlc_data(puts_requests, "Puts")
                    
                    if puts_dataframes:
                        puts_combined_df = pd.concat(puts_dataframes, ignore_index=True)
                        puts_output_file = f"puts_ohlc_data_{timestamp}.csv"
                        puts_combined_df.to_csv(puts_output_file, index=False)
                        self.logger.info(f"💾 Saved Puts data to: {puts_output_file}")
                        self.logger.info(f"✅ Puts: {len(puts_combined_df)} total candle records")
            
            # Display final summary
            self.logger.info("\n" + "="*80)
            self.logger.info("📈 PIPELINE COMPLETION SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Input file:                   {excel_file}")
            self.logger.info(f"Enhanced Excel file:          {enhanced_excel_file}")
            if calls_output_file:
                self.logger.info(f"Calls OHLC CSV:               {calls_output_file}")
            if puts_output_file:
                self.logger.info(f"Puts OHLC CSV:                {puts_output_file}")
            self.logger.info("="*80)
            
            self.logger.info(f"\n✅ Successfully completed options data processing pipeline!")
            
            return enhanced_excel_file, calls_output_file, puts_output_file
            
        except Exception as e:
            self.logger.error(f"❌ Error in complete pipeline: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return None, None, None

async def main():
    """Main function to run the consolidated options data processor."""
    try:
        # Initialize processor with optimized settings
        processor = OptionsDataProcessor(
            interval=DEFAULT_INTERVAL,  # 5 minutes to match option_analyze_optimized.py
            max_concurrent=MAX_CONCURRENT_REQUESTS
        )
        
        # Run complete pipeline
        enhanced_excel, calls_csv, puts_csv = await processor.process_complete_pipeline()
        
        if enhanced_excel:
            success_parts = [f"Enhanced Excel: {enhanced_excel}"]
            if calls_csv:
                success_parts.append(f"Calls CSV: {calls_csv}")
            if puts_csv:
                success_parts.append(f"Puts CSV: {puts_csv}")
            
            print(f"\n🎉 Success! Generated: {' | '.join(success_parts)}")
            print("📊 Files ready for options trading analysis!")
        else:
            print("❌ Pipeline failed - check logs for details")
            
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main()) 