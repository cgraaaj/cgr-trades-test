#!/usr/bin/env python3
"""
Script to fetch OHLC data for stocks in both Calls and Puts sheets using Upstox API.
Creates separate CSV files for calls and puts data.
"""

import pandas as pd
import asyncio
import aiohttp
import logging
from datetime import datetime
import urllib.parse
from typing import List, Dict, Optional, Tuple
import sys
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
API_BASE_URL = "https://api.upstox.com/v3/historical-candle"
INTERVAL = "15"  # 15 minutes interval
semaphore = asyncio.Semaphore(5)  # Control API concurrency


async def fetch_ohlc_data_with_retries(session: aiohttp.ClientSession, url: str, stock_info: Dict, max_retries: int = 3) -> Optional[pd.DataFrame]:
    """Fetch OHLC data from Upstox API with retry logic."""
    async with semaphore:
        for attempt in range(max_retries):
            try:
                logger.debug(f"Fetching data from: {url} (attempt {attempt + 1})")
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug(f"Successfully fetched data from: {url}")
                        return process_api_response(data, stock_info)
                    else:
                        logger.warning(f"HTTP {response.status} for {url}")
                        
            except Exception as e:
                logger.error(f"Error fetching {url} (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                    
        logger.error(f"Failed to fetch data after {max_retries} attempts: {url}")
        return None


def process_api_response(api_data: Dict, stock_info: Dict) -> Optional[pd.DataFrame]:
    """Process the API response and create a DataFrame."""
    try:
        if api_data.get("status") != "success":
            logger.warning(f"API returned unsuccessful status for {stock_info['stock']}")
            return None
            
        candles_data = api_data.get("data", {}).get("candles", [])
        
        if not candles_data:
            logger.warning(f"No candle data found for {stock_info['stock']} on {stock_info['date']}")
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
            logger.warning(f"Empty DataFrame for {stock_info['stock']} on {stock_info['date']}")
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
        
        logger.info(f"Successfully processed {len(df)} candles for {stock_info['stock']} on {stock_info['date']} ({stock_info['sheet_type']})")
        return df
        
    except Exception as e:
        logger.error(f"Error processing API response for {stock_info['stock']}: {e}")
        return None


def construct_api_url(instrument_key: str, date: str, interval: str = "15") -> str:
    """Construct the Upstox API URL for fetching historical data."""
    # URL encode the instrument key
    encoded_instrument_key = urllib.parse.quote(instrument_key, safe='')
    
    # Format: https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}/{from_date}/{to_date}
    url = f"{API_BASE_URL}/{encoded_instrument_key}/minutes/{interval}/{date}/{date}"
    
    return url


def read_calls_puts_data(excel_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Read both Calls and Puts sheets from the Excel file."""
    try:
        logger.info(f"Reading Calls and Puts sheets from: {excel_file}")
        
        # Check if file exists
        if not os.path.exists(excel_file):
            logger.error(f"Excel file not found: {excel_file}")
            return pd.DataFrame(), pd.DataFrame()
        
        calls_df = pd.DataFrame()
        puts_df = pd.DataFrame()
        
        # Try to read the Calls sheet
        try:
            calls_df = pd.read_excel(excel_file, sheet_name='Calls')
            logger.info(f"Successfully loaded {len(calls_df)} rows from Calls sheet")
        except ValueError as e:
            if "Worksheet named 'Calls' not found" in str(e):
                logger.warning("Calls sheet not found")
            else:
                logger.error(f"Error reading Calls sheet: {e}")
        
        # Try to read the Puts sheet
        try:
            puts_df = pd.read_excel(excel_file, sheet_name='Puts')
            logger.info(f"Successfully loaded {len(puts_df)} rows from Puts sheet")
        except ValueError as e:
            if "Worksheet named 'Puts' not found" in str(e):
                logger.warning("Puts sheet not found")
            else:
                logger.error(f"Error reading Puts sheet: {e}")
        
        if calls_df.empty and puts_df.empty:
            logger.error("Neither Calls nor Puts sheets found. Available sheets:")
            xl_file = pd.ExcelFile(excel_file)
            for sheet in xl_file.sheet_names:
                logger.error(f"  - {sheet}")
        
        return calls_df, puts_df
                
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        return pd.DataFrame(), pd.DataFrame()


def prepare_api_requests(df: pd.DataFrame, sheet_type: str) -> List[Dict]:
    """Prepare list of API requests from a DataFrame."""
    requests = []
    
    if df.empty:
        logger.warning(f"No data found in {sheet_type} sheet")
        return requests
    
    # Filter rows that have both instrument_key and valid date
    valid_rows = df.dropna(subset=['instrument_key', 'Date'])
    
    if valid_rows.empty:
        logger.warning(f"No valid rows found with both instrument_key and Date in {sheet_type} sheet")
        return requests
    
    logger.info(f"Found {len(valid_rows)} valid rows for {sheet_type} API requests")
    
    # Get unique combinations of Date, Stock, and instrument_key
    unique_combinations = valid_rows.groupby(['Date', 'Stock', 'instrument_key']).first().reset_index()
    
    logger.info(f"Found {len(unique_combinations)} unique stock-date combinations for {sheet_type}")
    
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
                logger.warning(f"Could not parse date: {row['Date']} for stock {row['Stock']} in {sheet_type}")
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
        api_url = construct_api_url(row['instrument_key'], date_str, INTERVAL)
        
        requests.append({
            "url": api_url,
            "stock_info": stock_info
        })
        
        logger.debug(f"Prepared {sheet_type} request for {row['Stock']} on {date_str}: {api_url}")
    
    logger.info(f"Prepared {len(requests)} API requests for {sheet_type}")
    return requests


async def fetch_all_ohlc_data(requests: List[Dict], sheet_type: str) -> List[pd.DataFrame]:
    """Fetch OHLC data for all requests concurrently."""
    logger.info(f"Starting to fetch {sheet_type} OHLC data for {len(requests)} requests...")
    
    if not requests:
        logger.warning(f"No requests to process for {sheet_type}")
        return []
    
    dataframes = []
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for all API calls
        tasks = [
            fetch_ohlc_data_with_retries(session, req["url"], req["stock_info"])
            for req in requests
        ]
        
        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        successful_dfs = 0
        failed_requests = 0
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Exception for {sheet_type} request {i}: {result}")
                failed_requests += 1
            elif result is not None:
                dataframes.append(result)
                successful_dfs += 1
            else:
                failed_requests += 1
        
        logger.info(f"{sheet_type}: Successfully fetched data for {successful_dfs} requests")
        logger.info(f"{sheet_type}: Failed requests: {failed_requests}")
    
    return dataframes


async def main():
    """Main function to fetch OHLC data for both Calls and Puts sheets."""
    try:
        logger.info("🚀 Starting OHLC data collection for both Calls and Puts sheets...")
        
        # Find the most recent Excel file with calls/puts
        import glob
        excel_files = glob.glob("option_predictions_calls_puts_*.xlsx")
        
        if not excel_files:
            logger.error("❌ No Excel file found with pattern 'option_predictions_calls_puts_*.xlsx'")
            logger.info("💡 Please run create_excel_with_instrument_keys.py first to generate the Excel file")
            return
        
        # Use the most recent file
        excel_file = max(excel_files, key=os.path.getctime)
        logger.info(f"📊 Using Excel file: {excel_file}")
        
        # Read both Calls and Puts data
        calls_df, puts_df = read_calls_puts_data(excel_file)
        
        if calls_df.empty and puts_df.empty:
            logger.error("❌ No data found in either Calls or Puts sheets")
            return
        
        # Show summary of data
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        calls_dataframes = []
        puts_dataframes = []
        
        # Process Calls data if available
        if not calls_df.empty:
            logger.info(f"\n📈 Calls sheet summary:")
            logger.info(f"  Total rows: {len(calls_df)}")
            logger.info(f"  Unique stocks: {calls_df['Stock'].nunique()}")
            logger.info(f"  Stocks with instrument keys: {calls_df['instrument_key'].notna().sum()}")
            logger.info(f"  Date range: {calls_df['Date'].min()} to {calls_df['Date'].max()}")
            
            # Prepare and fetch Calls data
            calls_requests = prepare_api_requests(calls_df, "Calls")
            if calls_requests:
                logger.info(f"\n🔄 Fetching Calls OHLC data...")
                calls_dataframes = await fetch_all_ohlc_data(calls_requests, "Calls")
        
        # Process Puts data if available
        if not puts_df.empty:
            logger.info(f"\n📈 Puts sheet summary:")
            logger.info(f"  Total rows: {len(puts_df)}")
            logger.info(f"  Unique stocks: {puts_df['Stock'].nunique()}")
            logger.info(f"  Stocks with instrument keys: {puts_df['instrument_key'].notna().sum()}")
            logger.info(f"  Date range: {puts_df['Date'].min()} to {puts_df['Date'].max()}")
            
            # Prepare and fetch Puts data
            puts_requests = prepare_api_requests(puts_df, "Puts")
            if puts_requests:
                logger.info(f"\n🔄 Fetching Puts OHLC data...")
                puts_dataframes = await fetch_all_ohlc_data(puts_requests, "Puts")
        
        # Save Calls data
        if calls_dataframes:
            calls_combined_df = pd.concat(calls_dataframes, ignore_index=True)
            calls_output_file = f"calls_ohlc_data_{timestamp}.csv"
            calls_combined_df.to_csv(calls_output_file, index=False)
            logger.info(f"💾 Saved Calls data to: {calls_output_file}")
            logger.info(f"✅ Calls: {len(calls_combined_df)} total candle records from {len(calls_dataframes)} DataFrames")
        else:
            logger.warning("❌ No Calls OHLC data was successfully fetched!")
        
        # Save Puts data
        if puts_dataframes:
            puts_combined_df = pd.concat(puts_dataframes, ignore_index=True)
            puts_output_file = f"puts_ohlc_data_{timestamp}.csv"
            puts_combined_df.to_csv(puts_output_file, index=False)
            logger.info(f"💾 Saved Puts data to: {puts_output_file}")
            logger.info(f"✅ Puts: {len(puts_combined_df)} total candle records from {len(puts_dataframes)} DataFrames")
        else:
            logger.warning("❌ No Puts OHLC data was successfully fetched!")
        
        # Display combined summary statistics
        logger.info("\n" + "="*80)
        logger.info("📈 COMBINED OHLC DATA SUMMARY")
        logger.info("="*80)
        
        if calls_dataframes:
            logger.info(f"CALLS DATA:")
            logger.info(f"  Total candle records:         {len(calls_combined_df)}")
            logger.info(f"  Unique stocks:                {calls_combined_df['stock'].nunique()}")
            logger.info(f"  Unique dates:                 {calls_combined_df['date'].nunique()}")
            logger.info(f"  Date range:                   {calls_combined_df['timestamp'].min()} to {calls_combined_df['timestamp'].max()}")
            logger.info(f"  Output file:                  {calls_output_file}")
        
        if puts_dataframes:
            logger.info(f"\nPUTS DATA:")
            logger.info(f"  Total candle records:         {len(puts_combined_df)}")
            logger.info(f"  Unique stocks:                {puts_combined_df['stock'].nunique()}")
            logger.info(f"  Unique dates:                 {puts_combined_df['date'].nunique()}")
            logger.info(f"  Date range:                   {puts_combined_df['timestamp'].min()} to {puts_combined_df['timestamp'].max()}")
            logger.info(f"  Output file:                  {puts_output_file}")
        
        # Show sample data
        if calls_dataframes:
            logger.info(f"\n📋 Sample Calls OHLC data:")
            sample_cols = ['timestamp', 'stock', 'date', 'grade', 'open', 'high', 'low', 'close', 'volume']
            available_cols = [col for col in sample_cols if col in calls_combined_df.columns]
            logger.info(f"\n{calls_combined_df[available_cols].head(5).to_string()}")
        
        if puts_dataframes:
            logger.info(f"\n📋 Sample Puts OHLC data:")
            sample_cols = ['timestamp', 'stock', 'date', 'grade', 'open', 'high', 'low', 'close', 'volume']
            available_cols = [col for col in sample_cols if col in puts_combined_df.columns]
            logger.info(f"\n{puts_combined_df[available_cols].head(5).to_string()}")
        
        logger.info(f"\n✅ Successfully completed OHLC data collection for both sheets!")
        
        # Return both lists of DataFrames
        return calls_dataframes, puts_dataframes
        
    except Exception as e:
        logger.error(f"❌ Error in main execution: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None, None


if __name__ == "__main__":
    # Run the async main function
    calls_result, puts_result = asyncio.run(main())
    
    success_message = []
    if calls_result:
        success_message.append(f"Calls: {len(calls_result)} DataFrames")
    if puts_result:
        success_message.append(f"Puts: {len(puts_result)} DataFrames")
    
    if success_message:
        print(f"\n🎉 Success! Created {' | '.join(success_message)} with OHLC data")
        print("📊 Each DataFrame contains candle data for one stock-date combination")
        print("💡 Separate CSV files created for Calls and Puts data")
    else:
        print("❌ Failed to fetch OHLC data") 