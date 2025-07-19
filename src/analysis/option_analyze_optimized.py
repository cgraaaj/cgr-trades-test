import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import requests
import pandas as pd
import numpy as np
import pprint
import urllib
from sqlalchemy import create_engine, text
from urllib.parse import quote
from uuid import UUID
import asyncio
from databases import Database
import math
import pickle
from collections import defaultdict
from decimal import Decimal
import time
from datetime import timedelta
import db_config
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
import logging
from option_ranking_optimized import OptionRankingOptimized, export_predictions_to_excel

# Configuration constants
TRADING_MINUTES_PER_DAY = 375  # 9:15 AM to 3:30 PM
DEFAULT_INTERVAL = 15  # minutes
TN_RATIO_THRESHOLD = 60  # Trend-to-none ratio threshold
DEFAULT_TRADE_DATE = "2024-07-26"
DEFAULT_EXPIRY_DATE = "2025-05-29"
MARKET_START_TIME = "09:15:00"
MARKET_END_TIME = "15:30:00"
BATCH_SIZE = 50  # Process stocks in batches
MAX_CONCURRENT_TASKS = 10  # Limit concurrent tasks

async def get_available_trading_dates():
    """
    Get all available trading dates from the database.
    Returns list of dates in YYYY-MM-DD format.
    """
    try:
        query = """
        SELECT DISTINCT DATE(time_stamp) as date
        FROM options.ticker
        ORDER BY date
        """
        
        result_df = await query_to_dataframe_optimized(query)
        
        if result_df.empty:
            logger.warning("No trading dates found in database")
            return []
        
        # Convert dates to string format
        trading_dates = [date.strftime("%Y-%m-%d") for date in result_df['date']]
        
        logger.info(f"Found {len(trading_dates)} trading dates in database")
        logger.info(f"Date range: {trading_dates[0]} to {trading_dates[-1]}")
        
        return trading_dates
        
    except Exception as e:
        logger.error(f"Error fetching trading dates from database: {e}")
        return []

async def get_available_expiry_dates():
    """
    Get all available expiry dates from the database.
    Returns list of dates in YYYY-MM-DD format.
    """
    try:
        query = """
        SELECT DISTINCT expiry
        FROM options.instrument
        WHERE instrument_type != 'FUT'
        ORDER BY expiry
        """
        
        result_df = await query_to_dataframe_optimized(query)
        
        if result_df.empty:
            logger.warning("No expiry dates found in database")
            return []
        
        # Convert dates to string format
        expiry_dates = [date.strftime("%Y-%m-%d") for date in result_df['expiry']]
        
        logger.info(f"Found {len(expiry_dates)} expiry dates in database")
        logger.info(f"Expiry range: {expiry_dates[0]} to {expiry_dates[-1]}")
        
        return expiry_dates
        
    except Exception as e:
        logger.error(f"Error fetching expiry dates from database: {e}")
        return []

def get_expiry_for_trade_date(trade_date: str, expiry_dates: list) -> str:
    """
    Get the appropriate expiry date for a given trade date.
    
    Args:
        trade_date: Trade date in YYYY-MM-DD format
        expiry_dates: List of available expiry dates
    
    Returns:
        Appropriate expiry date in YYYY-MM-DD format
    """
    try:
        from datetime import datetime
        
        trade_dt = datetime.strptime(trade_date, "%Y-%m-%d")
        trade_year_month = trade_dt.strftime("%Y-%m")
        
        # First, try to find expiry in the same month
        for expiry in expiry_dates:
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            expiry_year_month = expiry_dt.strftime("%Y-%m")
            
            if expiry_year_month == trade_year_month and expiry_dt >= trade_dt:
                return expiry
        
        # If no expiry found in same month, get the next available expiry
        for expiry in expiry_dates:
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            if expiry_dt >= trade_dt:
                return expiry
        
        # If no future expiry found, use the last available expiry
        if expiry_dates:
            logger.warning(f"No suitable expiry found for trade date {trade_date}, using last available: {expiry_dates[-1]}")
            return expiry_dates[-1]
        
        # Fallback to default
        logger.error(f"No expiry dates available for trade date {trade_date}")
        return DEFAULT_EXPIRY_DATE
        
    except Exception as e:
        logger.error(f"Error getting expiry for trade date {trade_date}: {e}")
        return DEFAULT_EXPIRY_DATE

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

pd.set_option('future.no_silent_downcasting', True)

# Optimized database connection with connection pooling
DATABASE_URL = db_config.DATABASE_URL
engine = create_engine(
    DATABASE_URL,
    pool_size=20,  # Increased pool size
    max_overflow=30,  # Allow more connections
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=3600,  # Recycle connections every hour
    echo=False  # Disable SQL logging for performance
)
database = Database(DATABASE_URL)

# Market action constants
bullish = ["Short Cover", "Long Buildup"]
bearish = ["Long Unwind", "Short Buildup"]

# Cache for frequently accessed data
@lru_cache(maxsize=1000)
def get_trading_timestamps(trade_date: str):
    """Cache trading timestamps for reuse"""
    start_time = pd.Timestamp(f"{trade_date} {MARKET_START_TIME}")
    end_time = pd.Timestamp(f"{trade_date} 15:29:00")
    return pd.date_range(start=start_time, end=end_time, freq="1min")

# Optimized query function with prepared statements
async def query_to_dataframe_optimized(query: str, parameters: dict = None):
    """Optimized query execution with prepared statements"""
    try:
        if parameters:
            results = await database.fetch_all(query=text(query), values=parameters)
        else:
            results = await database.fetch_all(query=query)
        
        if results:
            # Use record_to_dict for better performance
            columns = list(results[0].keys())
            data = [dict(result) for result in results]
            df = pd.DataFrame(data, columns=columns)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"Database query error: {e}")
        logger.error(f"Query was: {query[:100]}...")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()

# Bulk query for multiple stocks
async def get_all_instruments_bulk(stock_ids: list, expiry_date: str):
    """Fetch instruments for multiple stocks in one query"""
    placeholders = ",".join([f"uuid('{stock_id}')" for stock_id in stock_ids])
    query = f"""
    SELECT id, stock_id, segment, name, exchange, expiry, expiry_epoch, 
           instrument_type, asset_symbol, underlying_symbol, instrument_key, 
           lot_size, freeze_quantity, exchange_token, minimum_lot, asset_key, 
           underlying_key, tick_size, asset_type, underlying_type, 
           trading_symbol, strike_price, weekly 
    FROM options.instrument 
    WHERE stock_id IN ({placeholders})
    AND expiry = '{expiry_date}'
    AND instrument_type != 'FUT'
    ORDER BY stock_id, strike_price
    """
    return await query_to_dataframe_optimized(query)

# Bulk query for ticker data
async def get_ticker_data_bulk(instrument_ids: list, trade_date: str):
    """Fetch ticker data for multiple instruments in one query"""
    if not instrument_ids:
        return pd.DataFrame()
    
    placeholders = ",".join([f"uuid('{inst_id}')" for inst_id in instrument_ids])
    query = f"""
    SELECT * FROM options.ticker 
    WHERE instrument_id IN ({placeholders})
    AND time_stamp >= '{trade_date} {MARKET_START_TIME}'
    AND time_stamp <= '{trade_date} {MARKET_END_TIME}'
    ORDER BY instrument_id, time_stamp
    """
    return await query_to_dataframe_optimized(query)

# Vectorized OI action calculation
def calculate_oi_actions_vectorized(df, option_type_suffix):
    """Vectorized calculation of OI actions"""
    price_col = f"ltp_change_{option_type_suffix}"
    oi_col = f"open_interest_change_{option_type_suffix}"
    
    # Check if required columns exist
    if price_col not in df.columns or oi_col not in df.columns:
        logger.warning(f"Missing columns for OI action calculation: {price_col}, {oi_col}")
        logger.warning(f"Available columns: {df.columns.tolist()}")
        return pd.Series([""] * len(df), index=df.index)
    
    conditions = [
        (df[price_col] > 0) & (df[oi_col] > 0),
        (df[price_col] > 0) & (df[oi_col] < 0),
        (df[price_col] < 0) & (df[oi_col] > 0),
        (df[price_col] < 0) & (df[oi_col] < 0)
    ]
    
    choices = ["Long Buildup", "Short Cover", "Short Buildup", "Long Unwind"]
    
    return np.select(conditions, choices, default="")

# Optimized trend analysis with vectorized operations
def analyze_trend_vectorized(ticker_cepe_df):
    """Vectorized trend analysis for better performance"""
    try:
        # Check if DataFrame is empty
        if ticker_cepe_df.empty:
            logger.warning("Empty DataFrame passed to analyze_trend_vectorized")
            return ticker_cepe_df
        
        # Calculate OI actions using vectorized operations
        ticker_cepe_df["oi_action_x"] = calculate_oi_actions_vectorized(ticker_cepe_df, "x")
        ticker_cepe_df["oi_action_y"] = calculate_oi_actions_vectorized(ticker_cepe_df, "y")
        
        # Vectorized trend calculation
        ticker_cepe_df["trend_x"] = np.where(
            ticker_cepe_df["oi_action_x"].isin(bullish), "Bullish",
            np.where(ticker_cepe_df["oi_action_x"].isin(bearish), "Bearish", None)
        )
        
        ticker_cepe_df["trend_y"] = np.where(
            ticker_cepe_df["oi_action_y"].isin(bullish), "Bullish",
            np.where(ticker_cepe_df["oi_action_y"].isin(bearish), "Bearish", None)
        )
        
        return ticker_cepe_df
        
    except Exception as e:
        logger.error(f"Error in analyze_trend_vectorized: {e}")
        logger.error(f"DataFrame columns: {ticker_cepe_df.columns.tolist()}")
        logger.error(f"DataFrame shape: {ticker_cepe_df.shape}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return ticker_cepe_df

# Optimized DataFrame normalization
def normalize_df_optimized(df, trade_date):
    """Optimized DataFrame normalization using cached timestamps"""
    full_range = get_trading_timestamps(trade_date)
    full_range_df = pd.DataFrame(full_range, columns=["time_stamp"])
    
    # Use merge with optimized join
    merged_df = pd.merge(full_range_df, df, on="time_stamp", how="left")
    
    # Optimized fillna operations
    numeric_cols = ["open_interest", "open_interest_change", "volume", "ltp", "ltp_change"]
    for col in numeric_cols:
        if col in merged_df.columns:
            merged_df[col] = merged_df[col].fillna(0).astype('float32')  # Use float32 for memory efficiency
    
    return merged_df

# Optimized candlestick interval conversion
def convert_candlestick_interval_optimized(df, new_interval="15min"):
    """Optimized candlestick interval conversion"""
    if df.empty:
        return df
    
    try:
        # Convert to datetime once
        df["time_stamp"] = pd.to_datetime(df["time_stamp"])
        df.set_index("time_stamp", inplace=True)
        
        # Define all possible aggregation rules
        all_agg_rules = {
            "open_interest_x": "last",
            "open_interest_change_x": "sum",
            "volume_x": "sum",
            "ltp_x": "last",
            "ltp_change_x": "sum",
            "open_interest_y": "last",
            "open_interest_change_y": "sum",
            "volume_y": "sum",
            "ltp_y": "last",
            "ltp_change_y": "sum",
            "strike_price": "first",  # Add strike_price if it exists
        }
        
        # Filter to only include columns that actually exist in the DataFrame
        agg_rules = {k: v for k, v in all_agg_rules.items() if k in df.columns}
        
        if not agg_rules:
            logger.warning("No valid columns found for aggregation")
            logger.warning(f"DataFrame columns: {df.columns.tolist()}")
            return pd.DataFrame()
        
        # Resample with optimized operations
        resampled_df = df.resample(new_interval).agg(agg_rules).fillna(0.0)
        resampled_df.reset_index(inplace=True)
        
        return resampled_df
        
    except Exception as e:
        logger.error(f"Error in convert_candlestick_interval_optimized: {e}")
        logger.error(f"DataFrame columns: {df.columns.tolist()}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()

# Optimized batch processing
async def process_stocks_batch(stock_batch, trade_date, expiry_date):
    """Process a batch of stocks concurrently"""
    try:
        # Convert to list if needed and get length safely
        if hasattr(stock_batch, '__len__'):
            batch_size = len(stock_batch)
        else:
            stock_batch = list(stock_batch)
            batch_size = len(stock_batch)
        
        logger.info(f"Processing batch of {batch_size} stocks")
        
        if not stock_batch:
            logger.warning("Empty stock batch received")
            return []
        
        # Get all instruments for this batch
        stock_ids = [stock.id for stock in stock_batch]
        logger.info(f"Fetching instruments for {len(stock_ids)} stocks")
        
        instrument_df = await get_all_instruments_bulk(stock_ids, expiry_date)
        
        if instrument_df.empty:
            logger.warning("No instruments found for batch")
            return []
        
        # Get all ticker data for this batch
        instrument_ids = instrument_df['id'].tolist()
        ticker_df = await get_ticker_data_bulk(instrument_ids, trade_date)
        
        if ticker_df.empty:
            logger.warning("No ticker data found for batch")
            return []
        
        # Process each stock in the batch
        tasks = []
        for stock in stock_batch:
            task = process_single_stock_optimized(stock, instrument_df, ticker_df, trade_date)
            tasks.append(task)
        
        # Execute with limited concurrency
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        
        async def limited_task(task):
            async with semaphore:
                return await task
        
        limited_tasks = [limited_task(task) for task in tasks]
        return await asyncio.gather(*limited_tasks, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"Error in process_stocks_batch: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return []

async def process_single_stock_optimized(stock, instrument_df, ticker_df, trade_date):
    """Process a single stock with optimized operations"""
    try:
        # Filter instruments for this stock
        stock_instruments = instrument_df[instrument_df['stock_id'] == stock.id]
        
        if stock_instruments.empty:
            return None
        
        # Filter ticker data for this stock's instruments
        stock_instrument_ids = stock_instruments['id'].tolist()
        stock_ticker_df = ticker_df[ticker_df['instrument_id'].isin(stock_instrument_ids)]
        
        if stock_ticker_df.empty:
            return None
        
        # Create CE/PE pairs more efficiently
        ce_instruments = stock_instruments[stock_instruments['instrument_type'] == 'CE'][['id', 'strike_price']]
        pe_instruments = stock_instruments[stock_instruments['instrument_type'] == 'PE'][['id', 'strike_price']]
        
        # Merge CE and PE data
        ce_pe_pairs = pd.merge(ce_instruments, pe_instruments, on='strike_price', how='outer', suffixes=('_ce', '_pe'))
        ce_pe_pairs.columns = ['ce_id', 'strike_price', 'pe_id']
        ce_pe_pairs = ce_pe_pairs.sort_values('strike_price')
        
        # Process all pairs efficiently
        processed_data = []
        for _, pair in ce_pe_pairs.iterrows():
            pair_data = process_ce_pe_pair_optimized(stock_ticker_df, pair, trade_date)
            if not pair_data.empty:
                processed_data.append(pair_data)
        
        if not processed_data:
            return None
        
        # Concatenate all data
        combined_df = pd.concat(processed_data, ignore_index=True)
        
        # Optimized simulation
        simulation_dfs = get_min_simulation_optimized(combined_df, DEFAULT_INTERVAL)
        
        # Process simulation results
        simulated_data = []
        for sim_df in simulation_dfs:
            if not sim_df.empty:
                analysis = trend_n_grade_analysis_optimized(sim_df)
                simulated_data.append(analysis)
        
        return {
            "name": stock.name,
            "opt_data": simulated_data
        }
    
    except Exception as e:
        logger.error(f"Error processing stock {stock.name}: {e}")
        return None

def process_ce_pe_pair_optimized(ticker_df, pair, trade_date):
    """Optimized CE/PE pair processing"""
    try:
        columns_cepe = ["time_stamp", "open_interest", "open_interest_change", "volume", "ltp", "ltp_change"]
        
        # Process CE data
        if pd.isna(pair.ce_id):
            ce_df = pd.DataFrame(columns=columns_cepe)
        else:
            ce_df = ticker_df[ticker_df['instrument_id'] == pair.ce_id].copy()
            if not ce_df.empty:
                ce_df['ltp'] = ce_df['open']
                ce_df['ltp_change'] = ce_df['ltp'].diff()
                ce_df['open_interest_change'] = ce_df['open_interest'].diff()
                ce_df = ce_df[columns_cepe]
            else:
                ce_df = pd.DataFrame(columns=columns_cepe)
        
        # Process PE data
        if pd.isna(pair.pe_id):
            pe_df = pd.DataFrame(columns=columns_cepe)
        else:
            pe_df = ticker_df[ticker_df['instrument_id'] == pair.pe_id].copy()
            if not pe_df.empty:
                pe_df['ltp'] = pe_df['open']
                pe_df['ltp_change'] = pe_df['ltp'].diff()
                pe_df['open_interest_change'] = pe_df['open_interest'].diff()
                pe_df = pe_df[columns_cepe]
            else:
                pe_df = pd.DataFrame(columns=columns_cepe)
        
        # Normalize both DataFrames - this ensures they have the same timestamps
        ce_df = normalize_df_optimized(ce_df, trade_date)
        pe_df = normalize_df_optimized(pe_df, trade_date)
        
        # Check if both DataFrames are empty
        if ce_df.empty and pe_df.empty:
            return pd.DataFrame()
        
        # Ensure both DataFrames have the same structure before merging
        if ce_df.empty:
            # Create empty CE dataframe with same index as PE
            ce_df = pd.DataFrame(index=pe_df.index, columns=columns_cepe)
            ce_df['time_stamp'] = pe_df['time_stamp']
            ce_df = ce_df.fillna(0.0)
        
        if pe_df.empty:
            # Create empty PE dataframe with same index as CE  
            pe_df = pd.DataFrame(index=ce_df.index, columns=columns_cepe)
            pe_df['time_stamp'] = ce_df['time_stamp']
            pe_df = pe_df.fillna(0.0)
        
        # Merge CE and PE data with explicit suffixes
        merged_df = pd.merge(ce_df, pe_df, on="time_stamp", how="outer", suffixes=('_x', '_y'))
        merged_df = merged_df.fillna(0.0)
        
        # Verify we have the expected columns before proceeding
        expected_cols = ['ltp_change_x', 'ltp_change_y', 'open_interest_change_x', 'open_interest_change_y']
        missing_cols = [col for col in expected_cols if col not in merged_df.columns]
        
        if missing_cols:
            logger.warning(f"Missing expected columns after merge: {missing_cols}")
            # Create missing columns with zeros
            for col in missing_cols:
                merged_df[col] = 0.0
        
        # Convert to optimized interval
        merged_df = convert_candlestick_interval_optimized(merged_df, "15min")
        
        # Skip trend analysis if dataframe is empty after interval conversion
        if merged_df.empty:
            return pd.DataFrame()
        
        # Apply trend analysis
        merged_df = analyze_trend_vectorized(merged_df)
        
        # Add strike price
        merged_df['strike_price'] = pair.strike_price
        
        return merged_df
    
    except Exception as e:
        logger.error(f"Error processing CE/PE pair: {e}")
        logger.error(f"Pair details - CE: {pair.ce_id}, PE: {pair.pe_id}, Strike: {pair.strike_price}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame()

def get_min_simulation_optimized(df, interval=15):
    """Optimized simulation with better memory management"""
    if df.empty:
        return []
    
    try:
        records_per_day = TRADING_MINUTES_PER_DAY // interval
        total_records = len(df)
        
        if total_records == 0:
            return []
        
        total_days = total_records // records_per_day
        
        # Use list comprehension for better performance
        dfs = [
            df.iloc[i::records_per_day].copy()
            for i in range(min(records_per_day, total_records))
        ]
        
        return [sim_df for sim_df in dfs if not sim_df.empty]
    
    except Exception as e:
        logger.error(f"Error in simulation: {e}")
        return []

def trend_n_grade_analysis_optimized(df):
    """Optimized trend and grade analysis"""
    if df.empty:
        return {}
    
    try:
        no_of_strike_price = len(df)
        
        # Vectorized calculations
        calls_bullish = (df["trend_x"] == "Bullish").sum()
        calls_bearish = (df["trend_x"] == "Bearish").sum()
        puts_bullish = (df["trend_y"] == "Bullish").sum()
        puts_bearish = (df["trend_y"] == "Bearish").sum()
        
        # Calculate percentages and grades
        calls_percentage = calculate_percentage(calls_bullish, calls_bearish)
        puts_percentage = calculate_percentage(puts_bullish, puts_bearish)
        
        # Calculate TN ratios
        calls_tn_ratio = math.ceil(((calls_bullish + calls_bearish) / no_of_strike_price) * 100)
        puts_tn_ratio = math.ceil(((puts_bullish + puts_bearish) / no_of_strike_price) * 100)
        
        return {
            "time_stamp": df.iloc[0]["time_stamp"],
            "options": {
                "calls": {
                    "bullish": calls_bullish,
                    "bearish": calls_bearish,
                    "percentage": calls_percentage,
                    "grade": get_grade_from_percentage(calls_percentage),
                    "tn_ratio": calls_tn_ratio
                },
                "puts": {
                    "bullish": puts_bullish,
                    "bearish": puts_bearish,
                    "percentage": puts_percentage,
                    "grade": get_grade_from_percentage(puts_percentage),
                    "tn_ratio": puts_tn_ratio
                }
            },
            "callTrend": calls_bullish > calls_bearish,
            "putTrend": puts_bullish > puts_bearish
        }
    
    except Exception as e:
        logger.error(f"Error in trend analysis: {e}")
        return {}

def calculate_percentage(bullish_count, bearish_count):
    """Calculate trend percentage"""
    if bullish_count == 0:
        return 0
    return math.ceil(((bullish_count - bearish_count) / bullish_count) * 100)

def get_grade_from_percentage(percentage):
    """Convert percentage to grade"""
    if percentage == 100:
        return "A"
    elif 85 < percentage < 100:
        return "B"
    elif 50 < percentage <= 85:
        return "C"
    else:
        return "D"

# Option ranking functionality moved to separate module option_ranking_optimized.py

# Main optimized function
async def main_optimized():
    """Main function with optimized performance"""
    start_time = time.time()
    
    try:
        logger.info("Starting optimized analysis...")
        
        # Connect to database
        logger.info("Connecting to database...")
        await database.connect()
        logger.info("Database connected successfully")
        
        # Get all active stocks - fix the query
        logger.info("Fetching active stocks...")
        try:
            # Try both possible boolean formats
            stock_df = await query_to_dataframe_optimized("SELECT * FROM options.stock WHERE is_active = true")
            if stock_df.empty:
                logger.info("No stocks found with is_active = true, trying is_active = 1...")
                stock_df = await query_to_dataframe_optimized("SELECT * FROM options.stock WHERE is_active = 1")
            if stock_df.empty:
                logger.info("No stocks found with is_active = 1, trying without filter...")
                stock_df = await query_to_dataframe_optimized("SELECT * FROM options.stock LIMIT 5")
        except Exception as db_error:
            logger.error(f"Database query error: {db_error}")
            logger.error(f"Error type: {type(db_error)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return
        
        if stock_df.empty:
            logger.error("No stocks found in database")
            return
        
        logger.info(f"Found {len(stock_df)} stocks")
        
        # Get all available trading dates from database
        logger.info("Fetching available trading dates from database...")
        # trading_dates = await get_available_trading_dates()
        trading_dates = ["2025-05-08"]
        
        if not trading_dates:
            logger.error("No trading dates found in database. Exiting.")
            return
        
        # Get all available expiry dates from database
        logger.info("Fetching available expiry dates from database...")
        expiry_dates = await get_available_expiry_dates()
        
        if not expiry_dates:
            logger.error("No expiry dates found in database. Exiting.")
            return
        
        # Process stocks in batches for multiple dates
        all_results = []
        total_batches = (len(stock_df) - 1) // BATCH_SIZE + 1
        total_dates = len(trading_dates)
        
        logger.info(f"Processing {len(stock_df)} stocks across {total_dates} trading dates")
        
        for date_idx, trade_date in enumerate(trading_dates):
            # Get appropriate expiry date for this trade date
            expiry_date = get_expiry_for_trade_date(trade_date, expiry_dates)
            
            logger.info(f"Processing date {date_idx + 1}/{total_dates}: {trade_date} (expiry: {expiry_date})")
            date_results = []
            
            for i in range(0, len(stock_df), BATCH_SIZE):  # Process all stocks
                batch = stock_df.iloc[i:i+BATCH_SIZE]
                current_batch = i // BATCH_SIZE + 1
                
                logger.info(f"Date {trade_date} - Processing batch {current_batch}/{total_batches} ({len(batch)} stocks)")
                
                try:
                    batch_results = await process_stocks_batch(
                        list(batch.itertuples()), 
                        trade_date, 
                        expiry_date
                    )
                    
                    # Filter out None results and exceptions
                    valid_results = []
                    for result in batch_results:
                        if result is None:
                            logger.warning("Got None result from batch processing")
                        elif isinstance(result, Exception):
                            logger.error(f"Got exception from batch processing: {result}")
                        else:
                            valid_results.append(result)
                    
                    date_results.extend(valid_results)
                    logger.info(f"Date {trade_date} - Batch {current_batch} processed: {len(valid_results)} valid results")
                    
                except Exception as batch_error:
                    logger.error(f"Error processing batch {current_batch} for date {trade_date}: {batch_error}")
                    logger.error(f"Error type: {type(batch_error)}")
                    import traceback
                    logger.error(f"Traceback: {traceback.format_exc()}")
                    continue
            
            logger.info(f"Date {trade_date} completed: {len(date_results)} total results")
            all_results.extend(date_results)
        
        logger.info(f"Successfully processed {len(all_results)} stocks total")
        
        # Save results
        try:
            # Generate filename based on date range
            start_date = trading_dates[0].replace("-", "")
            end_date = trading_dates[-1].replace("-", "")
            filename = f'analyzed_stocks_data_optimized_{start_date}_to_{end_date}_close.pickle'
            
            with open(filename, 'wb') as handle:
                pickle.dump(all_results, handle, protocol=pickle.HIGHEST_PROTOCOL)
            logger.info(f"Results saved to {filename}")
        except Exception as save_error:
            logger.error(f"Error saving results: {save_error}")
        
        # Generate predictions using external OptionRankingOptimized module
        try:
            # Initialize the option ranking optimizer
            ranker = OptionRankingOptimized()
            prediction = ranker.rank_options(all_results)
            
            # Generate filename based on date range
            start_date = trading_dates[0].replace("-", "")
            end_date = trading_dates[-1].replace("-", "")
            pred_filename = f'prediction_optimized_{start_date}_to_{end_date}_close.pickle'
            
            with open(pred_filename, 'wb') as handle:
                pickle.dump(prediction, handle, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Calculate prediction counts for logging
            call_count = sum(len(pred.get("stock_data", [])) for pred in prediction.get("call", []))
            put_count = sum(len(pred.get("stock_data", [])) for pred in prediction.get("put", []))
            
            logger.info(f"Predictions generated: {call_count} calls, {put_count} puts")
            logger.info(f"Predictions saved to {pred_filename}")
            
            # Export to Excel
            try:
                excel_filename = f"option_predictions_optimized_{start_date}_to_{end_date}_close.xlsx"
                if export_predictions_to_excel(prediction, excel_filename):
                    logger.info(f"Excel export successful: {excel_filename}")
                else:
                    logger.warning(f"Excel export failed for {excel_filename}")
            except Exception as excel_error:
                logger.error(f"Error exporting to Excel: {excel_error}")
            
        except Exception as prediction_error:
            logger.error(f"Error generating predictions: {prediction_error}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        
    finally:
        try:
            await database.disconnect()
            logger.info("Database disconnected")
        except Exception as disconnect_error:
            logger.error(f"Error disconnecting from database: {disconnect_error}")
        
        execution_time = (time.time() - start_time) / 60
        logger.info(f"Total execution time: {execution_time:.2f} minutes")

if __name__ == "__main__":
    asyncio.run(main_optimized()) 