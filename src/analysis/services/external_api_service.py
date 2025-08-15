"""
External API Service
===================
Handles external API calls including Upstox API for OHLC data and NSE instrument mapping.
"""

import json
import os
import asyncio
import aiohttp
import logging
import urllib.parse
import pandas as pd
from typing import Dict, List, Optional
from sqlalchemy import create_engine, text

from ..config.settings import api_config, processing_config
from ..models.entities import OHLCData, APIRequest, Stock


class ExternalAPIService:
    """Service for handling external API operations"""
    
    def __init__(self, max_concurrent: int = None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.max_concurrent = max_concurrent or processing_config.MAX_CONCURRENT_REQUESTS
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.stock_to_instrument_key = {}
    
    def load_nse_instrument_mapping(self, db_connection_string: str) -> Dict[str, str]:
        """Get mapping of stock names to instrument keys from database and NSE data"""
        try:
            self.logger.info("📊 Connecting to database for instrument mapping...")
            engine = create_engine(db_connection_string)
            
            # Test connection
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("✅ Database connection verified for instrument mapping")
            
            # Get active stocks from database
            self.logger.info("📈 Fetching active stocks from database...")
            query = "SELECT * FROM options.stock WHERE is_active ORDER BY id ASC"
            with engine.connect() as conn:
                active_stocks = pd.read_sql(query, conn)
            
            active_stock_names = set(active_stocks['name'].tolist())
            self.logger.info(f"✅ Found {len(active_stocks)} active stocks in database")
            
            # Load NSE data
            self.logger.info(f"📋 Loading NSE data from: {api_config.NSE_DATA_PATH}")
            
            if not os.path.exists(api_config.NSE_DATA_PATH):
                self.logger.warning(f"❌ NSE file not found: {api_config.NSE_DATA_PATH}")
                return {}
            
            with open(api_config.NSE_DATA_PATH, 'r') as file:
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
            return {}
    
    def construct_api_url(self, instrument_key: str, date: str) -> str:
        """Construct the Upstox API URL for fetching historical data"""
        # URL encode the instrument key
        encoded_instrument_key = urllib.parse.quote(instrument_key, safe='')
        
        # Format: https://api.upstox.com/v3/historical-candle/{instrument_key}/minutes/{interval}/{from_date}/{to_date}
        url = f"{api_config.UPSTOX_BASE_URL}/{encoded_instrument_key}/minutes/{api_config.DEFAULT_INTERVAL}/{date}/{date}"
        
        return url
    
    async def fetch_ohlc_data_with_retries(self, session: aiohttp.ClientSession, 
                                         url: str, stock_info: Dict) -> Optional[pd.DataFrame]:
        """Fetch OHLC data from Upstox API with retry logic"""
        async with self.semaphore:
            for attempt in range(processing_config.MAX_RETRIES):
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
                    if attempt < processing_config.MAX_RETRIES - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        
            self.logger.error(f"Failed to fetch data after {processing_config.MAX_RETRIES} attempts: {url}")
            return None
    
    def process_api_response(self, api_data: Dict, stock_info: Dict) -> Optional[pd.DataFrame]:
        """Process the API response and create a DataFrame"""
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
            cols = ["timestamp", "stock", "date", "grade", "tn_ratio", "open", "high", 
                   "low", "close", "volume", "open_interest", "instrument_key", "sheet_type"]
            df = df[[col for col in cols if col in df.columns]]
            
            self.logger.info(f"Successfully processed {len(df)} candles for {stock_info['stock']} on {stock_info['date']} ({stock_info['sheet_type']})")
            return df
            
        except Exception as e:
            self.logger.error(f"Error processing API response for {stock_info['stock']}: {e}")
            return None
    
    def prepare_api_requests(self, df: pd.DataFrame, sheet_type: str) -> List[APIRequest]:
        """Prepare list of API requests from a DataFrame"""
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
            
            request = APIRequest(
                url=api_url,
                stock_info=stock_info,
                retry_count=0
            )
            
            requests.append(request)
            
            self.logger.debug(f"Prepared {sheet_type} request for {row['Stock']} on {date_str}: {api_url}")
        
        self.logger.info(f"Prepared {len(requests)} API requests for {sheet_type}")
        return requests
    
    async def fetch_all_ohlc_data(self, requests: List[APIRequest], 
                                sheet_type: str) -> List[pd.DataFrame]:
        """Fetch OHLC data for all requests concurrently"""
        self.logger.info(f"Starting to fetch {sheet_type} OHLC data for {len(requests)} requests...")
        
        if not requests:
            self.logger.warning(f"No requests to process for {sheet_type}")
            return []
        
        dataframes = []
        
        async with aiohttp.ClientSession() as session:
            # Create tasks for all API calls
            tasks = [
                self.fetch_ohlc_data_with_retries(session, req.url, req.stock_info)
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
    
    def get_instrument_key_for_stock(self, stock_name: str) -> Optional[str]:
        """Get instrument key for a specific stock"""
        return self.stock_to_instrument_key.get(stock_name)
    
    def add_instrument_keys_to_dataframe(self, df: pd.DataFrame, 
                                       stock_column: str = 'Stock') -> pd.DataFrame:
        """Add instrument keys to a DataFrame containing stock names"""
        if self.stock_to_instrument_key:
            df['instrument_key'] = df[stock_column].map(self.stock_to_instrument_key)
            stocks_with_keys = df['instrument_key'].notna().sum()
            total_stocks = len(df)
            
            self.logger.info(f"Added instrument keys to {stocks_with_keys}/{total_stocks} rows")
            
            # Log stocks without instrument keys
            stocks_without_keys = df[df['instrument_key'].isna()][stock_column].unique()
            if len(stocks_without_keys) > 0:
                self.logger.warning(f"Stocks without instrument keys: {list(stocks_without_keys)}")
        else:
            self.logger.warning("No instrument key mapping available. Adding empty instrument_key column.")
            df['instrument_key'] = None
        
        return df 