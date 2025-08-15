"""
Database Service
===============
Handles all database operations with connection pooling and error handling.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import pandas as pd
from sqlalchemy import create_engine, text
from databases import Database
import logging
from typing import Dict, List, Optional, Any
from functools import lru_cache

import db_config
from ..config.settings import database_config, trading_config
from ..models.entities import Stock, Instrument, TickerData


class DatabaseService:
    """Database service for handling all database operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self._engine = None
        self._database = None
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize database connections with connection pooling"""
        try:
            # Synchronous connection with SQLAlchemy
            self._engine = create_engine(
                db_config.DATABASE_URL,
                pool_size=database_config.POOL_SIZE,
                max_overflow=database_config.MAX_OVERFLOW,
                pool_pre_ping=True,
                pool_recycle=database_config.POOL_RECYCLE,
                echo=database_config.ECHO
            )
            
            # Asynchronous connection with databases
            self._database = Database(db_config.DATABASE_URL)
            
            self.logger.info("Database connections initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database connections: {e}")
            raise
    
    async def connect(self):
        """Connect to the database"""
        try:
            await self._database.connect()
            self.logger.info("Database connected successfully")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from the database"""
        try:
            await self._database.disconnect()
            self.logger.info("Database disconnected successfully")
        except Exception as e:
            self.logger.error(f"Error disconnecting from database: {e}")
    
    async def execute_query(self, query: str, parameters: Optional[Dict] = None) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        try:
            if parameters:
                results = await self._database.fetch_all(query=text(query), values=parameters)
            else:
                results = await self._database.fetch_all(query=query)
            
            if results:
                columns = list(results[0].keys())
                data = [dict(result) for result in results]
                df = pd.DataFrame(data, columns=columns)
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Database query error: {e}")
            self.logger.error(f"Query was: {query[:100]}...")
            return pd.DataFrame()
    
    async def get_active_stocks(self) -> List[Stock]:
        """Get all active stocks from the database"""
        try:
            query = """
            SELECT id, name, symbol, is_active, sector, lot_size
            FROM options.stock 
            WHERE is_active = true
            ORDER BY name
            """
            
            df = await self.execute_query(query)
            
            if df.empty:
                # Try alternative boolean format
                query = query.replace("is_active = true", "is_active = 1")
                df = await self.execute_query(query)
            
            stocks = []
            for _, row in df.iterrows():
                stock = Stock(
                    id=row['id'],
                    name=row['name'],
                    symbol=row.get('symbol', row['name']),
                    is_active=row['is_active'],
                    sector=row.get('sector'),
                    lot_size=row.get('lot_size')
                )
                stocks.append(stock)
            
            self.logger.info(f"Retrieved {len(stocks)} active stocks")
            return stocks
            
        except Exception as e:
            self.logger.error(f"Error fetching active stocks: {e}")
            return []
    
    async def get_instruments_bulk(self, stock_ids: List[str], expiry_date: str) -> pd.DataFrame:
        """Fetch instruments for multiple stocks in one query"""
        try:
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
            
            return await self.execute_query(query)
            
        except Exception as e:
            self.logger.error(f"Error fetching instruments bulk: {e}")
            return pd.DataFrame()
    
    async def get_ticker_data_bulk(self, instrument_ids: List[str], trade_date: str) -> pd.DataFrame:
        """Fetch ticker data for multiple instruments in one query"""
        try:
            if not instrument_ids:
                return pd.DataFrame()
            
            placeholders = ",".join([f"uuid('{inst_id}')" for inst_id in instrument_ids])
            query = f"""
            SELECT * FROM options.ticker
            WHERE instrument_id IN ({placeholders})
            AND time_stamp >= '{trade_date} {trading_config.MARKET_START_TIME}'
            AND time_stamp <= '{trade_date} {trading_config.MARKET_END_TIME}'
            ORDER BY instrument_id, time_stamp
            """
            
            return await self.execute_query(query)
            
        except Exception as e:
            self.logger.error(f"Error fetching ticker data bulk: {e}")
            return pd.DataFrame()
    
    async def get_available_trading_dates(self) -> List[str]:
        """Get all available trading dates from the database"""
        try:
            query = """
            SELECT DISTINCT DATE(time_stamp) as date
            FROM options.ticker
            ORDER BY date
            """
            
            df = await self.execute_query(query)
            
            if df.empty:
                self.logger.warning("No trading dates found in database")
                return []
            
            # Convert dates to string format
            trading_dates = [date.strftime("%Y-%m-%d") for date in df['date']]
            
            self.logger.info(f"Found {len(trading_dates)} trading dates in database")
            return trading_dates
            
        except Exception as e:
            self.logger.error(f"Error fetching trading dates: {e}")
            return []
    
    async def get_available_expiry_dates(self) -> List[str]:
        """Get all available expiry dates from the database"""
        try:
            query = """
            SELECT DISTINCT expiry
            FROM options.instrument
            WHERE instrument_type != 'FUT'
            ORDER BY expiry
            """
            
            df = await self.execute_query(query)
            
            if df.empty:
                self.logger.warning("No expiry dates found in database")
                return []
            
            # Convert dates to string format
            expiry_dates = [date.strftime("%Y-%m-%d") for date in df['expiry']]
            
            self.logger.info(f"Found {len(expiry_dates)} expiry dates in database")
            return expiry_dates
            
        except Exception as e:
            self.logger.error(f"Error fetching expiry dates: {e}")
            return []
    
    @lru_cache(maxsize=1000)
    def get_trading_timestamps(self, trade_date: str):
        """Cache trading timestamps for reuse"""
        start_time = pd.Timestamp(f"{trade_date} {trading_config.MARKET_START_TIME}")
        end_time = pd.Timestamp(f"{trade_date} 15:29:00")
        return pd.date_range(start=start_time, end=end_time, freq="1min")
    
    def get_synchronous_connection(self):
        """Get synchronous database connection for pandas operations"""
        return self._engine 