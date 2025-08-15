"""
Options Analysis Configuration Settings
======================================
Centralized configuration for the options analysis system.
"""

import os
from dataclasses import dataclass
from enum import Enum
from typing import List


class Grade(Enum):
    """Option grading enumeration"""
    A = "A"
    B = "B" 
    C = "C"
    D = "D"


@dataclass
class TradingConfig:
    """Trading session configuration"""
    TRADING_MINUTES_PER_DAY: int = 375  # 9:15 AM to 3:30 PM
    DEFAULT_INTERVAL: int = 15  # minutes
    MARKET_START_TIME: str = "09:15:00"
    MARKET_END_TIME: str = "15:30:00"
    TRADING_START_TIME: str = "09:30:00"
    TRADING_END_TIME: str = "14:00:00"


@dataclass
class AnalysisConfig:
    """Analysis parameters configuration"""
    TN_RATIO_THRESHOLD: int = 60  # Trend-to-none ratio threshold
    MAX_TIME_INTERVALS: int = 25
    ACCEPTED_GRADES: List[str] = None
    CONSECUTIVE_WINDOW_MINUTES: int = 15
    
    def __post_init__(self):
        if self.ACCEPTED_GRADES is None:
            self.ACCEPTED_GRADES = ["A", "B", "C", "D"]


@dataclass
class ProcessingConfig:
    """Processing and performance configuration"""
    BATCH_SIZE: int = 50  # Process stocks in batches
    MAX_CONCURRENT_TASKS: int = 10  # Limit concurrent tasks
    MAX_CONCURRENT_REQUESTS: int = 5  # API concurrency limit
    MAX_RETRIES: int = 3


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    POOL_SIZE: int = 20
    MAX_OVERFLOW: int = 30
    POOL_RECYCLE: int = 3600  # 1 hour
    ECHO: bool = False


@dataclass
class APIConfig:
    """External API configuration"""
    UPSTOX_BASE_URL: str = "https://api.upstox.com/v3/historical-candle"
    DEFAULT_INTERVAL: str = "5"  # 5 minutes interval
    NSE_DATA_PATH: str = "/home/cgraaaj/Projects/cgr-trades/python/NSE.json"


@dataclass
class DefaultDates:
    """Default date configuration"""
    DEFAULT_TRADE_DATE: str = "2024-07-26"
    DEFAULT_EXPIRY_DATE: str = "2025-05-29"


# Market action constants
class MarketActions:
    """Market action classifications"""
    BULLISH = ["Short Cover", "Long Buildup"]
    BEARISH = ["Long Unwind", "Short Buildup"]


# Singleton configuration instances
trading_config = TradingConfig()
analysis_config = AnalysisConfig()
processing_config = ProcessingConfig()
database_config = DatabaseConfig()
api_config = APIConfig()
default_dates = DefaultDates()
market_actions = MarketActions()


def get_project_root() -> str:
    """Get the project root directory"""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))


def get_analysis_dir() -> str:
    """Get the analysis directory"""
    return os.path.join(get_project_root(), 'src', 'analysis') 