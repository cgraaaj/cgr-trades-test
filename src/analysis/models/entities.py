"""
Data Models and Entities for Options Analysis
=============================================
Contains all data classes, DTOs, and entity models used throughout the system.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from ..config.settings import Grade


@dataclass
class Stock:
    """Stock entity model"""
    id: str
    name: str
    symbol: str
    is_active: bool = True
    sector: Optional[str] = None
    lot_size: Optional[int] = None


@dataclass
class Instrument:
    """Option instrument model"""
    id: str
    stock_id: str
    segment: str
    name: str
    exchange: str
    expiry: str
    expiry_epoch: int
    instrument_type: str  # 'CE', 'PE', 'FUT'
    asset_symbol: str
    underlying_symbol: str
    instrument_key: str
    lot_size: int
    freeze_quantity: int
    exchange_token: str
    minimum_lot: int
    asset_key: str
    underlying_key: str
    tick_size: float
    asset_type: str
    underlying_type: str
    trading_symbol: str
    strike_price: float
    weekly: bool


@dataclass
class TickerData:
    """Ticker/OHLC data model"""
    instrument_id: str
    time_stamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: int
    ltp: Optional[float] = None
    ltp_change: Optional[float] = None
    open_interest_change: Optional[int] = None


@dataclass
class OptionAnalysis:
    """Option analysis result model"""
    bullish: int
    bearish: int
    percentage: float
    grade: str
    tn_ratio: int


@dataclass
class TimeAnalysisData:
    """Time-based analysis data model"""
    time_stamp: datetime
    calls: OptionAnalysis
    puts: OptionAnalysis
    call_trend: bool
    put_trend: bool


@dataclass
class StockPrediction:
    """Stock prediction result model"""
    stock: str
    timestamp: datetime
    grade: str
    option_type: str  # 'call' or 'put'
    tn_ratio: int
    bullish_count: int
    bearish_count: int


@dataclass
class PredictionResult:
    """Complete prediction result structure"""
    call: List[Dict[str, Any]] = field(default_factory=list)
    put: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class OHLCData:
    """OHLC candle data from external API"""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    open_interest: Optional[int] = None
    stock: Optional[str] = None
    date: Optional[str] = None
    instrument_key: Optional[str] = None
    grade: Optional[str] = None
    tn_ratio: Optional[int] = None
    sheet_type: Optional[str] = None


@dataclass
class CEPEPair:
    """Call-Put option pair model"""
    ce_id: Optional[str]
    pe_id: Optional[str]
    strike_price: float


@dataclass
class TrendAnalysisResult:
    """Trend analysis result model"""
    oi_action_x: str  # Call option action
    oi_action_y: str  # Put option action
    trend_x: Optional[str]  # Call trend
    trend_y: Optional[str]  # Put trend


@dataclass
class ProcessingBatch:
    """Batch processing configuration"""
    stocks: List[Stock]
    trade_date: str
    expiry_date: str
    batch_id: int


@dataclass
class APIRequest:
    """API request configuration"""
    url: str
    stock_info: Dict[str, Any]
    retry_count: int = 0


@dataclass
class ExportConfig:
    """Export configuration for results"""
    filename: str
    format: str  # 'excel', 'csv', 'pickle'
    include_summary: bool = True
    timestamp: Optional[datetime] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class PerformanceMetrics:
    """Performance tracking metrics"""
    total_stocks_processed: int
    successful_predictions: int
    failed_predictions: int
    processing_time_seconds: float
    grade_distribution: Dict[str, int] = field(default_factory=dict)
    api_success_rate: Optional[float] = None


# Result DTOs for API responses
@dataclass
class AnalysisResultDTO:
    """DTO for analysis results"""
    success: bool
    data: Optional[Any] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ExportResultDTO:
    """DTO for export results"""
    success: bool
    files_created: List[str] = field(default_factory=list)
    error_message: Optional[str] = None 