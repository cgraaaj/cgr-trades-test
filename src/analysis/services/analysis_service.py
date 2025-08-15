"""
Analysis Service
===============
Contains all option analysis business logic including trend analysis, grading, and prediction generation.
"""

import pandas as pd
import numpy as np
import math
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from ..config.settings import (
    analysis_config, trading_config, market_actions, 
    processing_config, Grade
)
from ..models.entities import (
    StockPrediction, OptionAnalysis, TimeAnalysisData, 
    TrendAnalysisResult, CEPEPair, PredictionResult
)


class AnalysisService:
    """Service for performing option analysis operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def calculate_oi_actions_vectorized(self, df: pd.DataFrame, option_type_suffix: str) -> pd.Series:
        """Vectorized calculation of OI (Open Interest) actions"""
        price_col = f"ltp_change_{option_type_suffix}"
        oi_col = f"open_interest_change_{option_type_suffix}"
        
        # Check if required columns exist
        if price_col not in df.columns or oi_col not in df.columns:
            self.logger.warning(f"Missing columns for OI action calculation: {price_col}, {oi_col}")
            return pd.Series([""] * len(df), index=df.index)
        
        conditions = [
            (df[price_col] > 0) & (df[oi_col] > 0),
            (df[price_col] > 0) & (df[oi_col] < 0),
            (df[price_col] < 0) & (df[oi_col] > 0),
            (df[price_col] < 0) & (df[oi_col] < 0)
        ]
        
        choices = ["Long Buildup", "Short Cover", "Short Buildup", "Long Unwind"]
        
        return np.select(conditions, choices, default="")
    
    def analyze_trend_vectorized(self, ticker_cepe_df: pd.DataFrame) -> pd.DataFrame:
        """Vectorized trend analysis for better performance"""
        try:
            # Check if DataFrame is empty
            if ticker_cepe_df.empty:
                self.logger.warning("Empty DataFrame passed to analyze_trend_vectorized")
                return ticker_cepe_df
            
            # Calculate OI actions using vectorized operations
            ticker_cepe_df["oi_action_x"] = self.calculate_oi_actions_vectorized(ticker_cepe_df, "x")
            ticker_cepe_df["oi_action_y"] = self.calculate_oi_actions_vectorized(ticker_cepe_df, "y")
            
            # Vectorized trend calculation
            ticker_cepe_df["trend_x"] = np.where(
                ticker_cepe_df["oi_action_x"].isin(market_actions.BULLISH), "Bullish",
                np.where(ticker_cepe_df["oi_action_x"].isin(market_actions.BEARISH), "Bearish", None)
            )
            
            ticker_cepe_df["trend_y"] = np.where(
                ticker_cepe_df["oi_action_y"].isin(market_actions.BULLISH), "Bullish",
                np.where(ticker_cepe_df["oi_action_y"].isin(market_actions.BEARISH), "Bearish", None)
            )
            
            return ticker_cepe_df
            
        except Exception as e:
            self.logger.error(f"Error in analyze_trend_vectorized: {e}")
            return ticker_cepe_df
    
    def normalize_df_optimized(self, df: pd.DataFrame, trade_date: str, 
                              trading_timestamps: pd.DatetimeIndex) -> pd.DataFrame:
        """Optimized DataFrame normalization using cached timestamps"""
        full_range_df = pd.DataFrame(trading_timestamps, columns=["time_stamp"])
        
        # Use merge with optimized join
        merged_df = pd.merge(full_range_df, df, on="time_stamp", how="left")
        
        # Optimized fillna operations
        numeric_cols = ["open_interest", "open_interest_change", "volume", "ltp", "ltp_change"]
        for col in numeric_cols:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].fillna(0).astype('float32')
        
        return merged_df
    
    def convert_candlestick_interval_optimized(self, df: pd.DataFrame, 
                                             new_interval: str = "15min") -> pd.DataFrame:
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
                "strike_price": "first",
            }
            
            # Filter to only include columns that actually exist in the DataFrame
            agg_rules = {k: v for k, v in all_agg_rules.items() if k in df.columns}
            
            if not agg_rules:
                self.logger.warning("No valid columns found for aggregation")
                return pd.DataFrame()
            
            # Resample with optimized operations
            resampled_df = df.resample(new_interval).agg(agg_rules).fillna(0.0)
            resampled_df.reset_index(inplace=True)
            
            return resampled_df
            
        except Exception as e:
            self.logger.error(f"Error in convert_candlestick_interval_optimized: {e}")
            return pd.DataFrame()
    
    def calculate_percentage(self, bullish_count: int, bearish_count: int) -> float:
        """Calculate trend percentage"""
        if bullish_count == 0:
            return 0
        return math.ceil(((bullish_count - bearish_count) / bullish_count) * 100)
    
    def get_grade_from_percentage(self, percentage: float) -> str:
        """Convert percentage to grade"""
        if percentage == 100:
            return Grade.A.value
        elif 85 < percentage < 100:
            return Grade.B.value
        elif 50 < percentage <= 85:
            return Grade.C.value
        else:
            return Grade.D.value
    
    def trend_n_grade_analysis_optimized(self, df: pd.DataFrame) -> Dict:
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
            calls_percentage = self.calculate_percentage(calls_bullish, calls_bearish)
            puts_percentage = self.calculate_percentage(puts_bullish, puts_bearish)
            
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
                        "grade": self.get_grade_from_percentage(calls_percentage),
                        "tn_ratio": calls_tn_ratio
                    },
                    "puts": {
                        "bullish": puts_bullish,
                        "bearish": puts_bearish,
                        "percentage": puts_percentage,
                        "grade": self.get_grade_from_percentage(puts_percentage),
                        "tn_ratio": puts_tn_ratio
                    }
                },
                "callTrend": calls_bullish > calls_bearish,
                "putTrend": puts_bullish > puts_bearish
            }
            
        except Exception as e:
            self.logger.error(f"Error in trend analysis: {e}")
            return {}
    
    def get_min_simulation_optimized(self, df: pd.DataFrame, 
                                   interval: int = 15) -> List[pd.DataFrame]:
        """Optimized simulation with better memory management"""
        if df.empty:
            return []
        
        try:
            records_per_day = trading_config.TRADING_MINUTES_PER_DAY // interval
            total_records = len(df)
            
            if total_records == 0:
                return []
            
            # Use list comprehension for better performance
            dfs = [
                df.iloc[i::records_per_day].copy()
                for i in range(min(records_per_day, total_records))
            ]
            
            return [sim_df for sim_df in dfs if not sim_df.empty]
            
        except Exception as e:
            self.logger.error(f"Error in simulation: {e}")
            return []
    
    def process_ce_pe_pair_optimized(self, ticker_df: pd.DataFrame, pair: CEPEPair, 
                                   trade_date: str, trading_timestamps: pd.DatetimeIndex) -> pd.DataFrame:
        """Optimized CE/PE pair processing"""
        try:
            columns_cepe = ["time_stamp", "open_interest", "open_interest_change", "volume", "ltp", "ltp_change"]
            
            # Process CE data
            if pd.isna(pair.ce_id):
                ce_df = pd.DataFrame(columns=columns_cepe)
            else:
                ce_df = ticker_df[ticker_df['instrument_id'] == pair.ce_id].copy()
                if not ce_df.empty:
                    ce_df['ltp'] = ce_df['close']
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
                    pe_df['ltp'] = pe_df['close']
                    pe_df['ltp_change'] = pe_df['ltp'].diff()
                    pe_df['open_interest_change'] = pe_df['open_interest'].diff()
                    pe_df = pe_df[columns_cepe]
                else:
                    pe_df = pd.DataFrame(columns=columns_cepe)
            
            # Normalize both DataFrames
            ce_df = self.normalize_df_optimized(ce_df, trade_date, trading_timestamps)
            pe_df = self.normalize_df_optimized(pe_df, trade_date, trading_timestamps)
            
            # Check if both DataFrames are empty
            if ce_df.empty and pe_df.empty:
                return pd.DataFrame()
            
            # Ensure both DataFrames have the same structure before merging
            if ce_df.empty:
                ce_df = pd.DataFrame(index=pe_df.index, columns=columns_cepe)
                ce_df['time_stamp'] = pe_df['time_stamp']
                ce_df = ce_df.fillna(0.0)
            
            if pe_df.empty:
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
                self.logger.warning(f"Missing expected columns after merge: {missing_cols}")
                for col in missing_cols:
                    merged_df[col] = 0.0
            
            # Convert to optimized interval
            merged_df = self.convert_candlestick_interval_optimized(
                merged_df, f"{trading_config.DEFAULT_INTERVAL}min"
            )
            
            if merged_df.empty:
                return pd.DataFrame()
            
            # Apply trend analysis
            merged_df = self.analyze_trend_vectorized(merged_df)
            
            # Add strike price
            merged_df['strike_price'] = pair.strike_price
            
            return merged_df
            
        except Exception as e:
            self.logger.error(f"Error processing CE/PE pair: {e}")
            return pd.DataFrame()


class OptionRankingService:
    """Service for ranking and filtering option predictions"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_data_structure(self, data: List[Dict]) -> bool:
        """Validate that the input data has the expected structure"""
        try:
            if not data or not isinstance(data, list):
                self.logger.error("Data must be a non-empty list")
                return False
            
            sample = data[0]
            required_keys = ["name", "opt_data"]
            
            for key in required_keys:
                if key not in sample:
                    self.logger.error(f"Missing required key: {key}")
                    return False
            
            if not isinstance(sample["opt_data"], list):
                self.logger.error("opt_data must be a list")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating data structure: {e}")
            return False
    
    def is_within_trading_hours(self, timestamp: datetime) -> bool:
        """Check if timestamp is within trading hours"""
        try:
            time_str = timestamp.strftime("%H:%M:%S")
            return (trading_config.TRADING_START_TIME <= time_str <= trading_config.TRADING_END_TIME)
        except Exception as e:
            self.logger.warning(f"Error checking trading hours for {timestamp}: {e}")
            return False
    
    def extract_qualified_stocks(self, data: List[Dict]) -> Tuple[List[StockPrediction], List[StockPrediction]]:
        """Extract stocks that meet the ranking criteria"""
        call_stocks = []
        put_stocks = []
        
        try:
            for stock_data in data:
                if not stock_data or "opt_data" not in stock_data or "name" not in stock_data:
                    continue
                
                stock_name = stock_data["name"]
                opt_data = stock_data["opt_data"]
                
                # Safe iteration with bounds checking
                max_intervals = min(len(opt_data), analysis_config.MAX_TIME_INTERVALS)
                
                for t in range(max_intervals):
                    time_data = opt_data[t]
                    
                    if not self._validate_time_data(time_data):
                        continue
                    
                    timestamp = time_data.get("time_stamp")
                    if not timestamp or not self.is_within_trading_hours(timestamp):
                        continue
                    
                    # Process calls
                    call_prediction = self._process_option_type(
                        time_data, stock_name, "calls", "call"
                    )
                    if call_prediction:
                        call_stocks.append(call_prediction)
                    
                    # Process puts
                    put_prediction = self._process_option_type(
                        time_data, stock_name, "puts", "put"
                    )
                    if put_prediction:
                        put_stocks.append(put_prediction)
        
        except Exception as e:
            self.logger.error(f"Error extracting qualified stocks: {e}")
        
        return call_stocks, put_stocks
    
    def _validate_time_data(self, time_data: Dict) -> bool:
        """Validate individual time data structure"""
        required_keys = ["time_stamp", "options"]
        return all(key in time_data for key in required_keys)
    
    def _process_option_type(self, time_data: Dict, stock_name: str, 
                           option_key: str, option_type: str) -> Optional[StockPrediction]:
        """Process individual option type (calls/puts)"""
        try:
            options = time_data["options"][option_key]
            
            # Check if option meets criteria
            if (options["tn_ratio"] > analysis_config.TN_RATIO_THRESHOLD and 
                options["bullish"] > options["bearish"] and
                options["grade"] in analysis_config.ACCEPTED_GRADES):
                
                return StockPrediction(
                    stock=stock_name,
                    timestamp=time_data["time_stamp"],
                    grade=options["grade"],
                    option_type=option_type,
                    tn_ratio=options["tn_ratio"],
                    bullish_count=options["bullish"],
                    bearish_count=options["bearish"]
                )
            
        except KeyError as e:
            self.logger.debug(f"Missing key in option data: {e}")
        except Exception as e:
            self.logger.warning(f"Error processing {option_type} for {stock_name}: {e}")
        
        return None
    
    def group_by_timestamp(self, predictions: List[StockPrediction]) -> Dict[datetime, List[StockPrediction]]:
        """Group predictions by timestamp"""
        grouped = defaultdict(list)
        
        for prediction in predictions:
            grouped[prediction.timestamp].append(prediction)
        
        return dict(sorted(grouped.items()))
    
    def find_consecutive_appearances(self, grouped_predictions: Dict[datetime, List[StockPrediction]]) -> Dict[str, List[StockPrediction]]:
        """Find stocks that appear in consecutive time intervals"""
        consecutive_by_date = defaultdict(list)
        
        timestamps = sorted(grouped_predictions.keys())
        prev_stocks = set()
        prev_timestamp = None
        current_date = None
        
        for timestamp in timestamps:
            # Reset on new trading day
            if current_date != timestamp.date():
                current_date = timestamp.date()
                prev_stocks = set()
                prev_timestamp = None
            
            current_stocks = {pred.stock for pred in grouped_predictions[timestamp]}
            
            # Check if current timestamp is exactly CONSECUTIVE_WINDOW_MINUTES after previous timestamp
            if prev_timestamp is not None:
                time_diff = timestamp - prev_timestamp
                is_consecutive = time_diff == timedelta(minutes=analysis_config.CONSECUTIVE_WINDOW_MINUTES)
                
                if is_consecutive:
                    # Find stocks that appeared in previous interval
                    consecutive_stocks = current_stocks.intersection(prev_stocks)
                    
                    if consecutive_stocks:
                        for prediction in grouped_predictions[timestamp]:
                            if prediction.stock in consecutive_stocks:
                                date_key = timestamp.strftime("%Y-%m-%d")
                                consecutive_by_date[date_key].append(prediction)
            
            prev_stocks = current_stocks
            prev_timestamp = timestamp
        
        return dict(consecutive_by_date)
    
    def remove_duplicate_stocks(self, predictions: List[StockPrediction]) -> List[StockPrediction]:
        """Remove duplicate stock entries, keeping the first occurrence"""
        seen_stocks = set()
        unique_predictions = []
        
        for prediction in predictions:
            if prediction.stock not in seen_stocks:
                seen_stocks.add(prediction.stock)
                unique_predictions.append(prediction)
        
        return unique_predictions
    
    def format_predictions(self, consecutive_calls: Dict, consecutive_puts: Dict) -> PredictionResult:
        """Format predictions into the expected output structure"""
        prediction_result = PredictionResult()
        
        # Format call predictions
        for date, predictions in consecutive_calls.items():
            unique_predictions = self.remove_duplicate_stocks(predictions)
            prediction_result.call.append({
                "date": date,
                "stock_data": [
                    {
                        "time_stamp": pred.timestamp,
                        "stock": pred.stock,
                        "grade": pred.grade,
                        "tn_ratio": pred.tn_ratio,
                        "bullish_count": pred.bullish_count,
                        "bearish_count": pred.bearish_count
                    }
                    for pred in unique_predictions
                ]
            })
        
        # Format put predictions
        for date, predictions in consecutive_puts.items():
            unique_predictions = self.remove_duplicate_stocks(predictions)
            prediction_result.put.append({
                "date": date,
                "stock_data": [
                    {
                        "time_stamp": pred.timestamp,
                        "stock": pred.stock,
                        "grade": pred.grade,
                        "tn_ratio": pred.tn_ratio,
                        "bullish_count": pred.bullish_count,
                        "bearish_count": pred.bearish_count
                    }
                    for pred in unique_predictions
                ]
            })
        
        return prediction_result
    
    def rank_options(self, data: List[Dict]) -> PredictionResult:
        """Main ranking function with optimized performance"""
        try:
            self.logger.info(f"Starting option ranking for {len(data)} stocks")
            
            # Validate input data
            if not self.validate_data_structure(data):
                return PredictionResult()
            
            # Extract qualified stocks
            call_stocks, put_stocks = self.extract_qualified_stocks(data)
            self.logger.info(f"Found {len(call_stocks)} call candidates, {len(put_stocks)} put candidates")
            
            # Group by timestamp
            grouped_calls = self.group_by_timestamp(call_stocks)
            grouped_puts = self.group_by_timestamp(put_stocks)
            
            # Find consecutive appearances
            consecutive_calls = self.find_consecutive_appearances(grouped_calls)
            consecutive_puts = self.find_consecutive_appearances(grouped_puts)
            
            # Format results
            result = self.format_predictions(consecutive_calls, consecutive_puts)
            
            self.logger.info(f"Generated {len(result.call)} call predictions, {len(result.put)} put predictions")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in rank_options: {e}")
            return PredictionResult() 