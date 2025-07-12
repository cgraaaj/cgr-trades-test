import pickle
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
from dataclasses import dataclass
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration constants
class Config:
    TN_RATIO_THRESHOLD = 60
    TRADING_START_TIME = "09:30:00"
    TRADING_END_TIME = "14:00:00"
    MAX_TIME_INTERVALS = 25
    ACCEPTED_GRADES = ["A","B"]  # Only accept A grades
    CONSECUTIVE_WINDOW_MINUTES = 15

class Grade(Enum):
    A = "A"
    B = "B" 
    C = "C"
    D = "D"

@dataclass
class StockPrediction:
    """Data class for stock prediction results"""
    stock: str
    timestamp: datetime
    grade: str
    option_type: str  # 'call' or 'put'
    tn_ratio: int
    bullish_count: int
    bearish_count: int

class OptionRankingOptimized:
    """Optimized option ranking with better performance and error handling"""
    
    def __init__(self, config: Config = None):
        self.config = config or Config()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def validate_data_structure(self, data: List[Dict]) -> bool:
        """Validate that the input data has the expected structure"""
        try:
            if not data or not isinstance(data, list):
                self.logger.error("Data must be a non-empty list")
                return False
            
            # Check first item structure
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
            return (self.config.TRADING_START_TIME <= time_str <= self.config.TRADING_END_TIME)
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
                max_intervals = min(len(opt_data), self.config.MAX_TIME_INTERVALS)
                
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
            if (options["tn_ratio"] > self.config.TN_RATIO_THRESHOLD and 
                options["bullish"] > options["bearish"] and
                options["grade"] in self.config.ACCEPTED_GRADES):
                
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
        
        # Sort by timestamp
        return dict(sorted(grouped.items()))
    
    def find_consecutive_appearances(self, grouped_predictions: Dict[datetime, List[StockPrediction]]) -> Dict[str, List[StockPrediction]]:
        """Find stocks that appear in consecutive time intervals"""
        consecutive_by_date = defaultdict(list)
        
        timestamps = sorted(grouped_predictions.keys())
        prev_stocks = set()
        current_date = None
        
        for timestamp in timestamps:
            # Reset on new trading day
            if current_date != timestamp.date():
                current_date = timestamp.date()
                prev_stocks = set()
            
            current_stocks = {pred.stock for pred in grouped_predictions[timestamp]}
            
            # Find stocks that appeared in previous interval
            consecutive_stocks = current_stocks.intersection(prev_stocks)
            
            if consecutive_stocks:
                for prediction in grouped_predictions[timestamp]:
                    if prediction.stock in consecutive_stocks:
                        date_key = timestamp.strftime("%Y-%m-%d")
                        consecutive_by_date[date_key].append(prediction)
            
            prev_stocks = current_stocks
        
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
    
    def format_predictions(self, consecutive_calls: Dict, consecutive_puts: Dict) -> Dict[str, List[Dict]]:
        """Format predictions into the expected output structure"""
        prediction_result = {
            "call": [],
            "put": []
        }
        
        # Format call predictions
        for date, predictions in consecutive_calls.items():
            unique_predictions = self.remove_duplicate_stocks(predictions)
            prediction_result["call"].append({
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
            prediction_result["put"].append({
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
    
    def rank_options(self, data: List[Dict]) -> Dict[str, List[Dict]]:
        """Main ranking function with optimized performance"""
        try:
            self.logger.info(f"Starting option ranking for {len(data)} stocks")
            
            # Validate input data
            if not self.validate_data_structure(data):
                return {"call": [], "put": []}
            
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
            
            self.logger.info(f"Generated {len(result['call'])} call predictions, {len(result['put'])} put predictions")
            return result
            
        except Exception as e:
            self.logger.error(f"Error in rank_options: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return {"call": [], "put": []}

def load_analyzed_data(filename: str) -> List[Dict]:
    """Load analyzed stock data from pickle file"""
    try:
        with open(filename, 'rb') as handle:
            data = pickle.load(handle)
        logger.info(f"Loaded {len(data)} stocks from {filename}")
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {filename}")
        return []
    except Exception as e:
        logger.error(f"Error loading data from {filename}: {e}")
        return []

def save_predictions(predictions: Dict, filename: str) -> bool:
    """Save predictions to pickle file"""
    try:
        with open(filename, 'wb') as handle:
            pickle.dump(predictions, handle, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"Predictions saved to {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving predictions to {filename}: {e}")
        return False

def export_predictions_to_excel(predictions: Dict, filename: str) -> bool:
    """Export predictions to Excel file with separate sheets for calls and puts"""
    try:
        # Create DataFrames for calls and puts
        calls_data = []
        puts_data = []
        
        # Process call predictions
        for date_entry in predictions.get("call", []):
            date = date_entry.get("date", "")
            for stock_data in date_entry.get("stock_data", []):
                calls_data.append({
                    "Date": date,
                    "Time": stock_data.get("time_stamp", ""),
                    "Stock": stock_data.get("stock", ""),
                    "Grade": stock_data.get("grade", ""),
                    "TN_Ratio": stock_data.get("tn_ratio", 0),
                    "Bullish_Count": stock_data.get("bullish_count", 0),
                    "Bearish_Count": stock_data.get("bearish_count", 0)
                })
        
        # Process put predictions
        for date_entry in predictions.get("put", []):
            date = date_entry.get("date", "")
            for stock_data in date_entry.get("stock_data", []):
                puts_data.append({
                    "Date": date,
                    "Time": stock_data.get("time_stamp", ""),
                    "Stock": stock_data.get("stock", ""),
                    "Grade": stock_data.get("grade", ""),
                    "TN_Ratio": stock_data.get("tn_ratio", 0),
                    "Bullish_Count": stock_data.get("bullish_count", 0),
                    "Bearish_Count": stock_data.get("bearish_count", 0)
                })
        
        # Create DataFrames
        calls_df = pd.DataFrame(calls_data)
        puts_df = pd.DataFrame(puts_data)
        
        # Export to Excel with multiple sheets
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            calls_df.to_excel(writer, sheet_name='Calls', index=False)
            puts_df.to_excel(writer, sheet_name='Puts', index=False)
            
            # Add a summary sheet
            summary_data = {
                "Option_Type": ["Calls", "Puts"],
                "Count": [len(calls_data), len(puts_data)],
                "Unique_Stocks": [calls_df['Stock'].nunique() if not calls_df.empty else 0, 
                                 puts_df['Stock'].nunique() if not puts_df.empty else 0],
                "Dates_Covered": [calls_df['Date'].nunique() if not calls_df.empty else 0,
                                 puts_df['Date'].nunique() if not puts_df.empty else 0]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        logger.info(f"Predictions exported to Excel: {filename}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting predictions to Excel: {e}")
        return False

def main():
    """Main function to run the optimized option ranking"""
    try:
        # Load data
        data = load_analyzed_data("analyzed_stocks_data_optimized.pickle")
        if not data:
            logger.error("No data loaded, exiting")
            return
        
        # Initialize ranker
        ranker = OptionRankingOptimized()
        
        # Generate rankings
        predictions = ranker.rank_options(data)
        
        # Print summary
        call_count = sum(len(pred["stock_data"]) for pred in predictions["call"])
        put_count = sum(len(pred["stock_data"]) for pred in predictions["put"])
        
        print(f"\n=== OPTION RANKING RESULTS ===")
        print(f"Call Predictions: {call_count} stocks across {len(predictions['call'])} dates")
        print(f"Put Predictions: {put_count} stocks across {len(predictions['put'])} dates")
        
        # Save results
        save_predictions(predictions, "option_predictions_optimized.pickle")
        
        # Export to Excel
        excel_filename = "option_predictions_optimized.xlsx"
        if export_predictions_to_excel(predictions, excel_filename):
            print(f"\nExcel export successful: {excel_filename}")
        else:
            print(f"\nExcel export failed")
        
        # Print detailed results
        print(f"\nDetailed Results:")
        print(f"Calls: {predictions['call']}")
        print(f"Puts: {predictions['put']}")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main() 