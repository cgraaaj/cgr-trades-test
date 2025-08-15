"""
Export View
==========
Handles all file export operations including Excel, CSV, and Pickle formats.
"""

import os
import pickle
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from ..models.entities import ExportConfig, ExportResultDTO, PredictionResult
from ..config.settings import get_analysis_dir


class ExportView:
    """View for handling data export operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def export_predictions_to_excel(self, predictions: PredictionResult, 
                                  filename: str) -> ExportResultDTO:
        """Export predictions to Excel file with separate sheets for calls and puts"""
        try:
            # Create DataFrames for calls and puts
            calls_data = []
            puts_data = []
            
            # Process call predictions
            for date_entry in predictions.call:
                date = date_entry.get("date", "")
                for stock_data in date_entry.get("stock_data", []):
                    # Format timestamp properly
                    time_stamp = stock_data.get("time_stamp", "")
                    formatted_time = self._format_timestamp(time_stamp)
                    
                    calls_data.append({
                        "Date": date,
                        "Time": formatted_time,
                        "Stock": stock_data.get("stock", ""),
                        "Grade": stock_data.get("grade", ""),
                        "TN_Ratio": stock_data.get("tn_ratio", 0),
                        "Bullish_Count": stock_data.get("bullish_count", 0),
                        "Bearish_Count": stock_data.get("bearish_count", 0)
                    })
            
            # Process put predictions
            for date_entry in predictions.put:
                date = date_entry.get("date", "")
                for stock_data in date_entry.get("stock_data", []):
                    # Format timestamp properly
                    time_stamp = stock_data.get("time_stamp", "")
                    formatted_time = self._format_timestamp(time_stamp)
                    
                    puts_data.append({
                        "Date": date,
                        "Time": formatted_time,
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
            
            self.logger.info(f"Predictions exported to Excel: {filename}")
            return ExportResultDTO(
                success=True,
                files_created=[filename]
            )
            
        except Exception as e:
            error_msg = f"Error exporting predictions to Excel: {e}"
            self.logger.error(error_msg)
            return ExportResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def export_enhanced_excel_with_calls_puts(self, calls_df: pd.DataFrame, 
                                            puts_df: pd.DataFrame) -> ExportResultDTO:
        """Save enhanced data with instrument keys to Excel"""
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
            return ExportResultDTO(
                success=True,
                files_created=[output_file]
            )
            
        except Exception as e:
            error_msg = f"Error saving enhanced Excel: {e}"
            self.logger.error(error_msg)
            return ExportResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def export_ohlc_data_to_csv(self, dataframes: List[pd.DataFrame], 
                               option_type: str) -> ExportResultDTO:
        """Export OHLC data to CSV files"""
        try:
            if not dataframes:
                self.logger.warning(f"No dataframes to export for {option_type}")
                return ExportResultDTO(
                    success=False,
                    error_message=f"No dataframes provided for {option_type}"
                )
            
            # Combine all dataframes
            combined_df = pd.concat(dataframes, ignore_index=True)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{option_type.lower()}_ohlc_data_{timestamp}.csv"
            
            # Save to CSV
            combined_df.to_csv(filename, index=False)
            
            self.logger.info(f"💾 Saved {option_type} data to: {filename}")
            self.logger.info(f"✅ {option_type}: {len(combined_df)} total candle records")
            
            return ExportResultDTO(
                success=True,
                files_created=[filename]
            )
            
        except Exception as e:
            error_msg = f"Error exporting {option_type} OHLC data to CSV: {e}"
            self.logger.error(error_msg)
            return ExportResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def export_analysis_results_to_pickle(self, results: List[Dict], 
                                        date_range: str) -> ExportResultDTO:
        """Export analysis results to pickle file"""
        try:
            filename = f'analyzed_stocks_data_optimized_{date_range}.pickle'
            
            with open(filename, 'wb') as handle:
                pickle.dump(results, handle, protocol=pickle.HIGHEST_PROTOCOL)
            
            self.logger.info(f"Results saved to {filename}")
            return ExportResultDTO(
                success=True,
                files_created=[filename]
            )
            
        except Exception as e:
            error_msg = f"Error saving results to pickle: {e}"
            self.logger.error(error_msg)
            return ExportResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def export_predictions_to_pickle(self, predictions: PredictionResult, 
                                   date_range: str) -> ExportResultDTO:
        """Export predictions to pickle file"""
        try:
            filename = f'prediction_optimized_{date_range}.pickle'
            
            # Convert PredictionResult to dict for pickle serialization
            prediction_dict = {
                "call": predictions.call,
                "put": predictions.put
            }
            
            with open(filename, 'wb') as handle:
                pickle.dump(prediction_dict, handle, protocol=pickle.HIGHEST_PROTOCOL)
            
            # Calculate prediction counts for logging
            call_count = sum(len(pred.get("stock_data", [])) for pred in predictions.call)
            put_count = sum(len(pred.get("stock_data", [])) for pred in predictions.put)
            
            self.logger.info(f"Predictions generated: {call_count} calls, {put_count} puts")
            self.logger.info(f"Predictions saved to {filename}")
            
            return ExportResultDTO(
                success=True,
                files_created=[filename]
            )
            
        except Exception as e:
            error_msg = f"Error saving predictions to pickle: {e}"
            self.logger.error(error_msg)
            return ExportResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def _format_timestamp(self, time_stamp) -> str:
        """Format timestamp for display"""
        formatted_time = ""
        if time_stamp:
            try:
                if hasattr(time_stamp, 'strftime'):
                    formatted_time = time_stamp.strftime("%H:%M:%S")
                else:
                    formatted_time = str(time_stamp)
            except:
                formatted_time = str(time_stamp)
        return formatted_time
    
    def separate_calls_puts_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Separate data into calls and puts based on available criteria"""
        
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


class FileManager:
    """Utility class for file management operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def find_latest_predictions_file(self, pattern: str = "option_predictions_optimized_*.xlsx") -> Optional[str]:
        """Find the latest option predictions Excel file"""
        try:
            import glob
            
            # Look for files matching the pattern from option_analyze_optimized.py
            patterns = [
                pattern,
                f"../{pattern}",
                pattern.replace("optimized_", ""),
                f"../{pattern.replace('optimized_', '')}"
            ]
            
            excel_files = []
            for search_pattern in patterns:
                excel_files.extend(glob.glob(search_pattern))
            
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
    
    def create_date_range_string(self, trading_dates: List[str]) -> str:
        """Create a date range string for filenames"""
        if not trading_dates:
            return datetime.now().strftime("%Y%m%d")
        
        if len(trading_dates) == 1:
            return trading_dates[0].replace("-", "")
        
        start_date = trading_dates[0].replace("-", "")
        end_date = trading_dates[-1].replace("-", "")
        return f"{start_date}_to_{end_date}"
    
    def ensure_output_directory(self, directory: str = None) -> str:
        """Ensure output directory exists"""
        if directory is None:
            directory = get_analysis_dir()
        
        os.makedirs(directory, exist_ok=True)
        return directory 