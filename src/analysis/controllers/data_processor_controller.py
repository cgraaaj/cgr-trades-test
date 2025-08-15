"""
Data Processor Controller
========================
Controller for handling Excel processing and OHLC data fetching operations.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import pandas as pd
import logging
from typing import Optional, Tuple

import db_config
from ..services.external_api_service import ExternalAPIService
from ..views.export_view import ExportView, FileManager
from ..models.entities import AnalysisResultDTO, ExportResultDTO
from ..config.settings import api_config


class DataProcessorController:
    """Controller for data processing operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize services and views
        self.api_service = ExternalAPIService()
        self.export_view = ExportView()
        self.file_manager = FileManager()
    
    async def process_complete_pipeline(self, excel_file: Optional[str] = None) -> AnalysisResultDTO:
        """Run the complete data processing pipeline: Excel processing + OHLC fetching"""
        try:
            self.logger.info("🚀 Starting complete data processing pipeline...")
            
            # Find Excel file if not provided
            if not excel_file:
                excel_file = self.file_manager.find_latest_predictions_file()
                if not excel_file:
                    error_msg = "No predictions Excel file found"
                    return AnalysisResultDTO(success=False, error_message=error_msg)
            
            # Process Excel file to add instrument keys and separate calls/puts
            calls_df, puts_df = self.process_excel_file(excel_file)
            
            if calls_df.empty and puts_df.empty:
                error_msg = "No data found in either calls or puts after processing"
                return AnalysisResultDTO(success=False, error_message=error_msg)
            
            # Save enhanced Excel file
            enhanced_excel_result = self.export_view.export_enhanced_excel_with_calls_puts(calls_df, puts_df)
            
            # Process OHLC data fetching
            calls_csv_file = None
            puts_csv_file = None
            files_created = []
            
            if enhanced_excel_result.success:
                files_created.extend(enhanced_excel_result.files_created)
            
            # Process Calls data if available
            if not calls_df.empty:
                self.logger.info(f"\n📈 Processing Calls data:")
                self.logger.info(f"  Total rows: {len(calls_df)}")
                self.logger.info(f"  Unique stocks: {calls_df['Stock'].nunique()}")
                self.logger.info(f"  Stocks with instrument keys: {calls_df['instrument_key'].notna().sum()}")
                
                calls_result = await self._process_option_type_data(calls_df, "Calls")
                if calls_result.success:
                    files_created.extend(calls_result.files_created)
                    calls_csv_file = calls_result.files_created[0] if calls_result.files_created else None
            
            # Process Puts data if available
            if not puts_df.empty:
                self.logger.info(f"\n📈 Processing Puts data:")
                self.logger.info(f"  Total rows: {len(puts_df)}")
                self.logger.info(f"  Unique stocks: {puts_df['Stock'].nunique()}")
                self.logger.info(f"  Stocks with instrument keys: {puts_df['instrument_key'].notna().sum()}")
                
                puts_result = await self._process_option_type_data(puts_df, "Puts")
                if puts_result.success:
                    files_created.extend(puts_result.files_created)
                    puts_csv_file = puts_result.files_created[0] if puts_result.files_created else None
            
            # Display final summary
            self.logger.info("\n" + "="*80)
            self.logger.info("📈 DATA PROCESSING PIPELINE COMPLETION SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Input file:                   {excel_file}")
            if enhanced_excel_result.success:
                self.logger.info(f"Enhanced Excel file:          {enhanced_excel_result.files_created[0]}")
            if calls_csv_file:
                self.logger.info(f"Calls OHLC CSV:               {calls_csv_file}")
            if puts_csv_file:
                self.logger.info(f"Puts OHLC CSV:                {puts_csv_file}")
            self.logger.info("="*80)
            
            self.logger.info(f"\n✅ Successfully completed data processing pipeline!")
            
            # Create metadata
            metadata = {
                "input_file": excel_file,
                "calls_processed": len(calls_df) if not calls_df.empty else 0,
                "puts_processed": len(puts_df) if not puts_df.empty else 0,
                "files_created": files_created
            }
            
            return AnalysisResultDTO(
                success=True,
                data={
                    "enhanced_excel": enhanced_excel_result.files_created[0] if enhanced_excel_result.success else None,
                    "calls_csv": calls_csv_file,
                    "puts_csv": puts_csv_file
                },
                metadata=metadata
            )
            
        except Exception as e:
            error_msg = f"Error in complete data processing pipeline: {e}"
            self.logger.error(error_msg)
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            return AnalysisResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def process_excel_file(self, excel_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Process Excel file to add instrument keys and separate calls/puts"""
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
                calls_df, puts_df = self._read_multi_sheet_excel(excel_file, sheet_names)
            else:
                # Single sheet - read and separate
                self.logger.info("📊 Single sheet found, reading and separating data...")
                original_df = pd.read_excel(excel_file)
                calls_df, puts_df = self.export_view.separate_calls_puts_data(original_df)
            
            self.logger.info(f"✅ Calls data: {len(calls_df)} rows")
            self.logger.info(f"✅ Puts data: {len(puts_df)} rows")
            
            # Load instrument key mapping
            self.logger.info("🔑 Loading instrument key mapping...")
            self.api_service.load_nse_instrument_mapping(db_config.DATABASE_URL)
            
            # Add instrument keys to both dataframes
            if self.api_service.stock_to_instrument_key:
                self.logger.info("✅ Adding instrument keys to both calls and puts data...")
                
                calls_df = self.api_service.add_instrument_keys_to_dataframe(calls_df)
                puts_df = self.api_service.add_instrument_keys_to_dataframe(puts_df)
                
                calls_stocks_with_keys = calls_df['instrument_key'].notna().sum()
                puts_stocks_with_keys = puts_df['instrument_key'].notna().sum()
                
                self.logger.info(f"✅ Calls: Added instrument keys to {calls_stocks_with_keys}/{len(calls_df)} rows")
                self.logger.info(f"✅ Puts: Added instrument keys to {puts_stocks_with_keys}/{len(puts_df)} rows")
            else:
                self.logger.warning("❌ No instrument key mapping available. Adding empty instrument_key column.")
                calls_df['instrument_key'] = None
                puts_df['instrument_key'] = None
            
            return calls_df, puts_df
            
        except Exception as e:
            self.logger.error(f"❌ Error processing Excel file: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return pd.DataFrame(), pd.DataFrame()
    
    def _read_multi_sheet_excel(self, excel_file: str, sheet_names: list) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Read Excel file with multiple sheets and identify calls/puts"""
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
            calls_df, puts_df = self.export_view.separate_calls_puts_data(original_df)
        
        return calls_df, puts_df
    
    async def _process_option_type_data(self, df: pd.DataFrame, option_type: str) -> ExportResultDTO:
        """Process OHLC data for a specific option type (Calls/Puts)"""
        try:
            # Prepare API requests
            requests = self.api_service.prepare_api_requests(df, option_type)
            
            if not requests:
                self.logger.warning(f"No API requests prepared for {option_type}")
                return ExportResultDTO(
                    success=False,
                    error_message=f"No valid requests prepared for {option_type}"
                )
            
            # Fetch OHLC data
            self.logger.info(f"🔄 Fetching {option_type} OHLC data...")
            dataframes = await self.api_service.fetch_all_ohlc_data(requests, option_type)
            
            if not dataframes:
                self.logger.warning(f"No OHLC data retrieved for {option_type}")
                return ExportResultDTO(
                    success=False,
                    error_message=f"No OHLC data retrieved for {option_type}"
                )
            
            # Export to CSV
            return self.export_view.export_ohlc_data_to_csv(dataframes, option_type)
            
        except Exception as e:
            error_msg = f"Error processing {option_type} data: {e}"
            self.logger.error(error_msg)
            return ExportResultDTO(
                success=False,
                error_message=error_msg
            )
    
    def get_instrument_key_for_stock(self, stock_name: str) -> Optional[str]:
        """Get instrument key for a specific stock"""
        if not self.api_service.stock_to_instrument_key:
            self.api_service.load_nse_instrument_mapping(db_config.DATABASE_URL)
        
        return self.api_service.get_instrument_key_for_stock(stock_name)
    
    def get_available_instrument_keys(self) -> dict:
        """Get all available instrument key mappings"""
        if not self.api_service.stock_to_instrument_key:
            self.api_service.load_nse_instrument_mapping(db_config.DATABASE_URL)
        
        return self.api_service.stock_to_instrument_key.copy() 