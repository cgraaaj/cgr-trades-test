"""
Analysis Controller
==================
Main controller for orchestrating the option analysis workflow.
"""

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any, Optional

import db_config
from ..services.database_service import DatabaseService
from ..services.analysis_service import AnalysisService, OptionRankingService
from ..services.external_api_service import ExternalAPIService
from ..views.export_view import ExportView, FileManager
from ..models.entities import (
    Stock, CEPEPair, ProcessingBatch, AnalysisResultDTO, 
    PredictionResult, PerformanceMetrics
)
from ..config.settings import (
    processing_config, trading_config, analysis_config, default_dates
)


class AnalysisController:
    """Main controller for option analysis operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Initialize services
        self.db_service = DatabaseService()
        self.analysis_service = AnalysisService()
        self.ranking_service = OptionRankingService()
        self.export_view = ExportView()
        self.file_manager = FileManager()
        
        # Performance tracking
        self.performance_metrics = None
    
    async def run_complete_analysis(self, trading_dates: Optional[List[str]] = None) -> AnalysisResultDTO:
        """Run the complete option analysis workflow"""
        start_time = time.time()
        
        try:
            self.logger.info("🚀 Starting complete option analysis workflow...")
            
            # Connect to database
            await self.db_service.connect()
            
            # Get trading dates if not provided
            if not trading_dates:
                self.logger.info("📅 Fetching available trading dates...")
                available_dates = await self.db_service.get_available_trading_dates()
                if not available_dates:
                    error_msg = "No trading dates found in database"
                    self.logger.error(error_msg)
                    return AnalysisResultDTO(success=False, error_message=error_msg)
                
                # Use latest date for demo - in production you might want multiple dates
                trading_dates = [available_dates[-1]]  # or available_dates for all dates
            
            self.logger.info(f"📊 Processing {len(trading_dates)} trading dates: {trading_dates}")
            
            # Get active stocks
            stocks = await self.db_service.get_active_stocks()
            if not stocks:
                error_msg = "No active stocks found in database"
                self.logger.error(error_msg)
                return AnalysisResultDTO(success=False, error_message=error_msg)
            
            self.logger.info(f"📈 Found {len(stocks)} active stocks")
            
            # Get available expiry dates
            expiry_dates = await self.db_service.get_available_expiry_dates()
            if not expiry_dates:
                error_msg = "No expiry dates found in database"
                self.logger.error(error_msg)
                return AnalysisResultDTO(success=False, error_message=error_msg)
            
            # Process analysis for all dates
            all_results = []
            total_batches = (len(stocks) - 1) // processing_config.BATCH_SIZE + 1
            
            for trade_date in trading_dates:
                # Get appropriate expiry date for this trade date
                expiry_date = self._get_expiry_for_trade_date(trade_date, expiry_dates)
                
                self.logger.info(f"📅 Processing date: {trade_date} (expiry: {expiry_date})")
                
                # Process stocks in batches
                date_results = []
                for i in range(0, len(stocks), processing_config.BATCH_SIZE):
                    batch_stocks = stocks[i:i + processing_config.BATCH_SIZE]
                    current_batch = i // processing_config.BATCH_SIZE + 1
                    
                    self.logger.info(f"🔄 Processing batch {current_batch}/{total_batches} ({len(batch_stocks)} stocks)")
                    
                    batch_results = await self._process_stocks_batch(
                        batch_stocks, trade_date, expiry_date
                    )
                    
                    # Filter valid results
                    valid_results = [r for r in batch_results if r is not None and not isinstance(r, Exception)]
                    date_results.extend(valid_results)
                    
                    self.logger.info(f"✅ Batch {current_batch} completed: {len(valid_results)} valid results")
                
                all_results.extend(date_results)
                self.logger.info(f"📊 Date {trade_date} completed: {len(date_results)} total results")
            
            self.logger.info(f"🎯 Analysis completed: {len(all_results)} total stock results")
            
            # Generate predictions
            predictions = self.ranking_service.rank_options(all_results)
            
            # Export results
            date_range = self.file_manager.create_date_range_string(trading_dates)
            
            # Export analysis results to pickle
            analysis_export = self.export_view.export_analysis_results_to_pickle(all_results, date_range)
            
            # Export predictions to Excel and pickle
            prediction_excel_filename = f"option_predictions_optimized_{date_range}.xlsx"
            excel_export = self.export_view.export_predictions_to_excel(predictions, prediction_excel_filename)
            pickle_export = self.export_view.export_predictions_to_pickle(predictions, date_range)
            
            # Calculate performance metrics
            execution_time = time.time() - start_time
            self.performance_metrics = PerformanceMetrics(
                total_stocks_processed=len(all_results),
                successful_predictions=len(predictions.call) + len(predictions.put),
                failed_predictions=len(stocks) * len(trading_dates) - len(all_results),
                processing_time_seconds=execution_time
            )
            
            self.logger.info(f"⏱️ Total execution time: {execution_time/60:.2f} minutes")
            
            # Create result metadata
            metadata = {
                "trading_dates": trading_dates,
                "total_stocks": len(stocks),
                "execution_time_minutes": execution_time / 60,
                "files_created": []
            }
            
            if analysis_export.success:
                metadata["files_created"].extend(analysis_export.files_created)
            if excel_export.success:
                metadata["files_created"].extend(excel_export.files_created)
            if pickle_export.success:
                metadata["files_created"].extend(pickle_export.files_created)
            
            return AnalysisResultDTO(
                success=True,
                data=predictions,
                metadata=metadata
            )
            
        except Exception as e:
            error_msg = f"Error in complete analysis workflow: {e}"
            self.logger.error(error_msg)
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            return AnalysisResultDTO(
                success=False,
                error_message=error_msg
            )
        
        finally:
            try:
                await self.db_service.disconnect()
            except Exception as e:
                self.logger.error(f"Error disconnecting from database: {e}")
    
    async def _process_stocks_batch(self, stocks: List[Stock], trade_date: str, 
                                  expiry_date: str) -> List[Dict]:
        """Process a batch of stocks concurrently"""
        try:
            stock_ids = [stock.id for stock in stocks]
            
            # Get all instruments for this batch
            instrument_df = await self.db_service.get_instruments_bulk(stock_ids, expiry_date)
            if instrument_df.empty:
                self.logger.warning("No instruments found for batch")
                return []
            
            # Get all ticker data for this batch
            instrument_ids = instrument_df['id'].tolist()
            ticker_df = await self.db_service.get_ticker_data_bulk(instrument_ids, trade_date)
            if ticker_df.empty:
                self.logger.warning("No ticker data found for batch")
                return []
            
            # Process each stock in the batch
            tasks = []
            semaphore = asyncio.Semaphore(processing_config.MAX_CONCURRENT_TASKS)
            
            for stock in stocks:
                task = self._process_single_stock_with_semaphore(
                    semaphore, stock, instrument_df, ticker_df, trade_date
                )
                tasks.append(task)
            
            return await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"Error in process_stocks_batch: {e}")
            return []
    
    async def _process_single_stock_with_semaphore(self, semaphore: asyncio.Semaphore, 
                                                 stock: Stock, instrument_df, ticker_df, 
                                                 trade_date: str):
        """Process a single stock with concurrency control"""
        async with semaphore:
            return await self._process_single_stock(stock, instrument_df, ticker_df, trade_date)
    
    async def _process_single_stock(self, stock: Stock, instrument_df, ticker_df, 
                                  trade_date: str) -> Optional[Dict]:
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
            
            # Create CE/PE pairs
            ce_pe_pairs = self._create_ce_pe_pairs(stock_instruments)
            if ce_pe_pairs.empty:
                return None
            
            # Get cached trading timestamps
            trading_timestamps = self.db_service.get_trading_timestamps(trade_date)
            
            # Process all pairs
            processed_data = []
            for _, pair_row in ce_pe_pairs.iterrows():
                pair = CEPEPair(
                    ce_id=pair_row.get('ce_id'),
                    pe_id=pair_row.get('pe_id'),
                    strike_price=pair_row['strike_price']
                )
                
                pair_data = self.analysis_service.process_ce_pe_pair_optimized(
                    stock_ticker_df, pair, trade_date, trading_timestamps
                )
                if not pair_data.empty:
                    processed_data.append(pair_data)
            
            if not processed_data:
                return None
            
            # Combine and simulate data
            combined_df = pd.concat(processed_data, ignore_index=True)
            simulation_dfs = self.analysis_service.get_min_simulation_optimized(
                combined_df, trading_config.DEFAULT_INTERVAL
            )
            
            # Process simulation results
            simulated_data = []
            for sim_df in simulation_dfs:
                if not sim_df.empty:
                    analysis = self.analysis_service.trend_n_grade_analysis_optimized(sim_df)
                    if analysis:
                        simulated_data.append(analysis)
            
            return {
                "name": stock.name,
                "opt_data": simulated_data
            }
            
        except Exception as e:
            self.logger.error(f"Error processing stock {stock.name}: {e}")
            return None
    
    def _create_ce_pe_pairs(self, instruments_df) -> pd.DataFrame:
        """Create CE/PE pairs from instruments"""
        import pandas as pd
        
        ce_instruments = instruments_df[instruments_df['instrument_type'] == 'CE'][['id', 'strike_price']]
        pe_instruments = instruments_df[instruments_df['instrument_type'] == 'PE'][['id', 'strike_price']]
        
        # Merge CE and PE data
        ce_pe_pairs = pd.merge(ce_instruments, pe_instruments, on='strike_price', 
                              how='outer', suffixes=('_ce', '_pe'))
        ce_pe_pairs.columns = ['ce_id', 'strike_price', 'pe_id']
        ce_pe_pairs = ce_pe_pairs.sort_values('strike_price')
        
        return ce_pe_pairs
    
    def _get_expiry_for_trade_date(self, trade_date: str, expiry_dates: List[str]) -> str:
        """Get the appropriate expiry date for a given trade date"""
        try:
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
                self.logger.warning(f"No suitable expiry found for {trade_date}, using last available: {expiry_dates[-1]}")
                return expiry_dates[-1]
            
            # Fallback to default
            self.logger.error(f"No expiry dates available for trade date {trade_date}")
            return default_dates.DEFAULT_EXPIRY_DATE
            
        except Exception as e:
            self.logger.error(f"Error getting expiry for trade date {trade_date}: {e}")
            return default_dates.DEFAULT_EXPIRY_DATE
    
    def get_performance_metrics(self) -> Optional[PerformanceMetrics]:
        """Get performance metrics from the last analysis run"""
        return self.performance_metrics 