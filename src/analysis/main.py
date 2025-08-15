"""
Main Application Entry Point
===========================
Demonstrates usage of the refactored MVC architecture for options analysis.
"""

import asyncio
import logging
import sys
import os
from typing import Optional, List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Import controllers
from .controllers.analysis_controller import AnalysisController
from .controllers.data_processor_controller import DataProcessorController
from .config.settings import get_analysis_dir


class OptionsAnalysisApp:
    """Main application class for options analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.analysis_controller = AnalysisController()
        self.data_processor_controller = DataProcessorController()
    
    async def run_full_analysis_workflow(self, trading_dates: Optional[List[str]] = None):
        """Run the complete analysis workflow: Analysis -> Processing -> Export"""
        try:
            self.logger.info("🚀 Starting FULL OPTIONS ANALYSIS WORKFLOW")
            self.logger.info("="*80)
            
            # Step 1: Run option analysis
            self.logger.info("📊 STEP 1: Running Option Analysis...")
            analysis_result = await self.analysis_controller.run_complete_analysis(trading_dates)
            
            if not analysis_result.success:
                self.logger.error(f"❌ Analysis failed: {analysis_result.error_message}")
                return analysis_result
            
            self.logger.info("✅ Option analysis completed successfully!")
            self.logger.info(f"📁 Files created: {analysis_result.metadata.get('files_created', [])}")
            
            # Step 2: Process Excel and fetch OHLC data
            self.logger.info("\n📈 STEP 2: Processing Excel and fetching OHLC data...")
            processing_result = await self.data_processor_controller.process_complete_pipeline()
            
            if not processing_result.success:
                self.logger.error(f"❌ Data processing failed: {processing_result.error_message}")
                return processing_result
            
            self.logger.info("✅ Data processing completed successfully!")
            self.logger.info(f"📁 Additional files created: {processing_result.metadata.get('files_created', [])}")
            
            # Summary
            self.logger.info("\n" + "="*80)
            self.logger.info("🎉 FULL WORKFLOW COMPLETED SUCCESSFULLY!")
            self.logger.info("="*80)
            
            # Get performance metrics
            metrics = self.analysis_controller.get_performance_metrics()
            if metrics:
                self.logger.info(f"📊 Performance Summary:")
                self.logger.info(f"  • Total stocks processed: {metrics.total_stocks_processed}")
                self.logger.info(f"  • Successful predictions: {metrics.successful_predictions}")
                self.logger.info(f"  • Processing time: {metrics.processing_time_seconds/60:.2f} minutes")
            
            # Combine results
            all_files = analysis_result.metadata.get('files_created', [])
            all_files.extend(processing_result.metadata.get('files_created', []))
            
            return {
                "success": True,
                "analysis_result": analysis_result,
                "processing_result": processing_result,
                "all_files_created": all_files,
                "performance_metrics": metrics
            }
            
        except Exception as e:
            error_msg = f"Error in full workflow: {e}"
            self.logger.error(error_msg)
            return {"success": False, "error": error_msg}
    
    async def run_analysis_only(self, trading_dates: Optional[List[str]] = None):
        """Run only the option analysis part"""
        self.logger.info("📊 Running ANALYSIS ONLY workflow...")
        return await self.analysis_controller.run_complete_analysis(trading_dates)
    
    async def run_data_processing_only(self, excel_file: Optional[str] = None):
        """Run only the data processing part (requires existing Excel file)"""
        self.logger.info("📈 Running DATA PROCESSING ONLY workflow...")
        return await self.data_processor_controller.process_complete_pipeline(excel_file)
    
    def get_instrument_key_for_stock(self, stock_name: str) -> Optional[str]:
        """Get instrument key for a specific stock"""
        return self.data_processor_controller.get_instrument_key_for_stock(stock_name)
    
    def get_available_instrument_keys(self) -> dict:
        """Get all available instrument key mappings"""
        return self.data_processor_controller.get_available_instrument_keys()


# CLI Interface Functions
async def main_analysis_workflow():
    """Main analysis workflow - can be called from CLI"""
    app = OptionsAnalysisApp()
    
    # You can specify custom trading dates here, or leave as None to use latest
    # trading_dates = ["2025-05-08"]  # Example
    trading_dates = None  # Use latest available date
    
    result = await app.run_full_analysis_workflow(trading_dates)
    
    if result.get("success"):
        print("\n🎉 SUCCESS! All files have been generated.")
        print(f"📁 Files created: {result.get('all_files_created', [])}")
    else:
        print(f"\n❌ FAILED: {result.get('error', 'Unknown error')}")
    
    return result


async def analysis_only_workflow():
    """Run only analysis workflow"""
    app = OptionsAnalysisApp()
    result = await app.run_analysis_only()
    
    if result.success:
        print(f"\n✅ Analysis completed! Files: {result.metadata.get('files_created', [])}")
    else:
        print(f"\n❌ Analysis failed: {result.error_message}")
    
    return result


async def data_processing_only_workflow(excel_file: Optional[str] = None):
    """Run only data processing workflow"""
    app = OptionsAnalysisApp()
    result = await app.run_data_processing_only(excel_file)
    
    if result.success:
        print(f"\n✅ Data processing completed! Files: {result.metadata.get('files_created', [])}")
    else:
        print(f"\n❌ Data processing failed: {result.error_message}")
    
    return result


def get_stock_instrument_key(stock_name: str):
    """Utility function to get instrument key for a stock"""
    app = OptionsAnalysisApp()
    key = app.get_instrument_key_for_stock(stock_name)
    
    if key:
        print(f"📊 Instrument key for {stock_name}: {key}")
    else:
        print(f"❌ No instrument key found for {stock_name}")
    
    return key


def list_all_instrument_keys():
    """Utility function to list all available instrument keys"""
    app = OptionsAnalysisApp()
    mappings = app.get_available_instrument_keys()
    
    print(f"📊 Found {len(mappings)} stock-to-instrument-key mappings:")
    for stock, key in sorted(mappings.items()):
        print(f"  {stock}: {key}")
    
    return mappings


# Command-line interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Options Analysis Application")
    parser.add_argument(
        "--mode", 
        choices=["full", "analysis", "processing", "get-key", "list-keys"],
        default="full",
        help="Operation mode"
    )
    parser.add_argument(
        "--excel-file",
        help="Excel file path for data processing mode"
    )
    parser.add_argument(
        "--stock-name",
        help="Stock name for get-key mode"
    )
    parser.add_argument(
        "--dates",
        nargs="+",
        help="Trading dates for analysis (YYYY-MM-DD format)"
    )
    
    args = parser.parse_args()
    
    if args.mode == "full":
        print("🚀 Running FULL workflow (Analysis + Data Processing)...")
        asyncio.run(main_analysis_workflow())
    
    elif args.mode == "analysis":
        print("📊 Running ANALYSIS ONLY workflow...")
        asyncio.run(analysis_only_workflow())
    
    elif args.mode == "processing":
        print("📈 Running DATA PROCESSING ONLY workflow...")
        asyncio.run(data_processing_only_workflow(args.excel_file))
    
    elif args.mode == "get-key":
        if not args.stock_name:
            print("❌ Please provide --stock-name for get-key mode")
            sys.exit(1)
        get_stock_instrument_key(args.stock_name)
    
    elif args.mode == "list-keys":
        list_all_instrument_keys()


"""
USAGE EXAMPLES:
==============

1. Run full workflow:
   python -m src.analysis.main --mode full

2. Run analysis only:
   python -m src.analysis.main --mode analysis

3. Run data processing only:
   python -m src.analysis.main --mode processing

4. Get instrument key for a stock:
   python -m src.analysis.main --mode get-key --stock-name RELIANCE

5. List all instrument keys:
   python -m src.analysis.main --mode list-keys

6. Run analysis for specific dates:
   python -m src.analysis.main --mode analysis --dates 2025-05-08 2025-05-09

PROGRAMMATIC USAGE:
==================

from src.analysis.main import OptionsAnalysisApp

# Create app instance
app = OptionsAnalysisApp()

# Run full workflow
result = await app.run_full_analysis_workflow()

# Run analysis only
analysis_result = await app.run_analysis_only(["2025-05-08"])

# Run data processing only
processing_result = await app.run_data_processing_only("my_predictions.xlsx")

# Get instrument key
key = app.get_instrument_key_for_stock("RELIANCE")
""" 