"""
Demo: Options Analysis MVC Architecture
=======================================
Simple demonstration of the new clean architecture.
"""

import asyncio
import logging
from datetime import datetime

# Configure logging for demo
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from .main import OptionsAnalysisApp


async def demo_full_workflow():
    """Demo: Complete analysis workflow"""
    print("🚀 Demo: Full Options Analysis Workflow")
    print("="*50)
    
    # Create app instance
    app = OptionsAnalysisApp()
    
    # Run complete workflow
    print("📊 Running complete analysis and data processing...")
    result = await app.run_full_analysis_workflow()
    
    if result["success"]:
        print("\n✅ SUCCESS! Workflow completed.")
        print(f"📁 Files created: {len(result['all_files_created'])}")
        for file in result['all_files_created']:
            print(f"   📄 {file}")
        
        # Show performance metrics
        metrics = result.get("performance_metrics")
        if metrics:
            print(f"\n📊 Performance:")
            print(f"   • Stocks processed: {metrics.total_stocks_processed}")
            print(f"   • Predictions generated: {metrics.successful_predictions}")
            print(f"   • Processing time: {metrics.processing_time_seconds/60:.2f} minutes")
    else:
        print(f"\n❌ FAILED: {result.get('error', 'Unknown error')}")
    
    return result


async def demo_analysis_only():
    """Demo: Analysis only workflow"""
    print("\n📈 Demo: Analysis Only Workflow")
    print("="*50)
    
    app = OptionsAnalysisApp()
    
    # Run only analysis (no OHLC data fetching)
    result = await app.run_analysis_only()
    
    if result.success:
        print("✅ Analysis completed!")
        print(f"📁 Files: {result.metadata.get('files_created', [])}")
        
        # Show prediction summary
        predictions = result.data
        call_count = sum(len(pred.get("stock_data", [])) for pred in predictions.call)
        put_count = sum(len(pred.get("stock_data", [])) for pred in predictions.put)
        
        print(f"📊 Predictions generated:")
        print(f"   • Call options: {call_count}")
        print(f"   • Put options: {put_count}")
    else:
        print(f"❌ Analysis failed: {result.error_message}")
    
    return result


async def demo_data_processing_only():
    """Demo: Data processing only workflow"""
    print("\n🔄 Demo: Data Processing Only Workflow")
    print("="*50)
    
    app = OptionsAnalysisApp()
    
    # This requires an existing Excel file from analysis
    print("📊 Processing latest predictions Excel file...")
    result = await app.run_data_processing_only()
    
    if result.success:
        print("✅ Data processing completed!")
        print(f"📁 Files: {result.metadata.get('files_created', [])}")
    else:
        print(f"❌ Data processing failed: {result.error_message}")
    
    return result


def demo_utility_functions():
    """Demo: Utility functions"""
    print("\n🔧 Demo: Utility Functions")
    print("="*50)
    
    app = OptionsAnalysisApp()
    
    # Get instrument keys
    print("📋 Getting instrument key mappings...")
    mappings = app.get_available_instrument_keys()
    
    if mappings:
        print(f"✅ Found {len(mappings)} instrument key mappings")
        
        # Show first 5 mappings as example
        print("📊 Sample mappings:")
        for i, (stock, key) in enumerate(sorted(mappings.items())[:5]):
            print(f"   {i+1}. {stock}: {key}")
        
        # Demonstrate getting key for specific stock
        sample_stock = list(mappings.keys())[0]
        key = app.get_instrument_key_for_stock(sample_stock)
        print(f"\n🔍 Instrument key for {sample_stock}: {key}")
    else:
        print("❌ No instrument key mappings found")
    
    return mappings


async def demo_individual_services():
    """Demo: Using individual services"""
    print("\n🔬 Demo: Individual Services")
    print("="*50)
    
    # Import individual services
    from .services.database_service import DatabaseService
    from .services.analysis_service import AnalysisService
    from .views.export_view import ExportView
    
    # Database service demo
    print("📊 Testing Database Service...")
    db_service = DatabaseService()
    try:
        await db_service.connect()
        stocks = await db_service.get_active_stocks()
        print(f"✅ Found {len(stocks)} active stocks")
        
        if stocks:
            sample_stock = stocks[0]
            print(f"📈 Sample stock: {sample_stock.name} (ID: {sample_stock.id})")
        
        await db_service.disconnect()
    except Exception as e:
        print(f"❌ Database error: {e}")
    
    # Analysis service demo
    print("\n🧮 Testing Analysis Service...")
    analysis_service = AnalysisService()
    
    # Test percentage to grade conversion
    test_percentages = [100, 90, 75, 40]
    print("📊 Grade conversion examples:")
    for pct in test_percentages:
        grade = analysis_service.get_grade_from_percentage(pct)
        print(f"   {pct}% → Grade {grade}")
    
    # Export view demo
    print("\n📁 Testing Export View...")
    export_view = ExportView()
    print("✅ Export view initialized successfully")


async def main_demo():
    """Run all demonstrations"""
    print("🎯 OPTIONS ANALYSIS MVC ARCHITECTURE DEMO")
    print("="*80)
    print(f"📅 Demo started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Demo 1: Utility functions (doesn't require database)
        demo_utility_functions()
        
        # Demo 2: Individual services
        await demo_individual_services()
        
        # Demo 3: Analysis only workflow
        await demo_analysis_only()
        
        # Demo 4: Data processing only (if Excel exists)
        await demo_data_processing_only()
        
        # Demo 5: Full workflow (complete pipeline)
        # await demo_full_workflow()  # Uncomment to run full demo
        
        print("\n" + "="*80)
        print("🎉 DEMO COMPLETED SUCCESSFULLY!")
        print("="*80)
        
    except KeyboardInterrupt:
        print("\n⚠️ Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run the demo
    asyncio.run(main_demo()) 