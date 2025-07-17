#!/usr/bin/env python3
"""
Script to update instrument_key column in options.stock table by comparing with NSE.json.
- Adds missing instrument keys for active stocks
- Corrects mismatched instrument keys with latest NSE data
- Handles cases like BAJFINANCE: NSE_EQ|INE296A01024 → NSE_EQ|INE296A01032
"""

import json
import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

# Add parent directory to path to import db_config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import db_config as config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_nse_data(nse_file_path: str) -> dict:
    """Load NSE.json data and create stock name to instrument key mapping."""
    try:
        logger.info(f"📋 Loading NSE data from: {nse_file_path}")
        
        if not os.path.exists(nse_file_path):
            logger.error(f"❌ NSE file not found: {nse_file_path}")
            return {}
        
        with open(nse_file_path, 'r') as file:
            nse_data = json.load(file)
        
        logger.info(f"✅ Loaded {len(nse_data)} instruments from NSE.json")
        
        # Create mapping of stock name to instrument key
        logger.info("🔍 Creating stock name to instrument key mapping...")
        
        stock_to_instrument_key = {}
        matched_count = 0
        
        for instrument in nse_data:
            # Filter criteria: NSE_EQ segment and EQ instrument_type
            if (instrument.get('segment') == 'NSE_EQ' and 
                instrument.get('instrument_type') == 'EQ'):
                
                stock_name = instrument.get('trading_symbol')
                instrument_key = instrument.get('instrument_key')
                
                if stock_name and instrument_key:
                    stock_to_instrument_key[stock_name] = instrument_key
                    matched_count += 1
        
        logger.info(f"✅ Created mapping for {matched_count} NSE_EQ + EQ instruments")
        return stock_to_instrument_key
        
    except Exception as e:
        logger.error(f"❌ Error loading NSE data: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {}


def get_stocks_from_database(engine) -> pd.DataFrame:
    """Get all active stocks from the database."""
    try:
        logger.info("📊 Fetching stocks from database...")
        
        # Query to get all active stocks
        query = """
        SELECT id, name, instrument_key, is_active, created_on, updated_on
        FROM options.stock 
        WHERE is_active = true 
        ORDER BY name
        """
        
        with engine.connect() as conn:
            stocks_df = pd.read_sql(query, conn)
        
        logger.info(f"✅ Found {len(stocks_df)} active stocks in database")
        
        # Show current instrument key status
        stocks_with_keys = stocks_df['instrument_key'].notna().sum()
        stocks_without_keys = len(stocks_df) - stocks_with_keys
        
        logger.info(f"📈 Current status:")
        logger.info(f"  Stocks with instrument keys:    {stocks_with_keys}")
        logger.info(f"  Stocks without instrument keys: {stocks_without_keys}")
        
        return stocks_df
        
    except Exception as e:
        logger.error(f"❌ Error fetching stocks from database: {e}")
        return pd.DataFrame()


def update_instrument_keys(engine, stocks_df: pd.DataFrame, stock_to_instrument_key: dict) -> dict:
    """Update instrument keys in the database."""
    try:
        logger.info("🔄 Starting database update process...")
        
        update_stats = {
            "updated_missing": 0,
            "corrected_mismatch": 0,
            "already_correct": 0,
            "not_found_in_nse": 0,
            "failed_updates": 0
        }
        
        updated_missing_stocks = []
        corrected_mismatch_stocks = []
        already_correct_stocks = []
        not_found_stocks = []
        failed_stocks = []
        
        with engine.begin() as conn:
            for _, stock in stocks_df.iterrows():
                stock_name = stock['name']
                current_instrument_key = stock['instrument_key']
                
                # Check if we have instrument key for this stock in NSE data
                if stock_name not in stock_to_instrument_key:
                    logger.debug(f"❌ {stock_name} not found in NSE data")
                    update_stats["not_found_in_nse"] += 1
                    not_found_stocks.append(stock_name)
                    continue
                
                nse_instrument_key = stock_to_instrument_key[stock_name]
                
                # Case 1: Stock has no instrument key (missing)
                if pd.isna(current_instrument_key) or not current_instrument_key:
                    action_type = "MISSING"
                    update_reason = f"Added missing instrument key"
                    update_stats["updated_missing"] += 1
                    updated_missing_stocks.append(stock_name)
                
                # Case 2: Stock has different instrument key (mismatch)
                elif current_instrument_key != nse_instrument_key:
                    action_type = "MISMATCH"
                    update_reason = f"Corrected mismatch: {current_instrument_key} → {nse_instrument_key}"
                    update_stats["corrected_mismatch"] += 1
                    corrected_mismatch_stocks.append({
                        'name': stock_name,
                        'old_key': current_instrument_key,
                        'new_key': nse_instrument_key
                    })
                
                # Case 3: Stock has correct instrument key (skip)
                else:
                    logger.debug(f"✅ {stock_name} already has correct instrument key: {current_instrument_key}")
                    update_stats["already_correct"] += 1
                    already_correct_stocks.append(stock_name)
                    continue
                
                # Perform the update
                try:
                    result = conn.execute(
                        text("""
                            UPDATE options.stock 
                            SET instrument_key = :instrument_key, 
                                updated_on = NOW() 
                            WHERE id = :stock_id
                        """),
                        {
                            'instrument_key': nse_instrument_key,
                            'stock_id': stock['id']
                        }
                    )
                    
                    if result.rowcount > 0:
                        logger.info(f"✅ {action_type}: {stock_name} → {nse_instrument_key}")
                        logger.debug(f"   Reason: {update_reason}")
                    else:
                        logger.warning(f"❌ Failed to update {stock_name} (no rows affected)")
                        update_stats["failed_updates"] += 1
                        failed_stocks.append(stock_name)
                        
                except Exception as e:
                    logger.error(f"❌ Error updating {stock_name}: {e}")
                    update_stats["failed_updates"] += 1
                    failed_stocks.append(stock_name)
        
        # Log detailed results
        logger.info(f"\n📊 Update Results:")
        logger.info(f"  Missing keys added:             {update_stats['updated_missing']}")
        logger.info(f"  Mismatches corrected:           {update_stats['corrected_mismatch']}")
        logger.info(f"  Already correct:                {update_stats['already_correct']}")
        logger.info(f"  Not found in NSE data:          {update_stats['not_found_in_nse']}")
        logger.info(f"  Failed updates:                 {update_stats['failed_updates']}")
        
        # Show detailed lists
        if updated_missing_stocks:
            logger.info(f"\n✅ Added missing instrument keys ({len(updated_missing_stocks)}):")
            for i, stock in enumerate(updated_missing_stocks, 1):
                key = stock_to_instrument_key.get(stock, 'N/A')
                logger.info(f"  {i:3d}. {stock:<15} | {key}")
        
        if corrected_mismatch_stocks:
            logger.info(f"\n🔄 Corrected mismatched instrument keys ({len(corrected_mismatch_stocks)}):")
            for i, stock_info in enumerate(corrected_mismatch_stocks, 1):
                logger.info(f"  {i:3d}. {stock_info['name']:<15}")
                logger.info(f"       OLD: {stock_info['old_key']}")
                logger.info(f"       NEW: {stock_info['new_key']}")
        
        if not_found_stocks:
            logger.info(f"\n❌ Stocks not found in NSE data ({len(not_found_stocks)}):")
            for stock in sorted(not_found_stocks):
                logger.info(f"  - {stock}")
        
        if failed_stocks:
            logger.info(f"\n⚠️  Failed to update ({len(failed_stocks)}):")
            for stock in sorted(failed_stocks):
                logger.info(f"  - {stock}")
        
        return update_stats
        
    except Exception as e:
        logger.error(f"❌ Error during database update: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {"error": str(e)}


def verify_updates(engine) -> None:
    """Verify the updates by checking the current state of the database."""
    try:
        logger.info("🔍 Verifying updates...")
        
        query = """
        SELECT 
            COUNT(*) as total_stocks,
            COUNT(instrument_key) as stocks_with_keys,
            COUNT(*) - COUNT(instrument_key) as stocks_without_keys
        FROM options.stock 
        WHERE is_active = true
        """
        
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchone()
        
        total = result.total_stocks
        with_keys = result.stocks_with_keys
        without_keys = result.stocks_without_keys
        coverage = (with_keys / total * 100) if total > 0 else 0
        
        logger.info(f"\n📈 Final Database Status:")
        logger.info(f"  Total active stocks:            {total}")
        logger.info(f"  Stocks with instrument keys:    {with_keys}")
        logger.info(f"  Stocks without instrument keys: {without_keys}")
        logger.info(f"  Coverage percentage:            {coverage:.1f}%")
        
        # Show some examples of updated stocks
        sample_query = """
        SELECT name, instrument_key, updated_on
        FROM options.stock 
        WHERE is_active = true AND instrument_key IS NOT NULL
        ORDER BY updated_on DESC
        LIMIT 5
        """
        
        with engine.connect() as conn:
            sample_results = conn.execute(text(sample_query)).fetchall()
        
        if sample_results:
            logger.info(f"\n📋 Sample of recently updated stocks:")
            for row in sample_results:
                logger.info(f"  {row.name:<15} | {row.instrument_key} | {row.updated_on}")
        
    except Exception as e:
        logger.error(f"❌ Error during verification: {e}")


def main():
    """Main function to update and correct instrument keys in database."""
    try:
        logger.info("🚀 Starting database instrument key update and correction process...")
        
        # Database connection
        logger.info("📊 Connecting to database...")
        engine = create_engine(config.DB_CONNECTION_STRING)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection verified")
        
        # Load NSE data
        nse_file_path = "/home/cgraaaj/Projects/cgr-trades/python/NSE.json"
        stock_to_instrument_key = load_nse_data(nse_file_path)
        
        if not stock_to_instrument_key:
            logger.error("❌ No instrument key mapping available. Exiting.")
            return
        
        # Get stocks from database
        stocks_df = get_stocks_from_database(engine)
        
        if stocks_df.empty:
            logger.error("❌ No stocks found in database. Exiting.")
            return
        
        # Analyze what needs to be updated
        stocks_missing_keys = stocks_df[stocks_df['instrument_key'].isna()]
        stocks_with_keys = stocks_df[stocks_df['instrument_key'].notna()]
        
        # Check for mismatches in existing keys
        potential_mismatches = []
        for _, stock in stocks_with_keys.iterrows():
            stock_name = stock['name']
            current_key = stock['instrument_key']
            if stock_name in stock_to_instrument_key:
                nse_key = stock_to_instrument_key[stock_name]
                if current_key != nse_key:
                    potential_mismatches.append(stock_name)
        
        stocks_to_add = stocks_missing_keys[stocks_missing_keys['name'].isin(stock_to_instrument_key.keys())]
        
        logger.info(f"\n📋 Update Plan:")
        logger.info(f"  Stocks missing instrument keys: {len(stocks_missing_keys)}")
        logger.info(f"  → Can be added from NSE data:   {len(stocks_to_add)}")
        logger.info(f"  Stocks with potential mismatches: {len(potential_mismatches)}")
        logger.info(f"  Total stocks to be updated:     {len(stocks_to_add) + len(potential_mismatches)}")
        
        if len(stocks_to_add) == 0 and len(potential_mismatches) == 0:
            logger.info("✅ All stocks already have correct instrument keys!")
            verify_updates(engine)
            return
        
        # Show specific stocks that will be updated
        if potential_mismatches:
            logger.info(f"\n🔄 Stocks with mismatched keys to be corrected:")
            for stock in potential_mismatches[:10]:  # Show first 10
                current_key = stocks_df[stocks_df['name'] == stock]['instrument_key'].iloc[0]
                nse_key = stock_to_instrument_key[stock]
                logger.info(f"  • {stock}: {current_key} → {nse_key}")
            if len(potential_mismatches) > 10:
                logger.info(f"  ... and {len(potential_mismatches) - 10} more")
        
        # Confirm before proceeding
        total_updates = len(stocks_to_add) + len(potential_mismatches)
        logger.info(f"\n⚠️  This will update {total_updates} stocks in the database.")
        logger.info("🔄 Proceeding with database updates...")
        
        # Update instrument keys
        update_stats = update_instrument_keys(engine, stocks_df, stock_to_instrument_key)
        
        if "error" in update_stats:
            logger.error(f"❌ Update process failed: {update_stats['error']}")
            return
        
        # Verify updates
        verify_updates(engine)
        
        # Final summary
        logger.info("\n" + "="*70)
        logger.info("📈 FINAL SUMMARY")
        logger.info("="*70)
        logger.info(f"Database update completed successfully!")
        total_changes = update_stats['updated_missing'] + update_stats['corrected_mismatch']
        logger.info(f"Total changes made: {total_changes} stocks")
        logger.info(f"  • Added missing keys: {update_stats['updated_missing']}")
        logger.info(f"  • Corrected mismatches: {update_stats['corrected_mismatch']}")
        logger.info(f"Total process completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info(f"\n✅ Database instrument key update completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Error in main execution: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")


if __name__ == "__main__":
    main() 