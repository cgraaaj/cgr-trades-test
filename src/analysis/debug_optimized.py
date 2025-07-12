import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import asyncio
import pandas as pd
from databases import Database
from sqlalchemy import create_engine, text
import db_config
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = db_config.DATABASE_URL
database = Database(DATABASE_URL)

async def test_database_connection():
    """Test basic database connectivity"""
    try:
        logger.info("Testing database connection...")
        await database.connect()
        logger.info("Database connected successfully")
        
        # Test basic query
        logger.info("Testing basic query...")
        results = await database.fetch_all("SELECT 1 as test")
        logger.info(f"Basic query result: {results}")
        
        # Test stock table exists
        logger.info("Testing stock table...")
        try:
            results = await database.fetch_all("SELECT COUNT(*) as count FROM options.stock")
            logger.info(f"Stock table count: {results}")
        except Exception as e:
            logger.error(f"Stock table error: {e}")
            
            # Try without schema
            try:
                results = await database.fetch_all("SELECT COUNT(*) as count FROM stock")
                logger.info(f"Stock table (no schema) count: {results}")
            except Exception as e2:
                logger.error(f"Stock table (no schema) error: {e2}")
        
        # Test stock table columns
        logger.info("Testing stock table structure...")
        try:
            results = await database.fetch_all("SELECT * FROM options.stock LIMIT 1")
            if results:
                logger.info(f"Stock table columns: {list(results[0].keys())}")
                logger.info(f"Sample row: {dict(results[0])}")
            else:
                logger.info("Stock table is empty")
        except Exception as e:
            logger.error(f"Stock table structure error: {e}")
        
        # Test is_active column
        logger.info("Testing is_active column...")
        try:
            results = await database.fetch_all("SELECT is_active, COUNT(*) as count FROM options.stock GROUP BY is_active")
            logger.info(f"is_active values: {[dict(r) for r in results]}")
        except Exception as e:
            logger.error(f"is_active column error: {e}")
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    finally:
        try:
            await database.disconnect()
            logger.info("Database disconnected")
        except Exception as e:
            logger.error(f"Error disconnecting: {e}")

if __name__ == "__main__":
    asyncio.run(test_database_connection()) 