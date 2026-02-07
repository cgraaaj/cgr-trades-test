#!/usr/bin/env python3
"""
Ticker Data Migration Script
Migrates options.ticker data from Docker PostgreSQL to VM PostgreSQL server.

Source: 192.168.1.72:5430 (Docker)
Destination: 192.168.1.40:5432 (VM)

Usage:
    python migrate_ticker_data.py --verify          # Verify data counts only
    python migrate_ticker_data.py --migrate         # Perform actual migration
    python migrate_ticker_data.py --migrate --batch-size 50000  # Custom batch size
"""

import sys
import argparse
import logging
from datetime import datetime
from urllib.parse import quote

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'logs/migration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Database connection configurations
SOURCE_DB = {
    "host": "192.168.1.72",
    "port": "5430",
    "database": "stock-dumps",
    "user": "sd_admin",
    "password": "sdadmin@postgres"
}

DEST_DB = {
    "host": "192.168.1.40",
    "port": "5432",
    "database": "stock-dumps",
    "user": "sd_admin",
    "password": "sdadmin@postgres"  # Update if different
}

# Migration cutoff - data after this timestamp will be migrated
CUTOFF_TIMESTAMP = "2026-01-02 15:30:00"


def get_connection_string(db_config: dict) -> str:
    """Generate SQLAlchemy connection string."""
    encoded_password = quote(db_config["password"])
    return (
        f"postgresql+psycopg2://{db_config['user']}:{encoded_password}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )


def create_engines():
    """Create database engines for source and destination."""
    source_engine = create_engine(
        get_connection_string(SOURCE_DB),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )
    dest_engine = create_engine(
        get_connection_string(DEST_DB),
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10
    )
    return source_engine, dest_engine


def verify_connections(source_engine, dest_engine):
    """Verify both database connections are working."""
    logger.info("Verifying database connections...")
    
    try:
        with source_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            logger.info(f"✓ Source DB (192.168.1.72:5430) - Connected")
    except Exception as e:
        logger.error(f"✗ Source DB connection failed: {e}")
        return False
    
    try:
        with dest_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).fetchone()
            logger.info(f"✓ Destination DB (192.168.1.40:5432) - Connected")
    except Exception as e:
        logger.error(f"✗ Destination DB connection failed: {e}")
        return False
    
    return True


def get_data_stats(engine, label: str, cutoff: str = None):
    """Get statistics about ticker data."""
    with engine.connect() as conn:
        # Total rows
        total_query = "SELECT COUNT(*) FROM options.ticker"
        total_rows = conn.execute(text(total_query)).scalar()
        
        # Min/Max dates
        date_query = """
            SELECT 
                MIN(created_on) as min_date,
                MAX(created_on) as max_date
            FROM options.ticker
        """
        date_result = conn.execute(text(date_query)).fetchone()
        
        # Rows after cutoff (if specified)
        rows_after_cutoff = None
        if cutoff:
            cutoff_query = f"""
                SELECT COUNT(*) 
                FROM options.ticker 
                WHERE created_on > '{cutoff}'
            """
            rows_after_cutoff = conn.execute(text(cutoff_query)).scalar()
        
        return {
            "label": label,
            "total_rows": total_rows,
            "min_date": date_result[0],
            "max_date": date_result[1],
            "rows_after_cutoff": rows_after_cutoff
        }


def verify_data(source_engine, dest_engine, cutoff: str = CUTOFF_TIMESTAMP):
    """Verify and compare data between source and destination."""
    logger.info("=" * 60)
    logger.info("DATA VERIFICATION REPORT")
    logger.info("=" * 60)
    logger.info(f"Cutoff timestamp: {cutoff}")
    logger.info("")
    
    # Get source stats
    logger.info("Fetching source database stats...")
    source_stats = get_data_stats(source_engine, "Source (Docker)", cutoff)
    
    # Get destination stats
    logger.info("Fetching destination database stats...")
    dest_stats = get_data_stats(dest_engine, "Destination (VM)", cutoff)
    
    # Display results
    logger.info("")
    logger.info("-" * 60)
    logger.info("SOURCE DATABASE (192.168.1.72:5430)")
    logger.info("-" * 60)
    logger.info(f"  Total rows:           {source_stats['total_rows']:,}")
    logger.info(f"  Date range:           {source_stats['min_date']} to {source_stats['max_date']}")
    logger.info(f"  Rows after cutoff:    {source_stats['rows_after_cutoff']:,}")
    
    logger.info("")
    logger.info("-" * 60)
    logger.info("DESTINATION DATABASE (192.168.1.40:5432)")
    logger.info("-" * 60)
    logger.info(f"  Total rows:           {dest_stats['total_rows']:,}")
    logger.info(f"  Date range:           {dest_stats['min_date']} to {dest_stats['max_date']}")
    logger.info(f"  Rows after cutoff:    {dest_stats['rows_after_cutoff']:,}")
    
    logger.info("")
    logger.info("-" * 60)
    logger.info("MIGRATION SUMMARY")
    logger.info("-" * 60)
    
    rows_to_migrate = source_stats['rows_after_cutoff'] - (dest_stats['rows_after_cutoff'] or 0)
    logger.info(f"  Rows to migrate:      {rows_to_migrate:,}")
    logger.info(f"  (from source after {CUTOFF_TIMESTAMP})")
    
    # Check for potential duplicates
    if dest_stats['rows_after_cutoff'] and dest_stats['rows_after_cutoff'] > 0:
        logger.warning(f"  ⚠ Destination already has {dest_stats['rows_after_cutoff']:,} rows after cutoff")
        logger.warning("  Consider using --skip-existing flag or verify data integrity")
    
    logger.info("=" * 60)
    
    return source_stats, dest_stats


def get_migration_date_ranges(source_engine, cutoff: str):
    """Get distinct dates to migrate for progress tracking."""
    query = f"""
        SELECT DISTINCT DATE(created_on) as trade_date, COUNT(*) as row_count
        FROM options.ticker
        WHERE created_on > '{cutoff}'
        GROUP BY DATE(created_on)
        ORDER BY trade_date
    """
    with source_engine.connect() as conn:
        result = pd.read_sql(query, conn)
    return result


def migrate_data(source_engine, dest_engine, cutoff: str = CUTOFF_TIMESTAMP, batch_size: int = 100000, skip_existing: bool = False):
    """Migrate ticker data from source to destination."""
    logger.info("=" * 60)
    logger.info("STARTING DATA MIGRATION")
    logger.info("=" * 60)
    logger.info(f"Batch size: {batch_size:,}")
    logger.info(f"Skip existing: {skip_existing}")
    logger.info("")
    
    # Get date ranges for migration
    date_ranges = get_migration_date_ranges(source_engine, cutoff)
    logger.info(f"Found {len(date_ranges)} dates to migrate:")
    for _, row in date_ranges.iterrows():
        logger.info(f"  {row['trade_date']}: {row['row_count']:,} rows")
    
    total_rows = date_ranges['row_count'].sum()
    logger.info(f"\nTotal rows to migrate: {total_rows:,}")
    
    # Confirm migration
    logger.info("")
    logger.info("Starting migration in 5 seconds... (Ctrl+C to cancel)")
    import time
    time.sleep(5)
    
    migrated_total = 0
    start_time = datetime.now()
    
    # Migrate date by date for better control
    for _, date_row in date_ranges.iterrows():
        trade_date = date_row['trade_date']
        expected_rows = date_row['row_count']
        
        logger.info(f"\nProcessing date: {trade_date} ({expected_rows:,} rows)")
        
        # Check if data already exists in destination
        if skip_existing:
            with dest_engine.connect() as conn:
                existing = conn.execute(text(f"""
                    SELECT COUNT(*) FROM options.ticker 
                    WHERE DATE(created_on) = '{trade_date}'
                """)).scalar()
                if existing > 0:
                    logger.info(f"  Skipping - {existing:,} rows already exist")
                    continue
        
        # Fetch data for this date
        query = f"""
            SELECT * FROM options.ticker
            WHERE created_on > '{cutoff}'
            AND DATE(created_on) = '{trade_date}'
            ORDER BY created_on
        """
        
        offset = 0
        date_migrated = 0
        
        with tqdm(total=expected_rows, desc=f"  {trade_date}", unit="rows") as pbar:
            while True:
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                
                with source_engine.connect() as conn:
                    df = pd.read_sql(batch_query, conn)
                
                if df.empty:
                    break
                
                # Insert into destination
                with dest_engine.begin() as conn:
                    df.to_sql(
                        "ticker",
                        schema="options",
                        con=conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=5000
                    )
                
                rows_inserted = len(df)
                date_migrated += rows_inserted
                migrated_total += rows_inserted
                offset += batch_size
                pbar.update(rows_inserted)
                
                if rows_inserted < batch_size:
                    break
        
        logger.info(f"  Completed: {date_migrated:,} rows migrated")
    
    # Final summary
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info("")
    logger.info("=" * 60)
    logger.info("MIGRATION COMPLETED")
    logger.info("=" * 60)
    logger.info(f"Total rows migrated: {migrated_total:,}")
    logger.info(f"Total time: {elapsed:.1f} seconds ({elapsed/60:.1f} minutes)")
    logger.info(f"Average speed: {migrated_total/elapsed:.0f} rows/second")
    
    return migrated_total


def main():
    parser = argparse.ArgumentParser(
        description="Migrate ticker data between PostgreSQL servers"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Only verify data counts, don't migrate"
    )
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Perform actual data migration"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Number of rows per batch (default: 100000)"
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip dates that already have data in destination"
    )
    parser.add_argument(
        "--cutoff",
        type=str,
        default=CUTOFF_TIMESTAMP,
        help=f"Cutoff timestamp (default: {CUTOFF_TIMESTAMP})"
    )
    
    args = parser.parse_args()
    
    # Update cutoff if provided
    cutoff_ts = args.cutoff
    
    if not args.verify and not args.migrate:
        parser.print_help()
        logger.info("\nPlease specify --verify or --migrate")
        sys.exit(1)
    
    # Create database engines
    logger.info("Initializing database connections...")
    source_engine, dest_engine = create_engines()
    
    # Verify connections
    if not verify_connections(source_engine, dest_engine):
        logger.error("Database connection verification failed!")
        sys.exit(1)
    
    # Verify data
    source_stats, dest_stats = verify_data(source_engine, dest_engine, cutoff_ts)
    
    # Migrate if requested
    if args.migrate:
        if source_stats['rows_after_cutoff'] == 0:
            logger.info("No rows to migrate!")
            sys.exit(0)
        
        migrate_data(
            source_engine,
            dest_engine,
            cutoff=cutoff_ts,
            batch_size=args.batch_size,
            skip_existing=args.skip_existing
        )
    else:
        logger.info("")
        logger.info("To perform migration, run with --migrate flag:")
        logger.info(f"  python {sys.argv[0]} --migrate")


if __name__ == "__main__":
    main()

