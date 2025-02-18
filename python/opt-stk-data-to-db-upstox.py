import asyncio
import json
import re
import uuid
from datetime import datetime, timedelta
from urllib.parse import quote
from uuid import UUID
from aiohttp import ClientResponseError
from sqlalchemy.exc import IntegrityError
import time
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
import aiohttp
import logzero
import pandas as pd
import pyotp
import requests
from logzero import logger
from sqlalchemy import create_engine, text
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)
from aiohttp import ClientError

semaphore = asyncio.Semaphore(1)
logger.disabled = True

NAMESPACE_STOCK = UUID("233c16a9-0a91-4c9d-adda-8a496c63a1a3")
# NAMESPACE_TICKER = '3dbc5dc5-15ce-417c-b896-b0416a604dc2'
# NAMESPACE_CANDLESTICK = '4692ebad-cb8d-49ed-b91c-82facd1e2f93'


# Define the retry strategy
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(ClientError),
)
async def fetch_data_with_retries(session, uplinkURL):
    async with semaphore:
        try:
            async with session.get(uplinkURL) as response:
                if 500 <= response.status < 600:  # Retry on server-side errors (5xx)
                    raise aiohttp.ClientError(f"Server error: {response.status}")
                if response.status == 200:  # Successful response
                    return await response.json()
                else:
                    response.raise_for_status()  # For client-side errors (4xx)

        except ClientError as ce:  # Catch network-related errors
            print(f"ClientError occurred during fetching data: {ce}")
            raise  # Optionally re-raise or handle differently

        except RetryError as re:  # Catch errors after all retries are exhausted
            print(f"All retry attempts failed: {re}")
            raise  # Optionally re-raise or handle differently


def query_to_dataframe(query, connection):
    result = connection.execute(text(query))
    rows = result.fetchall()
    columns = result.keys()
    return pd.DataFrame(rows, columns=columns)


async def get_valid_instrument_tickdata(
    session, row, interval="1minute", fromDate="2025-01-01", toDate="2025-01-21"
):
    # url = 'https://api.upstox.com/v2/historical-candle/NSE_FO|134606/1minute/2024-07-11/2024-07-1'
    uplinkURL = f"https://api.upstox.com/v2/historical-candle/{row.instrument_key}/{interval}/{toDate}/{fromDate}"
    # print("\n\ntrying stock:\n",row.instrument_key,"\n",interval,"\n",toDate,"\n",fromDate)
    try:
        res = await fetch_data_with_retries(session, uplinkURL)
        df = pd.DataFrame(
            res["data"]["candles"],
            columns=[
                "time_stamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "open_interest",
            ],
        )
        if not df.empty:
            df["id"] = [
                uuid.uuid5(NAMESPACE_STOCK, str(row.id) + r.time_stamp)
                for r in df.itertuples(index=False)
            ]
            df["instrument_id"] = row.id
            # print(f"done with stock {row.name}, instrument {row.trading_symbol}")
            return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def generate_dates(year, month, sday, holidays, end_date):
    # Get the first and last day of the month
    start_date = datetime(year, month, sday)
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate all dates in the month
    all_dates = pd.date_range(start=start_date, end=end_date).tolist()

    # Filter out weekends and holidays
    valid_dates = [
        date
        for date in all_dates
        if date.weekday() < 5 and date.strftime("%Y-%m-%d") not in holidays
    ]

    # Format the dates as 'yyyy-mm-dd'
    formatted_dates = [date.strftime("%Y-%m-%d") for date in valid_dates]

    return formatted_dates


def convert_epoch_to_date(epoch_time_ms):
    """
    Convert epoch time in milliseconds to a formatted date string.

    Parameters:
    epoch_time_ms (int): Epoch time in milliseconds.

    Returns
    str: Formatted date string in "%Y-%m-%d" format.
    """
    # Convert epoch time to seconds
    epoch_time_s = epoch_time_ms / 1000

    # Create a datetime object from the epoch time
    date_time = datetime.fromtimestamp(epoch_time_s)

    # Format the datetime object to a string in "%Y-%m-%d" format
    formatted_date = date_time.strftime("%Y-%m-%d")

    return formatted_date


async def process_instrument(instrument_df, session, date):
    tasks = [
        get_valid_instrument_tickdata(session, row, "1minute", date, date)
        for row in instrument_df.itertuples(index=False)
    ]

    valid_dfs = []
    
    # Use tqdm to show progress
    for future in tqdm_asyncio(asyncio.as_completed(tasks), total=len(tasks), desc="Processing instruments"):
        df = await future
        if df is not None:
            valid_dfs.append(df)
    
    candle_stick_df = pd.concat(valid_dfs, ignore_index=True) if valid_dfs else pd.DataFrame()
    
    return candle_stick_df

def get_id(x, tbl_stock):
    name = x.split()[0]
    filtered_df = tbl_stock[tbl_stock["name"] == name]
    return filtered_df.iloc[0]["id"]

def write_to_sql_with_progress(df, table_name, engine, schema="options", chunksize=5000):
    total_chunks = (len(df) // chunksize) + 1  # Calculate number of chunks
    
    with engine.begin() as conn:  # Ensures transaction safety
        for i, chunk in enumerate(tqdm(range(0, len(df), chunksize), total=total_chunks, desc="Writing to SQL")):
            df.iloc[chunk : chunk + chunksize].to_sql(
                table_name,
                schema=schema,
                if_exists="append",
                con=conn,
                index=True,
                method="multi"  # Optimizes batch inserts
            )

async def main():
    # Create a PostgreSQL engine
    engine = create_engine(
        "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
        % quote("sdadmin@postgres")
    )

    with engine.connect() as connection:
        # Query to select all from the 'stock' table
        tbl_stock = query_to_dataframe("SELECT * FROM options.stock", connection)

        # Query to select all from another table, e.g., 'another_table'
        # another_table_df = query_to_dataframe("SELECT * FROM another_table", connection)
    with open("/home/cgraaaj/Projects/cgr-trades/python/NSE.json", "r") as file:
        data = json.load(file)

    # Define the regex pattern for names ending with "NSETEST" preceded by numbers
    pattern = re.compile(r"\d+NSETEST$")
    # Filter objects where "segment" is "NSE_FO"
    instrument_data = [
        item
        for item in data
        if item.get("segment") == "NSE_FO"
        and item.get("name")
        not in ["BANKNIFTY", "NIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"]
        and not pattern.search(item.get("name", ""))
    ]

    nse_holidays_2025 = [
            "2025-01-26",  # Republic Day (Sunday)
            "2025-02-26",  # Mahashivratri
            "2025-03-14",  # Holi
            "2025-03-31",  # Id-Ul-Fitr (Ramzan Id)
            "2025-04-06",  # Shri Ram Navami (Sunday)
            "2025-04-10",  # Shri Mahavir Jayanti
            "2025-04-14",  # Dr. Baba Saheb Ambedkar Jayanti
            "2025-04-18",  # Good Friday
            "2025-05-01",  # Maharashtra Day
            "2025-06-07",  # Bakri Id (Saturday)
            "2025-07-06",  # Muharram (Sunday)
            "2025-08-15",  # Independence Day
            "2025-08-27",  # Ganesh Chaturthi
            "2025-10-02",  # Mahatma Gandhi Jayanti/Dussehra
            "2025-10-21",  # Diwali Laxmi Pujan (Muhurat Trading will be conducted)
            "2025-10-22",  # Diwali Balipratipada
            "2025-11-05",  # Prakash Gurpurb (Guru Nanak Jayanti)
            "2025-12-25",  # Christmas
    ]
    ticker_df = pd.DataFrame([])

    # year = 2024
    # month = 7
    # end_date = 2024-07-02
    dates = generate_dates(2025, 2, 10, nse_holidays_2025, "2025-02-14")


    instrument_df = pd.DataFrame(instrument_data)
    # instrument_df["stock_id"] = instrument_df["trading_symbol"].apply(
    #     lambda x: tbl_stock[tbl_stock["name"] == x.split()[0]].iloc[0]["id"]
    # )

    instrument_df["stock_id"] = instrument_df["trading_symbol"].apply(lambda x: get_id(x, tbl_stock))

    # print("Missing names:", missing_names)

    instrument_df["id"] = [
        uuid.uuid5(NAMESPACE_STOCK, str(r.stock_id) + r.trading_symbol)
        for r in instrument_df.itertuples(index=False)
    ]
    instrument_df["expiry_epoch"] = instrument_df["expiry"]
    instrument_df["expiry"] = instrument_df["expiry_epoch"].apply(
        lambda x: convert_epoch_to_date(x)
    )
    print("Instrument_data processed")

    async with aiohttp.ClientSession() as session:
        tasks = [process_instrument(instrument_df, session, date) for date in dates]
        results = await asyncio.gather(*tasks)

        for candle_stick_df in results:
            ticker_df = pd.concat([ticker_df, candle_stick_df], ignore_index=True)

    print("Processing complete.")
    existing_ids = pd.read_sql("SELECT id FROM options.instrument", engine)['id']
    instrument_df_filtered = instrument_df[~instrument_df['id'].isin(existing_ids)]
    instrument_df_filtered.set_index("id", inplace=True)

    if not instrument_df_filtered.empty:
        # Insert only new data
        instrument_df_filtered.to_sql(
            "instrument", schema="options", if_exists="append", con=engine, index=True
        )

    print("Instrument Pushed to DB.")
    ticker_df.set_index("id", inplace=True)
    write_to_sql_with_progress(ticker_df, "ticker", engine)
    print("Ticker Pushed to DB.")



if __name__ == "__main__":
    start_time = time.time()  # Record the start time
    asyncio.run(main())  # Run your main async function
    end_time = time.time()  # Record the end time

    execution_time_seconds = end_time - start_time  # Calculate the execution time
    execution_time_minutes = execution_time_seconds / 60  # Convert to minutes
    print(f"Execution time: {execution_time_minutes:.2f} minutes")
