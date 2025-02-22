import asyncio
import json
import re
import uuid
import time
import pandas as pd
import aiohttp
from datetime import datetime
from urllib.parse import quote
from uuid import UUID
from sqlalchemy import create_engine, text
from tqdm.asyncio import tqdm_asyncio
import tqdm
import io
import csv
from aiohttp import ClientError
from sqlalchemy.exc import IntegrityError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)

# Global constants
NAMESPACE_STOCK = UUID("233c16a9-0a91-4c9d-adda-8a496c63a1a3")
semaphore = asyncio.Semaphore(10)  # Control concurrency
DB_CONNECTION_STRING = (
    "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
    % quote("sdadmin@postgres")
)


# Retry strategy for HTTP requests
@retry(
    wait=wait_exponential(multiplier=1, min=2, max=10),
    stop=stop_after_attempt(10),
    retry=retry_if_exception_type(ClientError),
)
async def fetch_data_with_retries(session, url):
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()  # Raise error for bad status codes
                return await response.json()
        except ClientError as e:
            print(f"ClientError fetching {url}: {e}")
            raise
        except RetryError:
            print(f"Retries exhausted for {url}")
            return None


def query_to_dataframe(query, connection):
    """Fetch SQL query result into Pandas DataFrame."""
    return pd.read_sql(query, connection)


async def get_valid_instrument_tickdata(
    session, row, interval="1minute", fromDate="2025-01-01", toDate="2025-01-21"
):
    """Fetch historical candlestick data for a given instrument."""
    url = f"https://api.upstox.com/v2/historical-candle/{row.instrument_key}/{interval}/{toDate}/{fromDate}"
    try:
        res = await fetch_data_with_retries(session, url)
        if not res or "data" not in res or "candles" not in res["data"]:
            return None

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
            df["id"] = df.apply(
                lambda r: uuid.uuid5(NAMESPACE_STOCK, f"{row.id}{r.time_stamp}"), axis=1
            )
            df["instrument_id"] = row.id
        return df
    except Exception as e:
        print(f"Error fetching data for {row.instrument_key}: {e}")
        return None


def generate_dates(year, month, start_day, holidays, end_date):
    """Generate valid trading dates, excluding weekends and holidays."""
    start_date = datetime(year, month, start_day)
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    all_dates = pd.date_range(start=start_date, end=end_date)

    return [
        d.strftime("%Y-%m-%d")
        for d in all_dates
        if d.weekday() < 5 and d.strftime("%Y-%m-%d") not in holidays
    ]


def convert_epoch_to_date(epoch_ms):
    """Convert epoch timestamp (ms) to 'YYYY-MM-DD'."""
    return datetime.fromtimestamp(epoch_ms / 1000).strftime("%Y-%m-%d")


async def process_instrument(instrument_df, session, date):
    """Fetch tick data for all instruments asynchronously for a given date."""
    tasks = [
        asyncio.create_task(
            get_valid_instrument_tickdata(session, row, "1minute", date, date)
        )
        for row in instrument_df.itertuples(index=False)
    ]

    valid_dfs = []
    for future in tqdm_asyncio(
        asyncio.as_completed(tasks), total=len(tasks), desc=f"Processing {date}"
    ):
        df = await future
        if df is not None and not df.empty:
            valid_dfs.append(df)

    return pd.concat(valid_dfs, ignore_index=True) if valid_dfs else pd.DataFrame()


def get_id(x, tbl_stock):
    """Fetch stock ID based on trading symbol."""
    name = x.split()[0]
    return (
        tbl_stock.loc[tbl_stock["name"] == name, "id"].values[0]
        if name in tbl_stock["name"].values
        else None
    )


# import chunks
def write_to_sql_with_progress(
    df, table_name, engine, schema="options", chunksize=10000
):
    """Write a DataFrame to SQL database in chunks with progress tracking."""
    if df.empty:
        return
    with engine.begin() as conn:
        for start in tqdm.tqdm(
            range(0, len(df), chunksize),
            total=(len(df) // chunksize) + 1,
            desc=f"Writing {table_name}",
        ):
            df.iloc[start : start + chunksize].to_sql(
                table_name,
                schema=schema,
                con=conn,
                if_exists="append",
                index=True,
                method="multi",
            )


# bulk import
def write_to_sql_postgres(df, table_name, engine, schema="options"):
    with engine.begin() as conn:
        output = io.StringIO()
        df.to_csv(output, sep=",", index=False, header=False, quoting=csv.QUOTE_MINIMAL)
        output.seek(0)
        columns = ",".join(df.columns)
        sql = f"COPY {schema}.{table_name} ({columns}) FROM STDIN WITH CSV"

        with conn.connection.cursor() as cur:
            cur.copy_expert(sql, output)  # High-speed bulk insert


async def main():
    """Main execution pipeline."""
    engine = create_engine(DB_CONNECTION_STRING)

    # Load instrument & stock data
    with engine.connect() as conn:
        tbl_stock = query_to_dataframe("SELECT * FROM options.stock", conn)

    with open("/home/cgraaaj/Projects/cgr-trades/python/NSE.json", "r") as file:
        data = json.load(file)

    # Filter out irrelevant instruments
    pattern = re.compile(r"\d+NSETEST$")
    instrument_data = [
        item
        for item in data
        if item["segment"] == "NSE_FO"
        and item["name"]
        not in {"BANKNIFTY", "NIFTY", "FINNIFTY", "MIDCPNIFTY", "NIFTYNXT50"}
        and not pattern.search(item.get("name", ""))
    ]

    # Define trading holidays
    nse_holidays_2025 = [
        "2025-01-26",
        "2025-02-26",
        "2025-03-14",
        "2025-03-31",
        "2025-04-06",
        "2025-04-10",
        "2025-04-14",
        "2025-04-18",
        "2025-05-01",
        "2025-06-07",
        "2025-07-06",
        "2025-08-15",
        "2025-08-27",
        "2025-10-02",
        "2025-10-21",
        "2025-10-22",
        "2025-11-05",
        "2025-12-25",
    ]
    dates = generate_dates(2025, 2, 21, nse_holidays_2025, "2025-02-21")

    instrument_df = pd.DataFrame(instrument_data)
    instrument_df["stock_id"] = instrument_df["trading_symbol"].apply(
        lambda x: get_id(x, tbl_stock)
    )
    instrument_df["id"] = instrument_df.apply(
        lambda r: uuid.uuid5(NAMESPACE_STOCK, f"{r.stock_id}{r.trading_symbol}"), axis=1
    )
    instrument_df["expiry"] = instrument_df["expiry"].apply(convert_epoch_to_date)

    print("Instrument data processed")

    async with aiohttp.ClientSession() as session:
        tasks = [process_instrument(instrument_df, session, date) for date in dates]
        results = await asyncio.gather(*tasks)

    ticker_df = pd.concat(results, ignore_index=True) if results else pd.DataFrame()

    # Store new instruments
    existing_ids = pd.read_sql("SELECT id FROM options.instrument", engine)["id"]
    instrument_df_filtered = instrument_df[~instrument_df["id"].isin(existing_ids)]
    instrument_df_filtered.set_index("id", inplace=True)

    if not instrument_df_filtered.empty:
        instrument_df_filtered.to_sql(
            "instrument", schema="options", con=engine, if_exists="append", index=True
        )
        print("Instrument data pushed to DB.")

    # Store ticker data
    ticker_df.set_index("id", inplace=True)
    write_to_sql_with_progress(ticker_df, "ticker", engine)
    print("Ticker data pushed to DB.")


if __name__ == "__main__":
    start_time = time.time()
    asyncio.run(main())
    print(f"Execution time: {(time.time() - start_time) / 60:.2f} minutes")
