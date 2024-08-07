import asyncio
import json
import re
import uuid
from datetime import datetime, timedelta
from urllib.parse import quote
from uuid import UUID

import aiohttp
import logzero
import pandas as pd
import pyotp
import requests
from logzero import logger
from sqlalchemy import create_engine, text
from tenacity import retry, wait_fixed, stop_after_attempt

semaphore = asyncio.Semaphore(1)
logger.disabled = True

NAMESPACE_STOCK = UUID("233c16a9-0a91-4c9d-adda-8a496c63a1a3")
# NAMESPACE_TICKER = '3dbc5dc5-15ce-417c-b896-b0416a604dc2'
# NAMESPACE_CANDLESTICK = '4692ebad-cb8d-49ed-b91c-82facd1e2f93'

# Define the retry strategy
@retry(wait=wait_fixed(2), stop=stop_after_attempt(5))
async def fetch_data_with_retries(session, uplinkURL):
    async with session.get(uplinkURL) as response:
        if response.status == 200:
            return await response.json()
        else:
            response.raise_for_status()

def query_to_dataframe(query, connection):
    result = connection.execute(text(query))
    rows = result.fetchall()
    columns = result.keys()
    return pd.DataFrame(rows, columns=columns)


async def get_valid_instrument_tickdata(
    session, row, interval="1minute", fromDate="2024-07-26", toDate="2024-07-26"
):
    # url = 'https://api.upstox.com/v2/historical-candle/NSE_FO|134606/1minute/2024-07-11/2024-07-1'
    uplinkURL = f"https://api.upstox.com/v2/historical-candle/{row.instrument_key}/{interval}/{toDate}/{fromDate}"
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
            print(f'done with stock {row.name}, instrument {row.trading_symbol}')
            return df
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def generate_dates(year, month, sday, holidays, end_date):
    # Get the first and last day of the month
    start_date = datetime(year, month, sday)
    end_date = datetime.strptime(end_date, "%Y-%m-%d")
    # if month == 12:
    #     end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    # else:
    #     end_date = datetime(year, month + 1, 1) - timedelta(days=1)

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
    valid_dfs = await asyncio.gather(*tasks)
    valid_dfs = [df for df in valid_dfs if (df is not None)]
    if valid_dfs:
        candle_stick_df = pd.concat(valid_dfs, ignore_index=True)
    else:
        candle_stick_df = pd.DataFrame()
    return candle_stick_df


stock_names = sorted(
    [
        "AARTIIND",
        "ABB",
        "ABBOTINDIA",
        "ABCAPITAL",
        "ABFRL",
        "ACC",
        "ADANIENT",
        "ADANIPORTS",
        "ALKEM",
        "AMBUJACEM",
        "APOLLOHOSP",
        "APOLLOTYRE",
        "ASHOKLEY",
        "ASIANPAINT",
        "ASTRAL",
        "ATUL",
        "AUBANK",
        "AUROPHARMA",
        "AXISBANK",
        "BAJAJ-AUTO",
        "BAJAJFINSV",
        "BAJFINANCE",
        "BALKRISIND",
        "BALRAMCHIN",
        "BANDHANBNK",
        "BANKBARODA",
        "BATAINDIA",
        "BEL",
        "BERGEPAINT",
        "BHARATFORG",
        "BHARTIARTL",
        "BHEL",
        "BIOCON",
        "BOSCHLTD",
        "BPCL",
        "BRITANNIA",
        "BSOFT",
        "CANBK",
        "CANFINHOME",
        "CHAMBLFERT",
        "CHOLAFIN",
        "CIPLA",
        "COALINDIA",
        "COFORGE",
        "COLPAL",
        "CONCOR",
        "COROMANDEL",
        "CROMPTON",
        "CUB",
        "CUMMINSIND",
        "DABUR",
        "DALBHARAT",
        "DEEPAKNTR",
        "DIVISLAB",
        "DIXON",
        "DLF",
        "DRREDDY",
        "EICHERMOT",
        "ESCORTS",
        "EXIDEIND",
        "FEDERALBNK",
        "GAIL",
        "GLENMARK",
        "GMRINFRA",
        "GNFC",
        "GODREJCP",
        "GODREJPROP",
        "GRANULES",
        "GRASIM",
        "GUJGASLTD",
        "HAL",
        "HAVELLS",
        "HCLTECH",
        "HDFCAMC",
        "HDFCBANK",
        "HDFCLIFE",
        "HEROMOTOCO",
        "HINDALCO",
        "HINDCOPPER",
        "HINDPETRO",
        "HINDUNILVR",
        "ICICIBANK",
        "ICICIGI",
        "ICICIPRULI",
        "IDEA",
        "IDFC",
        "IDFCFIRSTB",
        "IEX",
        "IGL",
        "INDHOTEL",
        "INDIACEM",
        "INDIAMART",
        "INDIGO",
        "INDUSINDBK",
        "INDUSTOWER",
        "INFY",
        "IOC",
        "IPCALAB",
        "IRCTC",
        "ITC",
        "JINDALSTEL",
        "JKCEMENT",
        "JSWSTEEL",
        "JUBLFOOD",
        "KOTAKBANK",
        "LALPATHLAB",
        "LAURUSLABS",
        "LICHSGFIN",
        "LT",
        "LTF",
        "LTIM",
        "LTTS",
        "LUPIN",
        "M&M",
        "M&MFIN",
        "MANAPPURAM",
        "MARICO",
        "MARUTI",
        "MCX",
        "METROPOLIS",
        "MFSL",
        "MGL",
        "MOTHERSON",
        "MPHASIS",
        "MRF",
        "MUTHOOTFIN",
        "NATIONALUM",
        "NAUKRI",
        "NAVINFLUOR",
        "NESTLEIND",
        "NMDC",
        "NTPC",
        "OBEROIRLTY",
        "OFSS",
        "ONGC",
        "PAGEIND",
        "PEL",
        "PERSISTENT",
        "PETRONET",
        "PFC",
        "PIDILITIND",
        "PIIND",
        "PNB",
        "POLYCAB",
        "POWERGRID",
        "PVRINOX",
        "RAMCOCEM",
        "RBLBANK",
        "RECLTD",
        "RELIANCE",
        "SAIL",
        "SBICARD",
        "SBILIFE",
        "SBIN",
        "SHREECEM",
        "SHRIRAMFIN",
        "SIEMENS",
        "SRF",
        "SUNPHARMA",
        "SUNTV",
        "SYNGENE",
        "TATACHEM",
        "TATACOMM",
        "TATACONSUM",
        "TATAMOTORS",
        "TATAPOWER",
        "TATASTEEL",
        "TCS",
        "TECHM",
        "TITAN",
        "TORNTPHARM",
        "TRENT",
        "TVSMOTOR",
        "UBL",
        "ULTRACEMCO",
        "UNITDSPR",
        "UPL",
        "VEDL",
        "VOLTAS",
        "WIPRO",
        "ZYDUSLIFE",
    ],
    key=len,
    reverse=True,
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
    with open("NSE.json", "r") as file:
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

    nse_holidays_2024 = [
        "2024-01-26",  # Republic Day
        "2024-03-08",  # Mahashivratri
        "2024-03-25",  # Holi
        "2024-03-29",  # Good Friday
        "2024-04-11",  # Id-Ul-Fitr (Ramadan Eid)
        "2024-04-17",  # Shri Ram Navmi
        "2024-05-01",  # Maharashtra Day
        "2024-06-17",  # Bakri Id
        "2024-07-17",  # Moharram
        "2024-08-15",  # Independence Day/Parsi New Year
        "2024-10-02",  # Mahatma Gandhi Jayanti
        "2024-11-01",  # Diwali Laxmi Pujan (Muhurat Trading will be conducted)
        "2024-11-15",  # Gurunanak Jayanti
        "2024-12-25",  # Christmas
        "2024-04-14",  # Dr. Baba Saheb Ambedkar Jayanti (Sunday)
        "2024-04-21",  # Shri Mahavir Jayanti (Sunday)
        "2024-09-07",  # Ganesh Chaturthi (Saturday)
        "2024-10-12",  # Dussehra (Saturday)
        "2024-11-02",  # Diwali-Balipratipada (Saturday)
    ]
    ticker_df = pd.DataFrame([])

    dates = generate_dates(2024, 8, 6, nse_holidays_2024, "2024-08-06")

    # year = 2024
    # month = 7
    # end_date = 2024-07-02

    instrument_df = pd.DataFrame(instrument_data)
    instrument_df["stock_id"] = instrument_df["trading_symbol"].apply(
        lambda x: tbl_stock[tbl_stock["name"] == x.split()[0]].iloc[0]["id"]
    )
    instrument_df["id"] = [
        uuid.uuid5(NAMESPACE_STOCK, str(r.stock_id) + r.trading_symbol)
        for r in instrument_df.itertuples(index=False)
    ]
    instrument_df["expiry_epoch"] = instrument_df["expiry"]
    instrument_df["expiry"] = instrument_df["expiry_epoch"].apply(
        lambda x: convert_epoch_to_date(x)
    )
    print('Instrument_data processed')

    async with aiohttp.ClientSession() as session:
        tasks = [process_instrument(instrument_df, session, date) for date in dates]
        results = await asyncio.gather(*tasks)

        for candle_stick_df in results:
            ticker_df = pd.concat(
                [ticker_df, candle_stick_df], ignore_index=True
            )

    print("Processing complete.")
    # instrument_df.set_index("id", inplace=True)
    # instrument_df.to_sql(
    #     "instrument", schema="options", if_exists="append", con=engine, index=True
    # )
    ticker_df.set_index("id", inplace=True)
    ticker_df.to_sql(
        "ticker", schema="options", if_exists="append", con=engine, index=True
    )
    print("Pushed to DB.")


asyncio.run(main())
