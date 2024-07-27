import asyncio
import json
from datetime import datetime, timedelta
from urllib.parse import quote
import re

import aiohttp
import pandas as pd
import pyotp
import requests
import logzero
from logzero import logger
from SmartApi.smartConnect import SmartConnect
from sqlalchemy import create_engine

semaphore = asyncio.Semaphore(1)
logger.disabled = True
missed_stocks = []


async def get_instrument_data(smartApi, exchange, searchscrip):
    async with semaphore:
        await asyncio.sleep(1)  # Adding delay to simulate throttling
        searchScripData = smartApi.searchScrip(exchange, searchscrip)
        # print(searchScripData)
        return searchScripData


async def get_valid_instrument_tickdata(
    session, symbolToken, interval="1minute", fromDate="2024-07-01", toDate="2024-07-01"
):
    # url = 'https://api.upstox.com/v2/historical-candle/NSE_FO|134606/1minute/2024-07-11/2024-07-1'
    uplinkURL = f"https://api.upstox.com/v2/historical-candle/NSE_FO|{symbolToken}/{interval}/{toDate}/{fromDate}"
    async with session.get(uplinkURL) as response:
        if response.status == 200:
            # await asyncio.sleep(1)  # Adding delay to simulate throttling
            res = await response.json()
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
                df.insert(1, "ticker_id", symbolToken + "-" + fromDate)
                df["cs_id"] = df[["ticker_id", "time_stamp"]].agg("-".join, axis=1)
                return df


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


# Define a function to extract the components
def extract_components(instrument):
    pattern = r'^(\d{4}-\d{2}-\d{2})?-?([A-Z-&]+)(\d{2}[A-Z]{3}\d{2})(\d+(\.\d+)?)([A-Z]+)$'
    match = re.match(pattern, instrument)
    if match:
        trade_date = match.group(1)
        stock_name = match.group(2)
        expiry_date = match.group(3)
        strike_price = match.group(4)
        option_type = match.group(6)
        return pd.Series(
            [trade_date, stock_name, expiry_date, strike_price, option_type]
        )

    # return pd.Series([None, None, None, None, None])

def categorize_tradingsymbol(row, stock_names):
    for name in stock_names:
        if re.match(f"^{name}", row['tradingsymbol']):
            return name
    return None

async def process_stock(smartApi, session, stock, date):
    print(f"Dumping {stock} on {date}")
    instrumentData = await get_instrument_data(smartApi, "NFO", stock)
    df = pd.DataFrame(
        instrumentData["data"], columns=["exchange", "tradingsymbol", "symboltoken"]
    )
    if not df.empty:
        df['stock_name'] = df.apply(lambda row: categorize_tradingsymbol(row, stock_names), axis=1)
        curr_mon_df = df[
            df["tradingsymbol"].str.contains("JUL")
            & ~df["tradingsymbol"].str.contains("FUT")
        ]
        curr_mon_df = curr_mon_df.loc[curr_mon_df["stock_name"]== stock]
        tasks = [
            get_valid_instrument_tickdata(session, symbolToken, "1minute", date, date)
            for symbolToken in curr_mon_df["symboltoken"]
        ]
        valid_dfs = await asyncio.gather(*tasks)

        valid_dfs = [df for df in valid_dfs if (df is not None)]
        if valid_dfs:
            res_candle_stick_df = pd.concat(valid_dfs, ignore_index=True)
        else:
            res_candle_stick_df = pd.DataFrame()

        curr_mon_df["tradingsymbol"] = curr_mon_df["tradingsymbol"].apply(
            lambda x: date + "-" + x
        )
        res_ticker_df = curr_mon_df
        print(f"Done {stock} on {date}")
        return res_candle_stick_df, res_ticker_df
    else:
        missed_stocks.append(stock)
        return pd.DataFrame(), pd.DataFrame()


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
        "MCDOWELL-N",
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

# ticker_df = pd.DataFrame(
#     columns=[
#         "ticker_id",
#         "trade_date",
#         "stock_name",
#         "expiry_date",
#         "strike_price",
#         "option_type",
#     ]
# )
# ticker_df["ticker_id"] = res_ticker_df["symboltoken"]
# ticker_df[
#     ["trade_date", "stock_name", "expiry_date", "strike_price", "option_type"]
# ] = res_ticker_df["tradingsymbol"].apply(extract_components)
# ticker_df["ticker_id"] = ticker_df[["ticker_id", "trade_date"]].agg("-".join, axis=1)


async def main():
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
    res_candle_stick_df = pd.DataFrame([])
    res_ticker_df = pd.DataFrame([])

    dates = generate_dates(2024, 7, 1, nse_holidays_2024, "2024-07-01")

    api_key = "BP42pHUk"
    username = "R60865380"
    pwd = "6713"
    smartApi = SmartConnect(api_key)

    # year = 2024
    # month = 7
    # end_date = 2024-07-02
    try:
        token = "ARA7CTUQBDWODFCWKVSAXTLR4U"
        totp = pyotp.TOTP(token).now()
    except Exception as e:
        logger.error("Invalid Token: The provided token is not valid.")
        raise e

    correlation_id = "abcde"
    data = smartApi.generateSession(username, pwd, totp)

    if data["status"] is False:
        logger.error(data)
    else:
        authToken = data["data"]["jwtToken"]
        refreshToken = data["data"]["refreshToken"]
        feedToken = smartApi.getfeedToken()
        res = smartApi.getProfile(refreshToken)
        smartApi.generateToken(refreshToken)
        res = res["data"]["exchanges"]
    # for stock in stock_names
    async with aiohttp.ClientSession() as session:
        tasks = [process_stock(smartApi, session, stock, date) for date in dates for stock in stock_names[:1]]
        results = await asyncio.gather(*tasks)

        for candle_stick_df, ticker_df in results:
            res_candle_stick_df = pd.concat(
                [res_candle_stick_df, candle_stick_df], ignore_index=True
            )
            res_ticker_df = pd.concat([res_ticker_df, ticker_df], ignore_index=True)

    # Save or process the res_candle_stick_df and res_ticker_df as needed

    # DB ACTIVITY
    # print(res_ticker_df)
    # print(candle_stick_df)

    # Create a PostgreSQL engine
    engine = create_engine(
        "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
        % quote("sdadmin@postgres")
    )
    # Save the DataFrame to a table named 'optstkdata'
    # curr_mon_df.to_sql('optstkdata',if_exists='append', con=engine, index=True)
    ticker_df = pd.DataFrame(
        columns=[
            "ticker_id",
            "trade_date",
            "stock_name",
            "expiry_date",
            "strike_price",
            "option_type",
        ]
    )
    ticker_df[
        ["trade_date", "stock_name", "expiry_date", "strike_price", "option_type"]
    ] = res_ticker_df["tradingsymbol"].apply(extract_components)
    ticker_df["ticker_id"] = res_ticker_df["symboltoken"]
    ticker_df["ticker_id"] = ticker_df[["ticker_id", "trade_date"]].agg(
        "-".join, axis=1
    )
    print("Processing complete.")
    test_ticker_df = ticker_df[ticker_df.duplicated(['ticker_id'], keep=False)]
    print(test_ticker_df)
    test_res_candle_stick_df = res_candle_stick_df[res_candle_stick_df.duplicated(['ticker_id'], keep=False)]
    print(test_res_candle_stick_df)
    # print('stocks that are missed')
    # print(missed_stocks)
    # ticker_df.to_sql(
    #     "ticker", schema="options", if_exists="append", con=engine, index=False
    # )
    # res_candle_stick_df.to_sql(
    #     "candle_stick", schema="options", if_exists="append", con=engine, index=False
    # )
    # print("Pushed to DB.")


asyncio.run(main())


# MCDOWELL-N is not getting any options data from smartapi
# [I 240725 11:14:48 smartConnect:488] Search successful. No matching trading symbols found for the given query.
