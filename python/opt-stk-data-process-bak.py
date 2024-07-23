import json
from datetime import datetime, timedelta
from urllib.parse import quote

import pandas as pd
import pyotp
import requests
from logzero import logger
from SmartApi.smartConnect import SmartConnect
from sqlalchemy import create_engine

api_key = "BP42pHUk"
username = "R60865380"
pwd = "6713"
smartApi = SmartConnect(api_key)

# List of possible stock names
# url https://www.nseindia.com/api/master-quote
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
# year = 2024
# month = 7
# end_date = 2024-07-02

res_ticker_df = pd.DataFrame([])
res_candle_stick_df = pd.DataFrame([])

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


def getInstrumentData(exchange, searchscrip):
    """_summary_

    Args:
        exchange (_type_): _description_
        searchscrip (_type_): _description_

    Returns:
        _type_: _description_
    """
    searchScripData = smartApi.searchScrip(exchange, searchscrip)
    return searchScripData


def get_response(url):
    headers = {
        "user-agent": "Chrome/80.0.3987.149 Safari/537.36",
        "accept-language": "en,gu;q=0.9,hi;q=0.8",
        "Accept": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=5)
    return response


def get_valid_instrument_tickdata(
    symbolToken, interval="1minute", fromDate="2024-07-01", toDate="2024-07-01"
):
    # url = 'https://api.upstox.com/v2/historical-candle/NSE_FO|134606/1minute/2024-07-11/2024-07-1'
    uplinkURL = f"https://api.upstox.com/v2/historical-candle/NSE_FO|{symbolToken}/{interval}/{toDate}/{fromDate}"
    response = get_response(uplinkURL)
    if response.ok:
        df = pd.DataFrame(
            response.json()["data"]["candles"],
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


def generate_dates(year, month, holidays, end_date):
    # Get the first and last day of the month
    start_date = datetime(year, month, 1)
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
def extract_components(s):
    parts = s.split("-")
    tradedate = parts[0] + "-" + parts[1] + "-" + parts[2]
    option_details = parts[3]

    # Identify stock name
    stockname = next(
        (name for name in stock_names if option_details.startswith(name)), None
    )

    if stockname:
        len_stockname = len(stockname)
    else:
        len_stockname = 0

    # Extract expiry date in the format dd-MMM-yy
    expiry_date_raw = option_details[len_stockname : len_stockname + 7]  # '25JUL24'
    expirydate = (
        expiry_date_raw[:2]
        + "-"
        + expiry_date_raw[2:5].upper()
        + "-"
        + "20"
        + expiry_date_raw[5:]
    )

    # Extract strike price
    strikeprice = "".join(filter(str.isdigit, option_details[len_stockname + 7 :]))

    # Extract option type
    optiontype = "".join(
        filter(str.isalpha, option_details[len_stockname + 7 + len(strikeprice) :])
    )

    return pd.Series([tradedate, stockname, expirydate, strikeprice, optiontype])


for date in generate_dates(2024, 7, nse_holidays_2024, "2024-07-22"):
    for stock in stock_names:
        print(f"Dumping {stock} on {date}")
        instrumentData = getInstrumentData("NFO", stock)
        df = pd.DataFrame(
            instrumentData["data"], columns=["exchange", "tradingsymbol", "symboltoken"]
        )
        curr_mon_df = df[
            df["tradingsymbol"].str.contains("JUL")
            & ~df["tradingsymbol"].str.contains("FUT")
        ]
        for symbolToken in curr_mon_df["symboltoken"]:
            valid_df = get_valid_instrument_tickdata(symbolToken, "1minute", date, date)
            res_candle_stick_df = pd.concat([res_candle_stick_df, valid_df])
        curr_mon_df["tradingsymbol"] = curr_mon_df["tradingsymbol"].apply(
            lambda x: date + "-" + x
        )
        res_ticker_df = pd.concat([res_ticker_df, curr_mon_df])
        print(f"Done {stock} on {date}")

print(res_ticker_df)

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
ticker_df["ticker_id"] = res_ticker_df["symboltoken"]
ticker_df[
    ["trade_date", "stock_name", "expiry_date", "strike_price", "option_type"]
] = res_ticker_df["tradingsymbol"].apply(extract_components)
ticker_df["ticker_id"] = ticker_df[["ticker_id", "trade_date"]].agg("-".join, axis=1)

# # DB ACTIVITY

# # Create a PostgreSQL engine
# engine = create_engine(
#     "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
#     % quote("sdadmin@postgres")
# )
# print(engine)
# # Save the DataFrame to a table named 'optstkdata'
# # curr_mon_df.to_sql('optstkdata',if_exists='append', con=engine, index=True)
# ticker_df.to_sql(
#     "ticker", schema="options", if_exists="append", con=engine, index=False
# )
# res_candle_stick_df.to_sql(
#     "candle_stick", schema="options", if_exists="append", con=engine, index=False
# )
