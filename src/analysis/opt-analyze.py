import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import requests
import pandas as pd
import numpy as np
import pprint
import urllib
from sqlalchemy import create_engine, text
from urllib.parse import quote
from uuid import UUID
import asyncio
from databases import Database
import math
import pickle
from collections import defaultdict
from decimal import Decimal
import config

DATABASE_URL = config.DATABASE_URL
stock_svc_url = config.STOCK_SVC_URL

engine = create_engine(DATABASE_URL)
database = Database(DATABASE_URL)

buillish = ["Short Cover", "Long Buildup"]
bearish = ["Long Unwind", "Short Buildup"]


async def query_to_dataframe(query):
    async with database.transaction():
        results = await database.fetch_all(query=query)
        if results:
            columns = results[0].keys()
            # Convert results to a list of dictionaries
            data = [dict(result) for result in results]
            df = pd.DataFrame(data, columns=columns)
        else:
            df = pd.DataFrame()
    return df


def oi_action(row, option_type):
    if option_type == "ce":
        price_change = row["ltp_change_x"]
        change_in_oi = row["open_interest_change_x"]
    elif option_type == "pe":
        price_change = row["ltp_change_y"]
        change_in_oi = row["open_interest_change_y"]
    else:
        raise ValueError("Invalid option type. Must be 'ce' or 'pe'.")

    if price_change > 0 and change_in_oi > 0:
        return "Long Buildup"
    elif price_change > 0 and change_in_oi < 0:
        return "Short Cover"
    elif price_change < 0 and change_in_oi > 0:
        return "Short Buildup"
    elif price_change < 0 and change_in_oi < 0:
        return "Long Unwind"


def normalize_df_with_timestamp(df, trade_date):
    start_time = pd.Timestamp(f"{trade_date} 09:15:00")
    end_time = pd.Timestamp(f"{trade_date} 15:29:00")
    full_range = pd.date_range(start=start_time, end=end_time, freq="1T")
    full_range_df = pd.DataFrame(full_range, columns=["time_stamp"])
    merged_df = pd.merge(full_range_df, df, on="time_stamp", how="left")
    merged_df["open_interest"].fillna(0.0, inplace=True)
    merged_df["open_interest_change"].fillna(0.0, inplace=True)
    merged_df["volume"].fillna(0.0, inplace=True)
    merged_df["ltp"].fillna(0.0, inplace=True)
    merged_df["ltp_change"].fillna(0.0, inplace=True)
    # merged_df["oi_action"].fillna("", inplace=True)
    # merged_df["trend"].fillna("", inplace=True)
    col = merged_df.pop("time_stamp")
    del df[col.name]
    merged_df.insert(len(merged_df.columns), col.name, col)
    return merged_df


def analyze_trend(ticker_cepe_df):
    ticker_cepe_df.insert(
        1, "oi_action_x", ticker_cepe_df.apply(lambda row: oi_action(row, "ce"), axis=1)
    )
    ticker_cepe_df.insert(
        1,
        "trend_x",
        np.where(
            ticker_cepe_df["oi_action_x"].isin(buillish),
            "Bullish",
            np.where(ticker_cepe_df["oi_action_x"].isin(bearish), "Bearish", None),
        ),
    )
    ticker_cepe_df["oi_action_y"] = ticker_cepe_df.apply(
        lambda row: oi_action(row, "pe"), axis=1
    )
    ticker_cepe_df["trend_y"] = np.where(
        ticker_cepe_df["oi_action_y"].isin(buillish),
        "Bullish",
        np.where(ticker_cepe_df["oi_action_y"].isin(bearish), "Bearish", None),
    )
    return ticker_cepe_df


def get_ticker_cepe_df(ticker_df, instrument_row):
    trade_date = ticker_df.iloc[0]["time_stamp"].strftime("%Y-%m-%d")
    columns_cepe = [
        "time_stamp",
        "open_interest",
        "open_interest_change",
        "volume",
        "ltp",
        "ltp_change",
        # "oi_action",
        # "trend",
    ]
    if pd.isnull(instrument_row.ce_id):
        ticker_ce_df = normalize_df_with_timestamp(
            pd.DataFrame(columns=columns_cepe), trade_date
        )
    else:
        ticker_ce_df = ticker_df[ticker_df["instrument_id"] == instrument_row.ce_id]
        ticker_ce_df["ltp"] = ticker_ce_df["close"]
        ticker_ce_df["ltp_change"] = ticker_ce_df["ltp"].diff()
        ticker_ce_df["open_interest_change"] = ticker_ce_df["open_interest"].diff()
        ticker_ce_df = normalize_df_with_timestamp(ticker_ce_df, trade_date)
        # ticker_ce_df["oi_action"] = ticker_ce_df.apply(
        #     lambda row: oi_action(row, "CE"), axis=1
        # )
        # ticker_ce_df["trend"] = np.where(
        #     ticker_ce_df["oi_action"].isin(buillish),
        #     "Bullish",
        #     np.where(ticker_ce_df["oi_action"].isin(bearish), "Bearish", None),
        # )
        ticker_ce_df = pd.DataFrame(ticker_ce_df)[columns_cepe]
    if pd.isnull(instrument_row.pe_id):
        ticker_pe_df = normalize_df_with_timestamp(
            pd.DataFrame(columns=columns_cepe), trade_date
        )
    else:
        ticker_pe_df = ticker_df[ticker_df["instrument_id"] == instrument_row.pe_id]
        ticker_pe_df["ltp"] = ticker_pe_df["close"]
        ticker_pe_df["ltp_change"] = ticker_pe_df["ltp"].diff()
        ticker_pe_df["open_interest_change"] = ticker_pe_df["open_interest"].diff()
        ticker_pe_df = normalize_df_with_timestamp(ticker_pe_df, trade_date)
        # ticker_pe_df["oi_action"] = ticker_pe_df.apply(
        #     lambda row: oi_action(row, "PE"), axis=1
        # )
        # ticker_pe_df["trend"] = np.where(
        #     ticker_pe_df["oi_action"].isin(buillish),
        #     "Bullish",
        #     np.where(ticker_pe_df["oi_action"].isin(bearish), "Bearish", None),
        # )
        ticker_pe_df = pd.DataFrame(ticker_pe_df)[columns_cepe]
    ticker_cepe_df = (
        pd.merge(ticker_ce_df, ticker_pe_df, how="outer", on="time_stamp")
        .fillna(0.0)
        .round(2)
    )
    # change interval
    ticker_cepe_df = convert_candlestick_interval(ticker_cepe_df, "15T")
    ticker_cepe_df = analyze_trend(ticker_cepe_df)
    ticker_cepe_df.insert(1, "strike_price", instrument_row.strike_price)
    return ticker_cepe_df


def convert_candlestick_interval(df, new_interval="5T"):
    """
    Convert 1-minute candlestick data to a specified higher timeframe.

    Args:
        df (pd.DataFrame): DataFrame with 1-minute candlestick data.
        new_interval (str): New interval for resampling (e.g., '5T' for 5 minutes, '10T' for 10 minutes, '15T' for 15 minutes).

    Returns:
        pd.DataFrame: Resampled DataFrame with the new interval.
    """
    # Ensure 'time_stamp' is a datetime column
    df["time_stamp"] = pd.to_datetime(df["time_stamp"])
    df.set_index("time_stamp", inplace=True)
    
    decimal_cols = [
        "open_interest_x",
        "open_interest_change_x",
        "volume_x",
        "ltp_x",
        "ltp_change_x",
        "open_interest_y",
        "open_interest_change_y",
        "volume_y",
        "ltp_y",
        "ltp_change_y",
    ]

    for col in decimal_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

    # Resample the DataFrame to the new interval
    resampled_df = (
        df.resample(new_interval)
        .agg(
            {
                "open_interest_x": "last",
                "open_interest_change_x": "sum",
                "volume_x": "sum",
                "ltp_x": "last",
                "ltp_change_x": "sum",
                "open_interest_y": "last",
                "open_interest_change_y": "sum",
                "volume_y": "sum",
                "ltp_y": "last",
                "ltp_change_y": "sum",
            }
        )
        .fillna(0.0)
    )

    # Drop rows with all NaN values (which can occur if there are no data points in the new interval)
    resampled_df.dropna(how="all", inplace=True)

    # Reset index to make 'time_stamp' a column again
    resampled_df.reset_index(inplace=True)

    return resampled_df


def get_min_simulation(df, interval=5):
    # Calculate the number of records per day and the total number of days
    records_per_day = 375 // interval
    total_records = len(df)
    total = total_records // records_per_day

    # Create a list to hold the new DataFrames
    dfs = []

    # Split the DataFrame into daily DataFrames and populate the new DataFrames
    for i in range(records_per_day):
        new_df = pd.concat(
            [
                df.iloc[j * records_per_day + i : j * records_per_day + i + 1]
                for j in range(total)
            ],
            ignore_index=True,
        )
        dfs.append(new_df)
    return dfs


def trend_n_grade_analysis(df):
    temp = {}
    no_of_strike_price = len(df)
    temp["time_stamp"] = df.iloc[0]["time_stamp"]
    temp["options"] = {
        "calls": {"bullish": 0, "bearish": 0, "percentage": 0, "grade": ""},
        "puts": {"bullish": 0, "bearish": 0, "percentage": 0, "grade": ""},
    }
    temp["options"]["calls"]["bullish"] = len(df[df["trend_x"] == "Bullish"])
    temp["options"]["calls"]["bearish"] = len(df[df["trend_x"] == "Bearish"])
    if temp["options"]["calls"]["bullish"] == 0:
        temp["options"]["calls"]["percentage"] = 0
    else:
        temp["options"]["calls"]["percentage"] = math.ceil(
            (
                (
                    temp["options"]["calls"]["bullish"]
                    - temp["options"]["calls"]["bearish"]
                )
                / temp["options"]["calls"]["bullish"]
            )
            * 100
        )
    if temp["options"]["calls"]["percentage"] == 100:
        temp["options"]["calls"]["grade"] = "A"
    elif (
        temp["options"]["calls"]["percentage"] > 85
        and temp["options"]["calls"]["percentage"] < 100
    ):
        temp["options"]["calls"]["grade"] = "B"
    elif (
        temp["options"]["calls"]["percentage"] > 50
        and temp["options"]["calls"]["percentage"] <= 85
    ):
        temp["options"]["calls"]["grade"] = "C"
    else:
        temp["options"]["calls"]["grade"] = "D"
    # calculate trend none ratio for call
    temp["options"]["calls"]["tn_ratio"] = math.ceil(
        (
            (temp["options"]["calls"]["bullish"] + temp["options"]["calls"]["bearish"])
            / no_of_strike_price
        )
        * 100
    )

    temp["options"]["puts"]["bullish"] = len(df[df["trend_y"] == "Bullish"])
    temp["options"]["puts"]["bearish"] = len(df[df["trend_y"] == "Bearish"])
    if temp["options"]["puts"]["bullish"] == 0:
        temp["options"]["puts"]["percentage"] = 0
    else:
        temp["options"]["puts"]["percentage"] = math.ceil(
            (
                (
                    temp["options"]["puts"]["bullish"]
                    - temp["options"]["puts"]["bearish"]
                )
                / temp["options"]["puts"]["bullish"]
            )
            * 100
        )
    if temp["options"]["puts"]["percentage"] == 100:
        temp["options"]["puts"]["grade"] = "A"
    elif (
        temp["options"]["puts"]["percentage"] > 85
        and temp["options"]["puts"]["percentage"] < 100
    ):
        temp["options"]["puts"]["grade"] = "B"
    elif (
        temp["options"]["puts"]["percentage"] > 50
        and temp["options"]["puts"]["percentage"] <= 85
    ):
        temp["options"]["puts"]["grade"] = "C"
    else:
        temp["options"]["puts"]["grade"] = "D"
    # calculate trend none ratio for put
    temp["options"]["puts"]["tn_ratio"] = math.ceil(
        (
            (temp["options"]["puts"]["bullish"] + temp["options"]["puts"]["bearish"])
            / no_of_strike_price
        )
        * 100
    )

    temp["callTrend"] = (
        True
        if temp["options"]["calls"]["bullish"] > temp["options"]["calls"]["bearish"]
        else False
    )
    temp["putTrend"] = (
        True
        if temp["options"]["puts"]["bullish"] > temp["options"]["puts"]["bearish"]
        else False
    )
    return temp

# async def process_option_ticker(call, put, trade_date):
async def process_option_ticker():
    instrument_id = 'eabca150-1dfe-550a-b612-49851cbb9502'
    trade_date = '2024-07-26'
    try:
        params ={
            "instrument_id":f"{instrument_id}",
            "trade_date":f"{trade_date}"
        }
        response = requests.get(stock_svc_url+'/get_ticker_data', params=params)
        response.raise_for_status()  # raise exception for HTTP errors (4xx, 5xx)

        data = response.json() 
        print(data)
        df = pd.DataFrame(data["payload"]["candles"])
        # Ensure time_stamp is datetime
        df["time_stamp"] = pd.to_datetime(df["time_stamp"])

        # LTP is just the 'close' price
        df["ltp"] = df["close"]

        # Compute changes
        df["oi_change"] = df["open_interest"].diff().fillna(0)
        df["ltp_change"] = df["ltp"].diff().fillna(0)

        # Optional: Reorder columns for readability
        df = df[["time_stamp", "open", "high", "low", "close", "volume", "open_interest", "oi_change", "ltp", "ltp_change"]]

        print(df)
        df.to_csv('test.csv')
    except Exception as e:
        print(e)


# Query to select all from the 'stock' table
async def option_analyze():
    # stock_id = "e451a2b6-8863-5cad-975a-674d7ff145bd"
    # stock_name = 'AARTIIND'
    try:
        params = {
            "stock_id": "e451a2b6-8863-5cad-975a-674d7ff145bd"
        }
        response = requests.get(stock_svc_url+'/get_fo_data', params=params)
        response.raise_for_status()  # raise exception for HTTP errors (4xx, 5xx)

        data = response.json()  # or use .text if it's plain text or .content for binary
#         {
#   "stock_id": "e451a2b6-8863-5cad-975a-674d7ff145bd",
#   "per_expiry": {
#     "2024-08-29": {
#       "per_trade_date": {
#         "2024-07-26": {
#           "570.0": {
#             "strike": 570,
#             "call": null,
#             "put": "eabca150-1dfe-550a-b612-49851cbb9502"
#           },
        # create common template consisting of  oi c-in-oi ce-ltp c-ce-ltp strike oi c-in-oi pe-ltp c-pe-ltp
        stock_id = data.get("stock_id")
        per_expiry = data.get("per_expiry", {})

        for expiry_date, expiry_data in per_expiry.items():
            trade_dates = expiry_data.get("per_trade_date", {})
            for trade_date, strikes in trade_dates.items():
                for strike_price, strike_info in strikes.items():
                    strike = strike_info.get("strike")
                    call = strike_info.get("call")
                    put = strike_info.get("put")

                    print(f"Stock: {stock_id}, Expiry: {expiry_date}, Trade Date: {trade_date}, "
                        f"Strike: {strike}, Call ID: {call}, Put ID: {put}")
                    process_option_ticker(call,put,trade_date)
        print(data)
    except  Exception as err:
        print("Error:", err)
    
    # create table ce_id strike_price pe_id
    columns = ["ce_id", "strike_price", "pe_id"]
    instrument_df_ce = instrument_df[instrument_df["instrument_type"] == "CE"]
    instrument_df_ce = pd.DataFrame(instrument_df_ce)[["id", "strike_price"]]
    instrument_df_pe = instrument_df[instrument_df["instrument_type"] == "PE"]
    instrument_df_pe = pd.DataFrame(instrument_df_pe)[["strike_price", "id"]]
    instrument_df_ce_pe = (
        pd.merge(
            instrument_df_ce, instrument_df_pe, how="outer", on="strike_price"
        ).fillna(np.nan)
    ).sort_values("strike_price")
    instrument_df_ce_pe.columns = columns
    candle_stick_df = []
    for row in instrument_df_ce_pe.itertuples():
        candle_stick_df.append(get_ticker_cepe_df(ticker_df, row))
    candle_stick_df = pd.concat(candle_stick_df, ignore_index=True)
    # change interval - simulate 5 min interval including every strike price
    simaltion_dfs = get_min_simulation(candle_stick_df, 15)
    simulated_data = []
    instrument_data = {}
    instrument_data["name"] = s_row.name
    for simaltion_df in simaltion_dfs:
        simulated_data.append(trend_n_grade_analysis(simaltion_df))
    instrument_data["opt_data"] = simulated_data
    return instrument_data


async def process_instrument(instrument_df, trade_date):
    instrument_ids = [
        f"uuid('{row.id}')" for row in instrument_df.itertuples(index=False)
    ]
    ticker_df = await query_to_dataframe(
        f"select * from options.ticker \
        where instrument_id in ({','.join(instrument_ids)}) \
        and time_stamp >= '{trade_date} 09:15:00' \
        and time_stamp <= '{trade_date} 15:30:00' \
        order by time_stamp"
    )
    return ticker_df

async def main():
    await database.connect()
    # stock_df = await query_to_dataframe("SELECT * FROM options.stock")
    # await option_analyze()
    await process_option_ticker()
    await database.disconnect()


asyncio.run(main())

df_ce_pe = pd.DataFrame()
columns = [
    "Call OI",
    "Call Change in OI",
    "Call Volume",
    "Call LTP",
    "Call Price Change",
    "Strike Price",
    "Put Price Change",
    "Put LTP",
    "Put Volume",
    "Put Change in OI",
    "Put OI",
]

