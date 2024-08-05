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

DATABASE_URL = (
    "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
    % quote("sdadmin@postgres")
)

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
    ticker_cepe_df.insert(1, "oi_action_x", ticker_cepe_df.apply(
        lambda row: oi_action(row, "ce"), axis=1
    ))
    ticker_cepe_df.insert(1, "trend_x", np.where(
        ticker_cepe_df["oi_action_x"].isin(buillish),
        "Bullish",
        np.where(ticker_cepe_df["oi_action_x"].isin(bearish), "Bearish", None),
    ))
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
    ticker_cepe_df = convert_candlestick_interval(ticker_cepe_df)
    ticker_cepe_df = analyze_trend(ticker_cepe_df)
    ticker_cepe_df.insert(1, 'strike_price', instrument_row.strike_price)
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

def get_min_simulation(df):
    # Calculate the number of records per day and the total number of days
    records_per_day = 375//5
    total_records = len(df)
    total = total_records//records_per_day

    # Create a list to hold the new DataFrames
    dfs = []

    # Split the DataFrame into daily DataFrames and populate the new DataFrames
    for i in range(records_per_day):
        new_df = pd.concat([df.iloc[j*records_per_day + i:j*records_per_day + i + 1] for j in range(total)], ignore_index=True)
        dfs.append(new_df)
    return dfs


# Query to select all from the 'stock' table
async def option_analyze(expiry_date="2024-08-29", trade_date="2024-07-26"):
    await database.connect()
    stock_df = await query_to_dataframe("SELECT * FROM options.stock")
    # for s_row in stock_df.itertuples():
    stock_id = "e451a2b6-8863-5cad-975a-674d7ff145bd"
    instrument_df = await query_to_dataframe(
        f"SELECT id, stock_id, segment, name, exchange, expiry, expiry_epoch, instrument_type, asset_symbol, \
        underlying_symbol, instrument_key, lot_size, freeze_quantity, exchange_token, minimum_lot, asset_key, \
        underlying_key, tick_size, asset_type, underlying_type, trading_symbol, strike_price, weekly \
        FROM options.instrument where stock_id = uuid('{stock_id}') \
        and expiry = '{expiry_date}' \
        and instrument_type != 'FUT'\
        order by strike_price"
    )
    ticker_df = await process_instrument(instrument_df, trade_date)
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
    # tasks = [
    #     get_ticker_cepe_df(ticker_df, row) for row in instrument_df_ce_pe.itertuples()
    # ]
    # valid_dfs = await asyncio.gather(*tasks)
    candle_stick_df = []
    for row in instrument_df_ce_pe.itertuples():
        candle_stick_df.append(get_ticker_cepe_df(ticker_df, row))
    # if valid_dfs:
    candle_stick_df = pd.concat(candle_stick_df, ignore_index=True)
    # simulate 5 min interval including every strike price
    candle_stick_df = get_min_simulation(candle_stick_df)

    await database.disconnect()


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


asyncio.run(option_analyze())


# for each stock get stock options id
# stock_df[]
# for each stock options get the ticker df
# calculate the trend

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


# df_ce_pe = (
#     pd.merge(df_ce, df_pe, how="outer", on="strikePrice").fillna(0.0).round(2)
# )
# df_ce_pe.columns = columns
# # sends each row axis = 1
# df_ce_pe["Call OI Action"] = df_ce_pe.apply(oi_action_ce, axis=1)
# df_ce_pe["Put OI Action"] = df_ce_pe.apply(oi_action_pe, axis=1)
# df_ce_pe["Call Trend"] = np.where(
#     df_ce_pe["Call OI Action"].isin(buillish),
#     "Bullish",
#     np.where(df_ce_pe["Call OI Action"].isin(bearish), "Bearish", None),
# )
# df_ce_pe["Put Trend"] = np.where(
#     df_ce_pe["Put OI Action"].isin(buillish),
#     "Bullish",
#     np.where(df_ce_pe["Put OI Action"].isin(bearish), "Bearish", None),
# )
# columns.insert(0, "Call Trend")
# columns.insert(1, "Call OI Action")
# columns.insert(len(df_ce_pe.columns) - 1, "Put OI Action")
# columns.insert(len(df_ce_pe.columns), "Put Trend")
# df_ce_pe = df_ce_pe[columns]
# # print(df_ce_pe)
# pprint.pp(df_ce_pe)
