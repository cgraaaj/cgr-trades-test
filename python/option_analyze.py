import requests
import pandas as pd
import numpy as np
import pprint
import urllib
from sqlalchemy import create_engine, text
from urllib.parse import quote
from uuid import UUID

engine = create_engine(
    "postgresql+psycopg2://sd_admin:%s@192.168.1.72:5430/stock-dumps"
    % quote("sdadmin@postgres")
)

NAMESPACE_STOCK = UUID("233c16a9-0a91-4c9d-adda-8a496c63a1a3")


def query_to_dataframe(query):
    with engine.connect() as connection:
        result = connection.execute(text(query))
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    return df


# Connect to the database


# Query to select all from the 'stock' table
def option_analyze(expiry_date="2024-08-29", trade_date="2024-07-26"):
    stock_df = query_to_dataframe("SELECT * FROM options.stock")
    res = []
    for s_row in stock_df.itertuples():
        ticker_df = query_to_dataframe(
            f"select option_type, count(option_type) from options.ticker where stock_id=uuid('{s_row.id}') group by option_type"
        )
        if ticker_df.iloc[0]['count'] != ticker_df.iloc[1]['count']:
            print(s_row.name, ticker_df.iloc[0]['count'])
            res.append(s_row.name)
    print(res,len(res))
        # for t_row in ticker_df.itertuples():
        # 	print(t_row)
        # 	if t_row.option_type == 'CE':
        # 		ce_candle_df = query_to_dataframe(f"select * from options.candle_stick where ticker_id = uuid('{t_row.id}')")
        # 		print('ce done')
        # 	else:
        # 		pe_candle_df = query_to_dataframe(f"select * from options.candle_stick where ticker_id = uuid('{t_row.id}')")
        # 		print('pe done')


option_analyze()

# for each stock get stock options id
# stock_df[]
# for each stock options get the ticker df
# calculate the trend


df_ce_pe = pd.DataFrame()
buillish = ["Short Cover", "Long Buildup"]
bearish = ["Long Unwind", "Short Buildup"]
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


def oi_action_ce(row):
    if row["Call Price Change"] > 0 and row["Call Change in OI"] > 0:
        return "Long Buildup"
    elif row["Call Price Change"] > 0 and row["Call Change in OI"] < 0:
        return "Short Cover"
    elif row["Call Price Change"] < 0 and row["Call Change in OI"] > 0:
        return "Short Buildup"
    elif row["Call Price Change"] < 0 and row["Call Change in OI"] < 0:
        return "Long Unwind"


def oi_action_pe(row):
    if row["Put Price Change"] > 0 and row["Put Change in OI"] > 0:
        return "Long Buildup"
    elif row["Put Price Change"] > 0 and row["Put Change in OI"] < 0:
        return "Short Cover"
    elif row["Put Price Change"] < 0 and row["Put Change in OI"] > 0:
        return "Short Buildup"
    elif row["Put Price Change"] < 0 and row["Put Change in OI"] < 0:
        return "Long Unwind"


# df_ce = pd.DataFrame(ce)[
#     [
#         "openInterest",
#         "changeinOpenInterest",
#         "totalTradedVolume",
#         "lastPrice",
#         "change",
#         "strikePrice",
#     ]
# ]
# df_pe = pd.DataFrame(pe)[
#     [
#         "strikePrice",
#         "change",
#         "lastPrice",
#         "totalTradedVolume",
#         "changeinOpenInterest",
#         "openInterest",
#     ]
# ]
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
