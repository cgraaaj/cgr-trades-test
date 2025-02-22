from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import pandas as pd
import uvicorn
from urllib.parse import quote
from datetime import date
from typing import List
from collections import defaultdict

app = FastAPI()

# Database connection
DATABASE_URL = (
    "postgresql+psycopg2://app-user:%s@192.168.1.72:5430/stock-dumps"
    % quote("appuser@postgres")
)
engine = create_engine(DATABASE_URL)


def execute_query(sql_query, params=None):
    """
    Executes a SQL query and returns the result as a Pandas DataFrame.

    :param sql_query: SQL query as a string or SQLAlchemy text() object.
    :param params: Optional dictionary of query parameters.
    :return: Pandas DataFrame with query results.
    """
    with engine.connect() as connection:
        df = pd.read_sql(text(sql_query), connection, params=params)
    return df


@app.get("/get-stocks/")
def get_stocks():
    sql_query = text(
        f"""
        select * from options.stock
    """
    )

    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)
    return {
        "stocks": df.to_dict(orient="records"),
        "count": len(df.to_dict(orient="records")),
    }


@app.get("/get-expiry", response_model=List[date])
def get_expiry(stock_id: str):
    sql_query = text(
        f"select * from options.instrument where stock_id = '{stock_id}' and instrument_type != 'FUT'"
    )
    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)
    df = df.fillna("")
    expiry = df["expiry"].unique()
    return sorted(expiry)


@app.get("/get-trade-dates", response_model=List[date])
def get_trade_dates(stock_id: str):
    sql_query = text(
        f"""SELECT distinct date(time_stamp) as trade_date
FROM options.ticker t
JOIN options.instrument i 
    ON t.instrument_id = i.id
WHERE i.stock_id = '{stock_id}'
AND i.instrument_type != 'FUT'"""
    )
    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)
    df = df.fillna("")
    trade_dates = df["trade_date"].unique()
    return sorted(trade_dates)


@app.get("/options-data")
def get_options_data(stock_id: str, exp_date: str, trade_date: str):
    sql_query = text(
        f"""SELECT distinct i.id, i.instrument_type, i.strike_price
FROM options.ticker t
JOIN options.instrument i 
    ON t.instrument_id = i.id
WHERE i.stock_id = '{stock_id}'
AND i.instrument_type != 'FUT'
and i.expiry = '{exp_date}'
and date(t.time_stamp) = '{trade_date}'
order by i.strike_price"""
    )
    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)
    res = df.to_dict(orient="records")
    table_dict = defaultdict(lambda: {"strike": 0, "call": "-", "put": "-"})

    for ins in res:
        strike = ins["strike_price"]
        if table_dict[strike]["strike"] == 0:
            table_dict[strike]["strike"] = strike
        if ins["instrument_type"] == "CE":
            table_dict[strike]["call"] = ins["id"]
        elif ins["instrument_type"] == "PE":
            table_dict[strike]["put"] = ins["id"]
    table_list = list(table_dict.values())
    return table_list


# @app.get("/get_fo_data")
# def get_fo_data(stock_id: str):
#     data = defaultdict()
#     data["stock_id"] = stock_id
#     data["per_expiry"] = defaultdict()

#     sql = "select * from options.instrument where stock_id = :stock_id and instrument_type != 'FUT'"
#     params = {"stock_id": f"{stock_id}"}
#     df = execute_query(sql, params)
#     expiry = df["expiry"].unique()
#     for exp in sorted(expiry):
#         data["per_expiry"][f"{exp}"] = defaultdict()
#         sql = """SELECT distinct date(time_stamp) as trade_date FROM options.ticker t JOIN options.instrument i ON t.instrument_id = i.id
#             WHERE i.stock_id = :stock_id
#             AND i.instrument_type != 'FUT'"""
#         params = {"stock_id": f"{stock_id}"}
#         df = execute_query(sql, params)
#         trade_dates = df["trade_date"].unique()
#         data["per_expiry"][f"{exp}"]["per_trade_date"] = defaultdict()
#         for td in sorted(trade_dates):
#             data["per_expiry"][f"{exp}"]["per_trade_date"][f"{td}"] = defaultdict()
#             sql = """SELECT distinct i.id, i.instrument_type, i.strike_price FROM options.ticker t JOIN options.instrument i ON t.instrument_id = i.id
#                         WHERE i.stock_id = :stock_id AND i.instrument_type != 'FUT' and i.expiry = :exp_date and date(t.time_stamp) = :trade_date
#                         order by i.strike_price"""
#             params = {
#                 "stock_id": f"{stock_id}",
#                 "exp_date": f"{exp}",
#                 "trade_date": f"{td}",
#             }
#             df = execute_query(sql, params)
#             options = df.to_dict(orient="records")
#             table_dict = defaultdict(lambda: {"strike": 0, "call": "-", "put": "-"})


#             for ins in options:
#                 strike = ins["strike_price"]
#                 if table_dict[strike]["strike"] == 0:
#                     table_dict[strike]["strike"] = strike
#                 if ins["instrument_type"] == "CE":
#                     table_dict[strike]["call"] = ins["id"]
#                 elif ins["instrument_type"] == "PE":
#                     table_dict[strike]["put"] = ins["id"]
#             table_list = list(table_dict.values())
#             data["per_expiry"][f"{exp}"]["per_trade_date"][f"{td}"] = table_list
#     return data


@app.get("/get_fo_data")
def get_fo_data(stock_id: str):
    data = {"stock_id": stock_id, "per_expiry": {}}

    # Step 1: Get All Required Data in One Query
    sql = """
        SELECT i.id, i.instrument_type, i.strike_price, i.expiry, DATE(t.time_stamp) as trade_date 
        FROM options.ticker t
        JOIN options.instrument i ON t.instrument_id = i.id
        WHERE i.stock_id = :stock_id AND i.instrument_type != 'FUT'
        ORDER BY i.expiry, trade_date, i.strike_price
    """
    params = {"stock_id": stock_id}
    df = execute_query(sql, params)  # Fetch all data at once

    if df.empty:
        return data  # No data found, return empty structure

    # Step 2: Group Data by Expiry and Trade Date
    grouped = df.groupby(["expiry", "trade_date"])

    for (expiry, trade_date), group in grouped:
        expiry_key = str(expiry)
        trade_date_key = str(trade_date)

        if expiry_key not in data["per_expiry"]:
            data["per_expiry"][expiry_key] = {"per_trade_date": {}}

        table_dict = defaultdict(lambda: {"strike": 0, "call": "-", "put": "-"})

        # Step 3: Process Each Row and Populate Table
        for _, row in group.iterrows():
            strike = row["strike_price"]
            if table_dict[strike]["strike"] == 0:
                table_dict[strike]["strike"] = strike
            if row["instrument_type"] == "CE":
                table_dict[strike]["call"] = row["id"]
            elif row["instrument_type"] == "PE":
                table_dict[strike]["put"] = row["id"]

        data["per_expiry"][expiry_key]["per_trade_date"][trade_date_key] = list(
            table_dict.values()
        )

    return data


@app.get("/")
async def root():
    return {"message": "Hello World"}


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=1234, reload=True)
