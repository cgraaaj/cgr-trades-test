import pickle
from collections import defaultdict, OrderedDict
from datetime import datetime, timedelta
import pandas as pd
from iteration_utilities import unique_everseen


UPSTOXURL = "https://api.upstox.com/v2/historical-candle/

def group_by_attribute(items, key):
    grouped_dict = defaultdict(list)

    for item in items:
        # filter begin and end time, time >= 10:00 time <=14:00 and filter out B, C and D grades
        if (
            datetime.strptime(item[key].strftime("%H:%M:%S"), "%H:%M:%S").time()
            >= datetime.strptime("09:30:00", "%H:%M:%S").time()
            and datetime.strptime(item[key].strftime("%H:%M:%S"), "%H:%M:%S").time()
            <= datetime.strptime("14:00:00", "%H:%M:%S").time()
            and item["grade"] not in ["B", "C", "D"]
        ):
            group_key = item[key]
            grouped_dict[group_key].append(item)
    sorted_grouped_dict = OrderedDict(sorted(grouped_dict.items()))
    return dict(sorted_grouped_dict)


def append_to_dict_value(dict_obj, key, value):
    if key in dict_obj:
        dict_obj[key].append(value)
    else:
        dict_obj[key] = [value]


def check_consecutive_appearances(grouped_data):
    # Flatten the grouped data into a sorted list of (timestamp, stock)
    consecutive_counts = defaultdict(int)

    prev_stocks = []
    prev_time_stamp = list(grouped_data.keys())[0]
    for time_stamp, items in grouped_data.items():
        temp_prev = []
        if time_stamp.strftime("%Y-%m-%d") != prev_time_stamp.strftime("%Y-%m-%d"):
            prev_stocks = []
        for item in items:
            if item["stock"] in prev_stocks:
                append_to_dict_value(
                    consecutive_counts,
                    time_stamp.strftime("%Y-%m-%d"),
                    {
                        "time_stamp": time_stamp,
                        "stock": item["stock"],
                        "grade": item["grade"],
                    },
                )
            temp_prev.append(item["stock"])
        prev_stocks = temp_prev
        prev_time_stamp = time_stamp
    return consecutive_counts

def remove_second_occurence(data, key):
    seen = set()
    result = []
    for item in data:
        if key in item:
            if item[key] not in seen:
                seen.add(item[key])
                result.append(item)
        else:
            result.append(item)
    return result

def option_ranking(data):
    # get tn_ratio > 60
    # get data of every stock on fifteen mins mark
    c_stocks = []
    p_stocks = []
    prediction = {}
    tn_ratio = 60

    for t in range(0, 25):
        for x in range(0, len(data)):
            if (data[x]["opt_data"][t]["options"]["calls"]["tn_ratio"] > tn_ratio) & (
                data[x]["opt_data"][t]["options"]["calls"]["bullish"]
                > data[x]["opt_data"][t]["options"]["calls"]["bearish"]
            ):
                data[x]["opt_data"][t]["stock"] = data[x]["name"]
                data[x]["opt_data"][t]["grade"] = data[x]["opt_data"][t]["options"][
                    "calls"
                ]["grade"]
                # find the underlying price and calculate day increase/decrease %


                c_stocks.append(data[x]["opt_data"][t])
            if (data[x]["opt_data"][t]["options"]["puts"]["tn_ratio"] > tn_ratio) & (
                data[x]["opt_data"][t]["options"]["puts"]["bullish"]
                > data[x]["opt_data"][t]["options"]["puts"]["bearish"]
            ):
                data[x]["opt_data"][t]["stock"] = data[x]["name"]
                data[x]["opt_data"][t]["grade"] = data[x]["opt_data"][t]["options"][
                    "puts"
                ]["grade"]
                # find the underlying price and calculate day increase/decrease %
                p_stocks.append(data[x]["opt_data"][t])
    call_prediction = check_consecutive_appearances(
        group_by_attribute(c_stocks, "time_stamp")
    )
    put_prediction = check_consecutive_appearances(
        group_by_attribute(p_stocks, "time_stamp")
    )
    prediction["call"] = [
        {"date": date, "stock_data": remove_second_occurence(items,'stock')} for date, items in call_prediction.items()
    ]
    prediction["put"] = [
        {"date": date, "stock_data": remove_second_occurence(items,'stock')} for date, items in put_prediction.items()
    ]
    print(prediction)


def main():
    with open("analyzed_stocks_data_till_05_08.pickle", "rb") as handle:
        data = pickle.load(handle)
    option_ranking(data)


main()
