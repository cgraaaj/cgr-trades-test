import pickle
from collections import defaultdict


def group_by_attribute(items, key):
    grouped_dict = defaultdict(list)

    for item in items:
        group_key = item[key]
        grouped_dict[group_key].append(item)

    return dict(grouped_dict)


def check_consecutive_appearances(grouped_data, threshold=2):
    # Flatten the grouped data into a sorted list of (timestamp, stock)
    data = sorted(
        (timestamp, item["stock"])
        for timestamp, items in grouped_data.items()
        for item in items
    )

    consecutive_counts = defaultdict(int)
    previous_stock = None
    previous_timestamp = None
    count = 0
    threshold_timestamp = None

    for timestamp, stock in data:
        if stock == previous_stock:
            count += 1
            if count == threshold:
                threshold_timestamp = timestamp
        else:
            if count >= threshold and threshold_timestamp:
                consecutive_counts[previous_stock] = (count, threshold_timestamp)
            count = 1
            threshold_timestamp = None

        previous_stock = stock

    # Final check at the end of the loop
    if count >= threshold and threshold_timestamp:
        consecutive_counts[previous_stock] = (count, threshold_timestamp)

    return consecutive_counts


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
                c_stocks.append(data[x]["opt_data"][t])
            if (data[x]["opt_data"][t]["options"]["puts"]["tn_ratio"] > tn_ratio) & (
                data[x]["opt_data"][t]["options"]["puts"]["bullish"]
                > data[x]["opt_data"][t]["options"]["puts"]["bearish"]
            ):
                data[x]["opt_data"][t]["stock"] = data[x]["name"]
                p_stocks.append(data[x]["opt_data"][t])
    call_prediction = check_consecutive_appearances(
        group_by_attribute(c_stocks, "time_stamp")
    )
    put_prediction = check_consecutive_appearances(
        group_by_attribute(p_stocks, "time_stamp")
    )
    prediction["call"] = sorted({
        (time_stamp, stock) for stock, (count, time_stamp) in call_prediction.items()
    },key=lambda x: x[0])
    prediction["put"] = sorted({
        (time_stamp, stock) for stock, (count, time_stamp) in put_prediction.items()
    },key=lambda x:x[0])
    print(prediction)


def main():
    with open("analyzed_stocks_data.pickle", "rb") as handle:
        data = pickle.load(handle)
    option_ranking(data)


main()
