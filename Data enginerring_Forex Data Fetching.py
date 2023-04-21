#Han Wang
#2023-3-1

from polygon import RESTClient
import time
from datetime import datetime

import sqlite3
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


def from_epoch_to_datetime(epoch_time):
    return datetime.fromtimestamp(epoch_time).strftime("%d-%m-%Y %H:%M:%S")


def get_real_time_forex(client, from_="USD", to="EUR"):
    quote = client.get_real_time_currency_conversion(from_=from_, to=to, amount=1, precision=4)
    return {
        "from": from_,
        "to": to,
        "fx_rate": quote.converted,
        "timestamp": quote.last.timestamp,
    }


def get_data(currency_pairs, timespan):
    result = []

    for i in range(timespan):
        time.sleep(1)
        for currency_pair in currency_pairs:
            from_ = currency_pair[:3]
            to = currency_pair[3:]
            result.append(get_real_time_forex(from_=from_, to=to))

    return result


def main():
    client = RESTClient(api_key="beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq")

    # create an SQLite DB
    conn = sqlite3.connect('fx_rates_table.db')
    cursor = conn.cursor()
    # create a table to store the FX rate data
    cursor.execute('''CREATE TABLE IF NOT EXISTS fx_rates_table
                      (timestamp TEXT, fx_rate TEXT, entry_timestamp TEXT)''')
    conn.commit()

    currency_pairs = ['EURUSD', 'USDJPY', 'GBPUSD', 'USDCAD', 'USDCNY']
    table_name = "fx_rates_table"

    for i in range(2 * 3600):  # change to 10 during testing
        time.sleep(1)  # comment out during testing
        for cp in currency_pairs:
            from_, to = cp[:3], cp[3:]
            result = get_real_time_forex(client, from_=from_, to=to)

            print(result["timestamp"], type(result["timestamp"]))

            ts = from_epoch_to_datetime(result["timestamp"] / 1000)
            fx_rate = result["from"] + result["to"] + "|" + str(result["fx_rate"]).replace(".", "-")
            entry_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(ts, fx_rate, entry_ts)
            cursor.execute(f"INSERT INTO {table_name} VALUES ('{ts}', '{fx_rate}', '{entry_ts}')")
        conn.commit()


    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    df.to_csv(f"{table_name}.csv")


    for cp in currency_pairs:
        df = pd.read_sql_query(f"SELECT * FROM {table_name} WHERE fx_rate LIKE '{cp}%'", conn)
        avg_fx_rate = np.mean([float(x.split('|')[1].replace("-", ".")) for x in list(df['fx_rate'])])
        print(f"The average FX rate for {cp} is {avg_fx_rate:.4f}")


if __name__ == '__main__':
    main()





# ticker = "AAPL"
# # List Aggregates (Bars)
# bars = client.get_aggs(ticker=ticker, multiplier=1, timespan="day", from_="2023-01-09", to="2023-01-10")
# for bar in bars:
#     print(bar)

# from datetime import datetime, timedelta
# end_time = datetime.now().replace(microsecond=0, second=0)
# start_time = end_time - timedelta(hours=2)
# print(start_time, end_time)

# def from_epoch_to_datetime(epoch_time):
#     return datetime.fromtimestamp(epoch_time).strftime("%d-%m-%Y %H:%M:%S")


# def test_aggs(ticker="C:EURUSD", from_=None, to=None):
#     while from_ < to:
#         bars = client.get_aggs(
#             ticker=ticker,
#             from_=from_,
#             to=from_,
#             multiplier=2 * 60,
#             timespan='minute',
#             # full_range=True,
#             # warnings=False
#         )

#         for bar in bars:
#             print(bar)

#         from_ = from_ + timedelta(seconds=1)
#         print(from_)



# def test_quotes(ticker="C:EURUSD", timestamp="2023-01-25", to="2023-01-26"):
#     quotes = client.list_quotes(
#         ticker=ticker,
#         timestamp=timestamp
#     )
#     for i, quote in enumerate(quotes):
#         if i > 10:
#             break
#         print(quote)
