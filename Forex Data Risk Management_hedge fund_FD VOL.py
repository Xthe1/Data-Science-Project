import time
import sqlite3
import numpy as np
from datetime import datetime
from polygon import RESTClient
import csv
import pandas as pd
import threading

POLYGON_API_KEY = "beBybSi8daPgsTp5yx5cHtHpYcrjp5Jq"
CURRENCY_PAIRS = ["USDINR", "EURUSD", "GBPUSD"]


def create_auxiliary_db(pair):
    connection = sqlite3.connect("auxiliary.db")
    cursor = connection.cursor()
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {pair} (
                        timestamp TEXT,
                        price REAL
                    )""")
    connection.commit()
    return cursor, connection


def create_final_db(pair):
    connection = sqlite3.connect("final.db")
    cursor = connection.cursor()
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {pair} (
                        timestamp TEXT,
                        max REAL,
                        min REAL,
                        mean REAL,
                        vol REAL,
                        fd REAL
                    )""")
    connection.commit()
    return cursor, connection


def get_current_price(client, api_key, CURRENCY_PAIR):
    client = RESTClient(api_key=api_key)
    from_currency, to_currency = CURRENCY_PAIR[:3], CURRENCY_PAIR[3:]
    response = client.get_last_forex_quote(from_currency, to_currency)
    bid = response.last.bid
    ask = response.last.ask
    avg_price = (bid + ask) / 2
    return avg_price


def calculate_stats_from_db(cursor, pair):
    cursor.execute(f"SELECT price FROM {pair}")
    price_data = cursor.fetchall()

    if not price_data:
        return None, None, None, None

    prices = [row[0] for row in price_data]
    max_price = max(prices)
    min_price = min(prices)
    mean_value = sum(prices) / len(prices)
    vol = (max_price - min_price) / mean_value

    return max_price, min_price, mean_value, vol


def count_crosses(cursor, pair, upper_bands, lower_bands):
    cursor.execute(f"SELECT price FROM {pair}")
    price_data = cursor.fetchall()

    if not price_data:
        return 0

    prices = [row[0] for row in price_data]

    upper_bands = np.array(upper_bands)
    lower_bands = np.array(lower_bands)
    crosses = 0

    for i in range(1, len(prices)):
        upper_crosses = np.sum(np.logical_or(
            np.logical_and(prices[i - 1] <= upper_bands, upper_bands <= prices[i]),
            np.logical_and(prices[i - 1] >= upper_bands, upper_bands >= prices[i])
        ))

        lower_crosses = np.sum(np.logical_or(
            np.logical_and(prices[i - 1] <= lower_bands, lower_bands <= prices[i]),
            np.logical_and(prices[i - 1] >= lower_bands, lower_bands >= prices[i])
        ))

        crosses += upper_crosses + lower_crosses

    return crosses


def clear_auxiliary_data(cursor, pair):
    cursor.execute(f"DELETE FROM {pair}")


def export_to_csv(connection, pair):
    df = pd.read_sql_query(f"SELECT * FROM {pair}", connection)
    df.to_csv(f"{pair}_data.csv", index=False)


def calculate_keltner_bands(mean_value, vol):
    upper_bands = [mean_value + n * 0.025 * vol for n in range(1, 101)]
    lower_bands = [mean_value - n * 0.025 * vol for n in range(1, 101)]
    return upper_bands, lower_bands


def insert_price_data(cursor, pair, timestamp, price):
    cursor.execute(f"INSERT INTO {pair} (timestamp, price) VALUES (?, ?)", (timestamp, price))
    cursor.connection.commit()


# function to aid threading

client = RESTClient(api_key=POLYGON_API_KEY)


def process_currency_pair(pair):
    auxiliary_cursor, auxiliary_connection = create_auxiliary_db(pair)
    final_cursor, final_connection = create_final_db(pair)

    end_time = time.time() + 5 * 60 * 60  # 5 hours from now

    previous_period_stats = None
    first_period = True

    while time.time() < end_time:
        period_start_time = time.time()
        period_end_time = period_start_time + 6 * 60

        while time.time() < period_end_time:
            current_price = get_current_price(client, POLYGON_API_KEY, pair)
            if current_price is not None:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                insert_price_data(auxiliary_cursor, pair, timestamp, current_price)
                print(pair, timestamp, current_price)
            time.sleep(1)

        max_price, min_price, mean_value, vol = calculate_stats_from_db(auxiliary_cursor, pair)

        if not first_period:
            keltner_upper_bands, keltner_lower_bands = calculate_keltner_bands(previous_period_stats[2],
                                                                               previous_period_stats[3])
            crosses = count_crosses(auxiliary_cursor, pair, keltner_upper_bands, keltner_lower_bands)
            if max_price != min_price:
                fd = crosses / (max_price - min_price)
            else:
                fd = 0
            final_cursor.execute(f"INSERT INTO {pair} (timestamp, max, min, mean, vol, fd) VALUES (?, ?, ?, ?, ?, ?)",
                                 (timestamp, max_price, min_price, mean_value, vol, fd))
            print(pair, timestamp, max_price, min_price, mean_value, vol, fd)
            final_connection.commit()

        previous_period_stats = (max_price, min_price, mean_value, vol)
        first_period = False

        clear_auxiliary_data(auxiliary_cursor, pair)

    export_to_csv(final_connection, pair)
    auxiliary_connection.close()
    final_connection.close()


def main():
    threads = []
    for pair in CURRENCY_PAIRS:
        thread = threading.Thread(target=process_currency_pair, args=(pair,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
