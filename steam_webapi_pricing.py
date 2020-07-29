# stdlib
import requests
from datetime import date

# 3rd party
import pyodbc
import pandas as pd


def steam_web_api_request(appid_string: str, price_dict: dict):
    """connects to steam's web API and gets the current price for an app ID,
    adding it to the price dict.
    params:
    - appid_string: str of comma-separated app IDs to insert into the API URL
    - price_dict: dict with app ID and accompanying price
    """
    url = f"https://store.steampowered.com/api/appdetails?appids={appid_string}&cc=us&filters=price_overview"
    response = requests.get(url, stream=True)
    json_data = response.json()
    for appid in json_data:
        if json_data[appid]["success"] is True:
            if json_data[appid]["data"]:
                price = json_data[appid]["data"]["price_overview"]["final_formatted"]
                price_dict[appid] = price


def get_steam_prices(conn, cursor):
    """
    gets all app IDs in a SQL database, pulls current price of each app ID
    from Steam web API.
    params:
    - conn: pyodbc SQL database connection
    - cursor: pyodbc cursor
    return: dict with app IDs and current prices
    """
    price_dict = {}

    # gets latest app ID by top 1 entry id (auto-increment primary key)
    top_app_id_SQL = (
        """
        SELECT TOP 1 [entry_id]
        FROM [Steam].[dbo].[app_ids]
        ORDER BY [app_id] DESC;
        """
    )
    cursor.execute(top_app_id_SQL)
    top_id = cursor.fetchone()

    # runs through and adds prices to the price dict until all of the app IDs
    # in the database have been accounted for
    while top_id > 0:

        # list of Steam App IDs, 800 at a time,
        # since that seems to be how many the web API can handle per request
        top_id_param = [top_id]
        get_app_ids_SQL = (
            """
            SELECT TOP 800 [app_id]
            FROM [Steam].[dbo].[app_ids]
            WHERE [entry_id] <= ?
            ORDER BY [entry_id] DESC;
            """
        )
        df = pd.read_sql(
            get_app_ids_SQL,
            conn,
            params=top_id_param
        )

        # turn aliases into a single comma-separated string to fit into API url
        appid_list = df["app_id"].tolist()
        intlist = [int(i) for i in appid_list]
        appid_string = ""
        for appid in intlist:
            appid_string += f"{appid},"
        appid_string = appid_string[:-1]

        steam_web_api_request(appid_string, price_dict)
        top_id -= 800

    return price_dict


def update_db_with_prices():
    """connects to a SQL database, gets app ID prices, updates db with current
    prices per app ID.
    """

    # connection to a local SQL Server database
    conn = pyodbc.connect(
        """
        Driver={ODBC Driver 17 for SQL Server};
        Server=.;
        Database=Steam;
        Trusted_Connection=yes;
        """
    )
    cursor = conn.cursor()
    cursor.fast_executemany = True

    # SQL script to update the db with the latest prices
    insert_SQL = (
        """
        INSERT INTO [Steam].[dbo].[app_prices] (app_id, date, price)
        VALUES ?, ?, ?;
        """
    )

    # generates dict of app IDs and current price from Steam web API
    price_dict = get_steam_prices(conn, cursor)
    today = date.today()

    # list of tuples to interface with pyodbc's executemany
    prices = [(appid, today, price_dict[appid]) for appid in price_dict]

    # updates the db
    cursor.executemany(
        insert_SQL,
        prices
    )
    conn.commit()


if __name__ == "__main__":
    update_db_with_prices()
