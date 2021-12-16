#!/usr/bin/env python3

import requests
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import access
from bs4 import BeautifulSoup
import sys

# External data resource
URL = "https://www.marinetraffic.com/en/ais/details/ships/shipid:"

def log(message):
    """Log function to log errors."""
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    with open("etl.log", "a") as f:
        f.write(timestamp + " " + message + "\n")

def request_web_page(ship_id):
    """Request web page for ship_id."""
    response = requests.get(
        URL + str(ship_id),
        headers={"User-Agent": "Mozilla/5.0"}
    )
    return response

def get_page_title(response, ship_id):
    """Get page title string from html response."""
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        return soup.title.text
    else:
        log("[ships_web_scrapper.py] [get_page_title()] " \
                + f"[{response.status_code} for ship id {ship_id}]")
        return False

def scrap_ship_details(title, ship_id):
    """Scrap ship details from web page title."""
    if not title:
        return False
    name_str = title[title.find("Ship") + 5:title.find("Registered in")].strip()
    name = name_str.split("(")[0].strip()
    ship_type = name_str.split("(")[1].replace(")", "")
    flag = title[title.find("Registered in") + 13:title.find("-")].strip()
    imo = title[title.find("IMO ") + 3: title.find("MMSI ") - 2].strip()
    if len(imo) == 7 and imo.isdigit():
        imo = int(imo)
    else:
        log("[ships_web_scrapper.py] [scrap_ship_details()] " \
                + f"[Incorrect imo {imo} for ship id {ship_id}]")
        imo = 0
    mmsi = title[title.find("MMSI ") + 4: title.find("MMSI ") + 14].strip()
    if len(mmsi) == 9 and mmsi.isdigit():
        mmsi = int(mmsi)
    else:
        log("[ships_web_scrapper.py] [scrap_ship_details()] " \
                + f"[Incorrect mmsi {mmsi} for ship id {ship_id}]")
        mmsi = 0
    call_sign = title[title.find("Call Sign") + 9:].strip()
    result = {
        "ship_id": ship_id, "name": name, "type": ship_type, "flag": flag,
        "imo": imo, "mmsi": mmsi, "callSign": call_sign
    }
    return result

def insert_ship_to_db(ship):
    """Insert ship record to db."""
    if not ship:
        return
    # Connect to database and update data
    conn = MongoClient(access.update)
    try:
        conn.admin.command("ping")
        now = datetime.now().replace(microsecond=0)
        ship["update"] = now
        cur = conn.one.ships.insert_one(ship)
        if cur.acknowledged == False:
            log("[ships_web_scrapper.py] [insert_ship_to_db()] "\
                + f"[Ship id {ship['ship_id']} imo {ship['imo']} not inserted]")
        conn.close()
    except ConnectionFailure:
        log("[ships_web_scrapper.py] [insert_ship_to_db()] "\
            + f"[DB Connection failure for ship id {ship['ship_id']} imo {ship['imo']}]")
        conn.close()
    except BaseException as err:
        log("[ships_web_scrapper.py] [insert_ship_to_db()] "\
            + f"[{err} for ship id {ship['ship_id']} imo {ship['imo']}]")
        conn.close()


def main():
    """ETL data pipeline."""
    for ship_id in range(999999):
        response = request_web_page(ship_id)
        title = get_page_title(response, ship_id)
        ship = scrap_ship_details(title, ship_id)
        insert_ship_to_db(ship)

if __name__ == '__main__':
    sys.exit(main())
