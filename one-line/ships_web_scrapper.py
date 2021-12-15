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
    return requests.get(URL + ship_id, headers={"User-Agent": "Mozilla/5.0"})

def parse_title(response):
    """Parse title string from html response."""
    response = BeautifulSoup(response.text, "html.parser")
    return response.title.text

def parse_ship_details(title):
    """Parse ship details from web page title."""
    name_str = title[title.find("Ship") + 5:title.find("Registered in")].strip()
    name = name_str.split("(")[0].strip()
    ship_type = name_str.split("(")[1].replace(")", "")
    flag = title[title.find("Registered in") + 13:title.find("-")].strip()
    imo = title[title.find("IMO") + 3: title.find("MMSI") - 2].strip()
    mmsi = title[title.find("MMSI") + 4: title.find("MMSI") + 14].strip()
    call_sign = title[title.find("Call Sign") + 9:].strip()
    result = {
        "ship_id": "", "name": name, "type": ship_type, "flag": flag,
        "imo": imo, "mmsi": mmsi, "callSign": call_sign
    }
    return result

def web_scrapper(ship_ids):
    """Scrap data from marinetraffic.com by ship id number."""
    result = []
    for ship_id in ship_ids:
        response = request_web_page(ship_id)
        if response.status_code == 200:
            #print(ship_id)
            #print(response.text)
            title = parse_title(response)
            ship_details = parse_ship_details(title)
            ship_details["ship_id"] = ship_id
            result.append(ship_details)
        else:
            log("[Ships_web_scrapper.py] [web_scrapper()] " \
                + f"[No ship for ship id {ship_id}]")
    return result

def insert_ship_to_db(ships):
    """Insert ship record to db."""
    # Connect to database and update data
    conn = MongoClient(access.update)
    try:
        conn.admin.command("ping")
        now = datetime.now().replace(microsecond=0)
        for ship in ships:
            ship_copy = ship.copy()
            #del ship_copy["cntrNo"]
            ship_copy["lastUpdate"] = now
            cur = conn.one.ships.insert_one(ship_copy)
            if cur.acknowledged == False:
                log("[Update ship location] [Insert ship to db] "\
                    + f"[Imo {ship['imo']} not inserted]")
        conn.close()
    except ConnectionFailure:
        log("[Update ship location] [Insert ship to db] "\
            + "[DB Connection failure for imo "\
            + f"{ship['imo']}, mmsi {ship['mmsi']}]")
        conn.close()
    except BaseException as err:
        log("[Update ship location] [Insert ship to db] "\
            + f"[{err} for imo {ship['imo']}, mmsi {ship['mmsi']}]")
        conn.close()

def main():
    for idx, i in enumerate(range(10,9999999,10)):
        ship_ids = [str(ii) for ii in range(idx * 10,i)]
        ships = web_scrapper(ship_ids)
        insert_ship_to_db(ships)

if __name__ == '__main__':
    sys.exit(main())