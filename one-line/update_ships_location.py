#!/usr/bin/env python3

# Update_ships_location script for one-line shippings.
# Updates ships location in one database, tracking collection.
# Adds ships information to one database, ships collection (imo, mmsi vesselName).

import sys
import requests
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.json_util import dumps
from bs4 import BeautifulSoup
import access

def log(message):
    """Log function to log errors."""
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    with open("etl.log", "a") as f:
        f.write(timestamp + " " + message + "\n")

def ships_to_update():
    """Find containers which require ship poistion update."""
    # Prepare connection, query and project fields
    conn = MongoClient(access.update)
    now = datetime.now().replace(microsecond=0)
    pipeline = [
    {"$match": {"trackEnd": None}},
    {"$unwind": "$schedule"},
    {"$match": {
        "schedule.status": "A",
        "schedule.eventDate": {"$lte": now},
        "schedule.imo": {"$ne": ""}}
    },
    # Group by cntrNo, add maxNo and push all items into array
    {"$group": {
        "_id": "$cntrNo",
        "maxNo": {"$max": "$schedule.no"},
        "items": {"$push": {
            "vesselName": "$schedule.vesselName",
            "imo": "$schedule.imo",
            "no": "$schedule.no"}}}
    },
    # Filter items to keep only one with item.no=maxNo
    {"$project": {
        "details": {
            "$filter": {
                "input": "$items",
                "as": "item",
                "cond": {"$eq": ["$$item.no", {"$getField": "maxNo"}]}}}}
    },
    # Extract item from array
    {"$project": {
        "details": {"$arrayElemAt": ["$details", 0]}}
    },
    # Project final output
    {"$project": {
        "cntrNo": "$_id",
        "vesselName": "$details.vesselName",
        "imo": "$details.imo",
        "_id": 0}
    }]
    # Query database
    try:
        conn.admin.command("ping")
        cur = conn.one.tracking.aggregate(pipeline)
        records = json.loads(dumps(cur))
        conn.close()
        if len(records) > 0:
            return records
        else:
            return False
    except ConnectionFailure:
        log("[Update ship location] [Ships to update] "\
            + f"[DB Connection failure]")
        conn.close()
        return False
    except BaseException as err:
        log("[Update ship location] [Ships to update] "\
            + f"[{err.details}]")
        conn.close()
        return False

def get_mmsi_from_web(imo):
    """Get mmsi number from https://www.shiplocation.com
    using imo number."""
    # Get mmsi number from website
    url = "https://www.shiplocation.com/vessels?"
    payload = {"page": "1", "vessel": imo, "sort": "none",
              "direction": "none", "flag": "none"}
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, params=payload, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "html.parser")
        obj = soup.find("a", class_="vessel-link")
        if obj:
            link = obj.get("href")
            mmsi = link[link.rfind("-") + 1:]
        else:
            mmsi = ""
        #idx_start = r.text.find("MMSI-") + 5 # offset for 'MMSI-'
        #idx_end = r.text.find('"', idx_start)
        #mmsi = r.text[idx_start:idx_end]
        if mmsi.isdigit() and len(mmsi) == 9:
            return mmsi
        else:
            log("[Update ship location] [Get mmsi from web] "\
                + f"[mmsi for imo {imo} not found]")
    else:
        log("[Update ship location] [Get mmsi from web] "\
                + f"[{r.status_code} for imo {imo}]")

def insert_ship_to_db(ship):
    """Insert ship record to db."""
    # Connect to database and update data
    conn = MongoClient(access.update)
    try:
        conn.admin.command("ping")
        now = datetime.now().replace(microsecond=0)
        ship_copy = ship.copy()
        del ship_copy["cntrNo"]
        ship_copy["lastUpdate"] = now
        cur = conn.one.ships.insert_one(ship_copy)
        conn.close()
        if cur.acknowledged == False:
            log("[Update ship location] [Insert ship to db] "\
                + f"[Imo {ship['imo']} not inserted]")
    except ConnectionFailure:
        log("[Update ship location] [Insert ship to db] "\
            + "[DB Connection failure for imo "\
            + f"{ship['imo']}, mmsi {ship['mmsi']}]")
        conn.close()
    except BaseException as err:
        log("[Update ship location] [Insert ship to db] "\
            + f"[{err} for imo {ship['imo']}, mmsi {ship['mmsi']}]")
        conn.close()

def get_mmsi(ships):
    """Get mmsi from db or web, add to 'ships' argument.
    If mmsi not found in db, add it from web to db.
    Return 'ships' agrument with mmsi."""
    # Check arguments
    if not ships:
        log("[Update ship location] [Get mmsi] "\
            + "[No input arguments]")
        return False
    # Connect to database and get data
    conn = MongoClient(access.update)
    for ship in ships:
        try:
            # Get record from db or from web
            conn.admin.command("ping")
            cur = conn.one.ships.find({"imo": ship["imo"]})
            now = datetime.now().replace(microsecond=0)
            db_data = json.loads(dumps(cur))
            if len(db_data) == 0:
                mmsi = get_mmsi_from_web(ship["imo"])
                if mmsi:
                    ship["mmsi"] = mmsi
                    insert_ship_to_db(ship)
                else:
                    log("[Update ship location] [Get mmsi] "\
                        + f"[MMSI for imo {ship['imo']} not inserted to db]")
            elif len(db_data) == 1:
                ship["mmsi"] = db_data[0]["mmsi"]
        except ConnectionFailure:
            log("[Update ship location] [Get mmsi] "\
                + f"[DB Connection failure for imo {ship['imo']}]")
            conn.close()
        except BaseException as err:
            log("[Update ship location] [Get mmsi] "\
                + f"[{err} for imo {ship['imo']}]")
            conn.close()
    return ships

def parse_lon_lat(html):
    """Get latitude and longitute from
    https://www.vesselfinder.com raw html."""
    soup = BeautifulSoup(html, "html.parser")
    location = []
    for i in ["coordinate lon", "coordinate lat"]:
        obj = soup.find("div", class_=i)
        if obj:
            text = obj.text
            if text.replace(".", "").replace("-", "").isdigit():
                location.append(text)
            else:
                location.append("")
        else:
            location.append("")
    return location

def get_ships_location(ships):
    """Get ships locations from web https://www.vesselfinder.com."""
    # Check arguments
    if not ships:
        log("[Update ship location] [Get ships location] "\
            + "[No input arguments]")
        return False
    # Run get requests for locations
    base_url = "https://www.vesselfinder.com/vessels/{}-IMO-{}-MMSI-{}"
    headers = {"User-Agent": "Mozilla/5.0"}
    for ship in ships:
        url = base_url.format(ship["vesselName"].replace(" ", "-"),
                              ship["imo"], ship["mmsi"])
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            location = parse_lon_lat(r.text)
            ship["location"] = location
            if "" in location:
                log("[Update ship location] [Get ships location] "\
                    + f"[Parsing failed for imo {ship['imo']}]")
                ship["location"] = location
            else:
                ship["location"] = [float(location[0]), float(location[1])]
        else:
            log("[Update ship location] [Get ships location] "\
                + f"[{r.status_code} for imo {ship['imo']}]")
            ship["location"] = ["", ""]
    return ships

def update(ships):
    """Update ships location in tracking collection."""
    # Connect to database and update
    conn = MongoClient(access.update)
    try:
        conn.admin.command("ping")
        for ship in ships:
            if "" in ship["location"]:
                log("[Update ship location] [Update] "\
                    + "[No lon lat to update imo "\
                    + f"{ship['imo']}/cntr {ship['cntrNo']}]")
            else:
                query = {"cntrNo": ship["cntrNo"]}
                change = {"$set": {
                    "vesselName": ship["vesselName"],
                    "location": ship["location"]
                }}
                cur = conn.one.tracking.update_one(query, change)
                if cur.acknowledged == False:
                    log("[Update ship location] [Update] "\
                    + f"[{ship['cntrNo']} location not updated]")
        conn.close()
    except ConnectionFailure:
        log(f"[Update ship location] [Update] [Connection failure]")
        conn.close()
    except BaseException as err:
        log(f"[Update ship location] [Update] [{err}]")
        conn.close()

def main():
	"""Pipeline."""
	ships = ships_to_update()
	ships_with_mmsi = get_mmsi(ships)
	ships_with_location = get_ships_location(ships_with_mmsi)
	update(ships_with_location)

if __name__ == '__main__':
	sys.exit(main())