#!/usr/bin/env python3

# ETL update script for one-line shippings.
# Updates records in one database, tracking collection.

import sys
import requests
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.json_util import dumps
import access

# External data resource
URL = "https://ecomm.one-line.com/ecom/CUP_HOM_3301GS.do"

def log(message):
    """Log function to log errors."""
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    with open("etl.log", "a") as f:
        f.write(timestamp + " " + message + "\n")

def records_to_update():
    """Prepare records which require update."""
    # Prepare connection, query and project fields
    conn = MongoClient(access.update)
    now = datetime.now().replace(microsecond=0)
    query = {
        "trackEnd": None,
        "schedule": {"$elemMatch": {"status": "E", "eventDate": {"$lte": now}}}
    }
    project = {"cntrNo": 1, "copNo": 1, "_id": 0}
    # Query database
    try:
        conn.admin.command("ping")
        cur = conn.one.tracking.find(query, project)
        records = json.loads(dumps(cur))
        conn.close()
        if len(records) > 0:
            return records
        else:
            return False
    except ConnectionFailure:
        log("[ETL Update] [Records to update] "\
            + f"[DB Connection failure]")
        conn.close()
        return False
    except BaseException as err:
        log("[ETL Update] [Records to update] "\
            + f"[{err.details}]")
        conn.close()
        return False

def extract_schedule_details(records):
    """Extract schedule details for update."""
    # Check input
    if not records:
        return False
    # Extract data
    for rec in records:
        # Create payload for get request
        payload = {
            '_search': 'false', 'f_cmd': '125', 'cntr_no': rec["cntrNo"],
            'bkg_no': '', 'cop_no': rec["copNo"]
        }
        # Run request and fetch json data
        r = requests.get(URL, params=payload)
        data = r.json()
        # Extract container schedule data and clean
        if "list" in data:
            schedule_details = data["list"]
            if "hashColumns" in schedule_details[0]:
                del schedule_details[0]["hashColumns"]
            rec["schedule"] = schedule_details
        else:
            log("[ETL Update] [Extract schedule details]"\
                + f" [No schedule for container {rec['cntrNo']}]")
            rec["schedule"] = None
    return records

def transform(records):
    """Transforms raw data for database load."""
    # Check input
    if not records:
        return False
    # Check schedule keys and extract schedule data
    schedule_keys = ["no", "statusNm", "placeNm", "yardNm",
                     "eventDt", "actTpCd", "actTpCd", "vslEngNm",
                     "lloydNo"]
    for rec in records:
        if set(schedule_keys).issubset(set(rec["schedule"][0])):
            schedule = [{
                "no": int(i["no"]),
                "event": i["statusNm"],
                "placeName": i["placeNm"],
                "yardName": i["yardNm"],
                "eventDate": datetime.strptime(i["eventDt"], "%Y-%m-%d %H:%M"),
                "status": i["actTpCd"],
                "vesselName": i["vslEngNm"],
                "imo": i["lloydNo"],
            } for i in rec["schedule"]]
            rec["schedule"] = schedule
        else:
            log("[ETL Update] [Transform] "\
                + f"[Keys do not match in schedule data {rec['cntrNo']}]")
            rec["schedule"] = None
    return records

def update(records):
    """Update records in database."""
    # Check input
    if not records:
        return False
    # Connect to database and update
    conn = MongoClient(access.update)
    try:
        conn.admin.command("ping")
        for rec in records:
            if rec["schedule"]:
                query = {"cntrNo": rec["cntrNo"]}
                change = {"$set": {"schedule": rec["schedule"]}}
                cur_tracking = conn.one.tracking.update_one(query, change)
                if cur_tracking.acknowledged == False:
                    log("[ETL Update] [Update] "\
                    + f"[{rec['cntrNo']} not updated in tracking]")
            else:
                log("[ETL Update] [Update] "\
                + f"[Not updated {rec['cntrNo']}]")
        conn.close()
    except ConnectionFailure:
        log(f"[ETL Update] [Update] [Connection failure]")
        conn.close()
    except BaseException as err:
        log(f"[ETL Update] [Update] [{err}]")
        conn.close()

def main():
	"""Pipeline."""
	records = records_to_update()
	raw_records = extract_schedule_details(records)
	transformed_records = transform(raw_records)
	update(transformed_records)

if __name__ == '__main__':
	sys.exit(main())