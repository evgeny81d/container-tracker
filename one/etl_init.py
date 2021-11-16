#!/usr/bin/env python3

# ETL init script for one-line shippings.
# Loads new records into one database, tracking and init collections.

import sys
import requests
import time
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

def check_record(bill_number):
    """Check that init and tracking database does not have container record yet."""
    conn = MongoClient(access.init)
    query = {"blNo": bill_number, "trackEnd": None}
    try:
        conn.admin.command("ping")
        init = conn.one.init.count_documents(query)
        tracking = conn.one.tracking.count_documents(query)
        if init == 0 and tracking == 0:
            conn.close()
            return True
        else:
            conn.close()
            log(f"[ETL Init] [Check record]"\
                + f" [Record already exists for {bill_number}]")
            return False
    except ConnectionFailure:
        log("[ETL Init] [Check record]"\
            + f" [DB Connection failure for {bill_number}]")
        conn.close()
        return False
    except BaseException as err:
        log("[ETL Init] [Check record]"\
            + f" [{err.details} for {bill_number}]")
        conn.close()
        return False

def extract_container_details(bill_number):
    """Post request to extract container details."""
    if isinstance(bill_number, str):
        # Create payload for get request
        payload = {
            '_search': 'false', 'nd': str(time.time_ns())[:-6],
            'rows': '10000', 'page': '1', 'sidx': '',
            'sord': 'asc', 'f_cmd': '121', 'search_type': 'A',
            'search_name': bill_number, 'cust_cd': '',
        }
        # Run request and fetch json data
        r = requests.get(URL, params=payload)
        data = r.json()
        # Extract container details data
        if "list" in data:
            container_details = data["list"][0]
            # Remove unnecessary data
            if "hashColumns" in container_details:
                del container_details["hashColumns"]
            return container_details
        else:
            log(f"[ETL Init] [Extract container details]"\
                + f" [No details data for {bill_number}]")
            return False
    else:
        log("[ETL Init] [Extract container details]"\
            + f" [Wrong argument type {bill_number}]")
        return False

def extract_schedule_details(cntr_details):
    """Extract schedule details."""
    if cntr_details:
        # Create payload for get request
        payload = {
            '_search': 'false', 'f_cmd': '125', 'cntr_no': cntr_details["cntrNo"],
            'bkg_no': '', 'cop_no': cntr_details["copNo"]
        }
        # Run request and fetch json data
        r = requests.get(URL, params=payload)
        data = r.json()
        # Extract container schedule data
        if "list" in data:
            schedule_details = data["list"]
            if "hashColumns" in schedule_details[0]:
                del schedule_details[0]["hashColumns"]
            return schedule_details
        else:
            log("[ETL Init] [Extract schedule details]"\
                + f" [No schedule for container {cntr_details['cntrNo']}]")
            return False
    else: 
        return False

def extract(bill_number):
    """Extract container and schedule details and
    return one document."""
    cntr_details = extract_container_details(bill_number)
    schedule_details = extract_schedule_details(cntr_details)
    if cntr_details and schedule_details:
        return {"container": cntr_details,
                "schedule": schedule_details,
                "number": bill_number}
    else:
        log("[ETL Init] [Extract phase]"\
            + f" [No data for {bill_number}]")
        return False

def transform(data):
    """Transforms raw data for database load."""
    # Check data argument
    if not data:
        log("[ETL Init] [Transform]"\
            + f" [No raw data]")
        return False
    # Check contnainer keys and extract container info
    cntr_keys = ["cntrNo", "cntrTpszNm", "copNo", "blNo"]
    if set(cntr_keys).issubset(set(data["container"])):
        result = {
            "cntrNo": data["container"]["cntrNo"],
            "cntrType": data["container"]["cntrTpszNm"],
            "copNo": data["container"]["copNo"],
            "blNo": data["container"]["blNo"],
            "trackStart": datetime.now().replace(microsecond=0),
            "trackEnd": None,
            "outboundTerminal": "",
            "inboundTerminal": "",
            "vesselName": None,
            "location": None,
            "schedule": None,
        }
    else:
        log("[ETL Init] [Transform]"\
            + f" [Keys do not match in container data {data['number']}]")
        return False
    # Check schedule keys and extract schedule data
    schedule_keys = ["no", "statusNm", "placeNm", "yardNm",
                     "eventDt", "actTpCd", "actTpCd", "vslEngNm",
                     "lloydNo"]
    if set(schedule_keys).issubset(set(data["schedule"][0])):
        schedule = [{
            "no": int(i["no"]),
            "event": i["statusNm"],
            "placeName": i["placeNm"],
            "yardName": i["yardNm"],
            "eventDate": datetime.strptime(i["eventDt"], "%Y-%m-%d %H:%M"),
            "status": i["actTpCd"],
            "vesselName": i["vslEngNm"],
            "imo": i["lloydNo"],
        } for i in data["schedule"]]
        result["schedule"] = schedule
        # Find and save outbound and inbound terminals
        for i in data["schedule"]:
            if i["statusNm"].find("Outbound Terminal") > -1:
                result["outboundTerminal"] = i["placeNm"]\
                + "|" + i["yardNm"]
            if i["statusNm"].find("Inbound Terminal") > -1:
                result["inboundTerminal"] = i["placeNm"]\
                + "|" + i["yardNm"]
    else:
        log("[ETL Init] [Transform]"\
            + f" [Keys do not match in schedule data {data['number']}]")
        return False
    return result

def load(data):
    """Loads data into init and tracking collections."""
    # Check data argument
    if not data:
        log("[ETL Init] [Load] [No data to load]")
        return None
    # Connect to database and load data
    conn = MongoClient(access.init)
    try:
        conn.admin.command("ping")
        cur_init = conn.one.init.insert_one(data)
        if cur_init.acknowledged == False:
            log("[ETL Init] [Load] "\
                + f"[{data['blNo']} not loaded to init]")
        cur_tracking = conn.one.tracking.insert_one(data)
        if cur_tracking.acknowledged == False:
            log("[ETL Init] [Load] "\
                + f"[{data['blNo']} not loaded to tracking]")
        conn.close()
    except ConnectionFailure:
        log("[ETL Init] [Load] "\
            + f"[Connection failure for {data['blNo']}]")
        conn.close()
    except BaseException as err:
        log("[ETL Init] [Load] "\
            + f"[{err.details} for {data['blNo']}]")
        conn.close()

def main(args):
    """Pipeline."""
    if len(args) > 0:
        for arg in args:
            if check_record(arg):
                raw_data = extract(arg)
                transformed_data = transform(raw_data)
                load(transformed_data)

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
