#!/usr/bin/env python3

# Track end script for one-line shippings.
# Set trackEnd field in database to current date and time for containers
# which reached point of destination.

import sys
import json
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.json_util import dumps
import access

def log(message):
    """Log function to log errors."""
    timestamp = datetime.strftime(datetime.now(), "%Y-%m-%d %H:%M:%S")
    with open("etl.log", "a") as f:
        f.write(timestamp + " " + message + "\n")

def containers_at_destination():
    """Find containers which reached point of destination."""
    # Prepare connection
    conn = MongoClient(access.track_end)
    # Query database: count all documents with status=A and
    # compare with total number of documents.
    try:
        conn.admin.command("ping")
        cur = conn.one.tracking.aggregate([
            {"$match": {"trackEnd": None}},
            {"$addFields": {
                "onlyA": {
                    "$filter": {
                        "input": "$schedule",
                        "as": "item",
                        "cond": {"$eq": ["$$item.status", "A"]}
            }}}},
            {"$redact": {
                "$cond": {
                    "if": {"$ne": [{"$size": "$onlyA"}, {"$size": "$schedule"}]},
                    "then": "$$PRUNE",
                    "else": "$$KEEP"
                }}},
            {"$project": {"_id": 0, "cntrNo": 1}}
        ])
        records = json.loads(dumps(cur))
        conn.close()
        if len(records) > 0:
            return records
        else:
            return False
    except ConnectionFailure:
        log("[Tracking closer] [Records to close] "\
            + f"[DB Connection failure]")
        conn.close()
        return False
    except BaseException as err:
        log("[Tracking closer] [Records to close] "\
            + f"[{err.details}]")
        conn.close()
        return False

def set_track_end(data):
    """Set trackEnd field in database to current date and time."""
    if not data:
        return False
    # Prepare connection
    conn = MongoClient(access.track_end)
    # Close records
    try:
        conn.admin.command("ping")
        for item in data:
            cur = conn.one.tracking.update_one(
                {"cntrNo": rec["cntrNo"]},
                {"$set": {"trackEnd": datetime.now().replace(microsecond=0)}},
            )
            if cur.acknowledged == False:
                log("[Tracking closer] [Close] "\
                    + f"[{rec['cntrNo']} not closed in tracking]")
        conn.close()
    except ConnectionFailure:
        log("[Tracking closer] [Records to close] "\
            + f"[DB Connection failure]")
        conn.close()
        return False
    except BaseException as err:
        log("[Tracking closer] [Records to close] "\
            + f"[{err.details}]")
        conn.close()
        return False

def main():
	"""Pipeline."""
	containers = containers_at_destination()
	set_track_end(containers)

if __name__ == '__main__':
	sys.exit(main())