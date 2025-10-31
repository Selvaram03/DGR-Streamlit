# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def _to_iso_date_pipeline():
    """
    Returns the $addFields stage that normalizes `timestamp` into `ts` (Mongo date).
    Handles:
      - native BSON date type
      - strings like "YYYY-MM-DD HH:MM"
    """
    return {
        "$addFields": {
            "ts": {
                "$switch": {
                    "branches": [
                        # native date
                        {"case": {"$eq": [{"$type": "$timestamp"}, "date"]}, "then": "$timestamp"},
                        # string "YYYY-MM-DD HH:MM"
                        {
                            "case": {
                                "$and": [
                                    {"$eq": [{"$type": "$timestamp"}, "string"]},
                                    {"$regexMatch": {
                                        "input": "$timestamp",
                                        "regex": "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}$"
                                    }}
                                ]
                            },
                            "then": {
                                "$dateFromString": {"dateString": "$timestamp", "format": "%Y-%m-%d %H:%M"}
                            }
                        }
                    ],
                    "default": None
                }
            }
        }
    }


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):
    """
    REPORT mode fetch:
      - normalize timestamps -> ts
      - compute day (YYYY-MM-DD)
      - filter days in range
      - for each day pick the latest record (per day)
      - return DataFrame sorted by day ascending
    """
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    coll = client[DATABASE_NAME][collection_name]

    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},
        _to_iso_date_pipeline(),
        {"$match": {"ts": {"$ne": None}}},
        {"$addFields": {"day": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}}}},
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},
        # sort before group so $first picks latest per day
        {"$sort": {"ts": -1}},
        {"$group": {"_id": "$day", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"day": 1}}
    ]

    raw = list(coll.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(raw)

    # safety: ensure ts exists and is datetime-like
    if "ts" in df.columns:
        try:
            df = df.sort_values("ts")
        except Exception:
            pass

    client.close()
    return df


def fetch_latest_row(collection_name: str):
    """
    LIVE mode fetch:
      - normalize timestamps -> ts
      - match ts not null
      - sort by ts desc and limit 1 (latest document)
      - return single-row DataFrame (or empty df)
    """
    client = MongoClient(MONGO_URI)
    coll = client[DATABASE_NAME][collection_name]

    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},
        _to_iso_date_pipeline(),
        {"$match": {"ts": {"$ne": None}}},
        {"$sort": {"ts": -1}},
        {"$limit": 1}
    ]

    raw = list(coll.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(raw)

    client.close()
    return df

