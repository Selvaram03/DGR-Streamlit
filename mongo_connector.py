# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


# ✅ COMMON — DATE RANGE FETCH (used ONLY for Report Page)
def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):

    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    coll = client[DATABASE_NAME][collection_name]

    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},

        # ✅ Normalize timestamp → ts
        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            # Mongo's ISODate()
                            {
                                "case": {"$eq": [{"$type": "$timestamp"}, "date"]},
                                "then": "$timestamp"
                            },

                            # String "YYYY-MM-DD HH:MM"
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
                                    "$dateFromString": {
                                        "dateString": "$timestamp",
                                        "format": "%Y-%m-%d %H:%M"
                                    }
                                }
                            },
                        ],
                        "default": None
                    }
                }
            }
        },

        {"$match": {"ts": {"$ne": None}}},
        {"$addFields": {"day": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}}}},
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},
        {"$limit": 200000}
    ]

    raw_data = list(coll.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(raw_data)

    if "ts" in df.columns:
        df = df.sort_values("ts")

    client.close()
    return df



# ✅ LIVE MODE — ALWAYS RETURN THE LATEST ROW FROM MONGO
def fetch_latest_row(collection_name: str):
    client = MongoClient(MONGO_URI)
    coll = client[DATABASE_NAME][collection_name]

    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},
        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": [{"$type": "$timestamp"}, "date"]}, "then": "$timestamp"},
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
                                    "$dateFromString": {
                                        "dateString": "$timestamp",
                                        "format": "%Y-%m-%d %H:%M"
                                    }
                                }
                            },
                        ],
                        "default": None
                    }
                }
            }
        },
        {"$match": {"ts": {"$ne": None}}},
        {"$sort": {"ts": -1}},
        {"$limit": 1}
    ]

    raw = list(coll.aggregate(pipeline))
    df = pd.DataFrame(raw)

    client.close()
    return df
