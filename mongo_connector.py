# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):

    start_date = datetime.strptime(start_date_str, "%d-%b-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y")

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]

    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},

        # ✅ NORMALIZE TIMESTAMP (handle string + ISODate)
        {
            "$addFields": {
                "ts": {
                    "$cond": [
                        { "$eq": [ { "$type": "$timestamp" }, "string" ] },
                        {
                            "$dateFromString": {
                                "dateString": "$timestamp",
                                "format": "%Y-%m-%d %H:%M"
                            }
                        },
                        "$timestamp"
                    ]
                }
            }
        },

        # ✅ Convert timestamp → day string
        {
            "$addFields": {
                "day": {
                    "$dateToString": {
                        "date": "$ts",
                        "format": "%Y-%m-%d"
                    }
                }
            }
        },

        # ✅ Filter by day (ignore time)
        {"$match": {"day": {"$gte": start_date.strftime("%Y-%m-%d"),
                            "$lte": end_date.strftime("%Y-%m-%d")}}},

        # ✅ Sort latest to oldest inside each day
        {"$sort": {"ts": -1}},
    ]

    # ✅ Special logic for PGCIL (10th record)
    if customer == "PGCIL":
        pipeline = base_pipeline + [
            {"$group": {"_id": "$day", "last_10_records": {"$push": "$$ROOT"}}},
            {"$project": {"last_record": {"$arrayElemAt": ["$last_10_records", 9]}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"day": 1}}
        ]
    else:
        pipeline = base_pipeline + [
            {"$group": {"_id": "$day", "last_record": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"day": 1}}
        ]

    cleaned_data = list(collection.aggregate(pipeline))
    client.close()

    return pd.DataFrame(cleaned_data)
