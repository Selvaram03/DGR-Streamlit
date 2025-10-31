# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):
    # Convert dd-MMM-YYYY -> YYYY-MM-DD for day-level comparison
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]

    # ✅ Base pipeline for all customers (convert timestamp + match by day)
    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},

        # ✅ Convert "2025-10-31 09:49" → Date → "2025-10-31"
        {
            "$addFields": {
                "day": {
                    "$dateToString": {
                        "date": {
                            "$dateFromString": {
                                "dateString": "$timestamp",
                                "format": "%Y-%m-%d %H:%M"
                            }
                        },
                        "format": "%Y-%m-%d"
                    }
                }
            }
        },

        # ✅ Match by day only (ignore time)
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},

        # ✅ Sort latest to oldest within each day
        {"$sort": {"timestamp": -1}},
    ]

    # ✅ Special PGCIL logic: pick 10th record of each day
    if customer == "PGCIL":
        pipeline = base_pipeline + [
            {"$group": {"_id": "$day", "last_10_records": {"$push": "$$ROOT"}}},
            {"$project": {"last_record": {"$arrayElemAt": ["$last_10_records", 9]}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"day": 1}}
        ]

    # ✅ Default logic: pick the latest record of each day
    else:
        pipeline = base_pipeline + [
            {"$group": {"_id": "$day", "last_record": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"day": 1}}
        ]

    # ✅ Execute
    cleaned_data = list(collection.aggregate(pipeline))
    client.close()

    return pd.DataFrame(cleaned_data)
