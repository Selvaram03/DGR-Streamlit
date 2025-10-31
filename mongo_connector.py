# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):

    # Convert input dates to YYYY-MM-DD
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]

    # ✅ NEW: Extract date using substring (no date conversion needed)
    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},

        # Extract date part: "2025-10-31"
        {"$addFields": {"day": {"$substr": ["$timestamp", 0, 10]}}},

        # Filter by day
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},

        # Sort inside each day by timestamp descending
        {"$sort": {"timestamp": -1}}
    ]

    if customer == "PGCIL":
        pipeline = base_pipeline + [
            {"$group": {"_id": "$day", "last_10_records": {"$push": "$$ROOT"}}},
            {"$project": {"last_record": {"$arrayElemAt": ["$last_10_records", 9]}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"_id": 1}}
        ]
    else:
        pipeline = base_pipeline + [
            {"$group": {"_id": "$day", "last_record": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"_id": 1}}
        ]

    cleaned_data = list(collection.aggregate(pipeline, allowDiskUse=True))
    client.close()

    return pd.DataFrame(cleaned_data)
