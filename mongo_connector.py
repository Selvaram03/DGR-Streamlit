# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):

    # Convert input DD-MMM-YYYY → datetime
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y")

    # Add one extra previous day (your original logic)
    prev_date = start_date - timedelta(days=1)

    # Convert to ISO strings (but time will be ignored later)
    start_date_iso = prev_date.strftime("%Y-%m-%dT00:00:00.000Z")
    end_date_iso = (end_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00.000Z")

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]

    # ✅ Extract DATE ONLY from timestamp (works for ISODate and string)
    date_extractor = {
        "$dateToString": {
            "date": {
                "$cond": [
                    {"$eq": [{"$type": "$timestamp"}, "string"]},
                    {"$toDate": "$timestamp"},
                    "$timestamp"
                ]
            },
            "format": "%Y-%m-%d"
        }
    }

    if customer == "PGCIL":
        pipeline = [
            {"$match": {"timestamp": {"$ne": None}}},
            {"$addFields": {"dateOnly": date_extractor}},
            {"$match": {"dateOnly": {"$gte": prev_date.strftime("%Y-%m-%d"),
                                     "$lte": end_date.strftime("%Y-%m-%d")}}},
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$dateOnly",
                "last_10": {"$push": "$$ROOT"}
            }},
            {"$project": {"last_record": {"$arrayElemAt": ["$last_10", 9]}}},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"_id": 1}}
        ]

    else:
        pipeline = [
            {"$match": {"timestamp": {"$ne": None}}},
            {"$addFields": {"dateOnly": date_extractor}},
            {"$match": {"dateOnly": {"$gte": prev_date.strftime("%Y-%m-%d"),
                                     "$lte": end_date.strftime("%Y-%m-%d")}}},
            {"$sort": {"timestamp": -1}},
            {"$group": {
                "_id": "$dateOnly",
                "last_record": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$last_record"}},
            {"$sort": {"_id": 1}}
        ]

    # ✅ FIX: Allow Disk Sorting (solves your error)
    cleaned_data = list(collection.aggregate(pipeline, allowDiskUse=True))

    client.close()
    return pd.DataFrame(cleaned_data)
