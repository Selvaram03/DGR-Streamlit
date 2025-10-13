# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str):
    """Fetch cleaned inverter data between two dates from MongoDB"""
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y")

    prev_date = start_date - timedelta(days=1)
    start_date_iso = prev_date.strftime("%Y-%m-%dT00:00:00.000Z")
    end_date_inc = end_date - timedelta(milliseconds=1)
    end_date_iso = end_date_inc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},
        {"$match": {"timestamp": {"$gte": start_date_iso, "$lte": end_date_iso}}},
        {"$addFields": {
            "day": {"$dateToString": {"date": {"$toDate": "$timestamp"}, "format": "%Y-%m-%d"}}
        }},
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": "$day", "last_record": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$last_record"}},
        {"$sort": {"day": 1}}
    ]

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]
    cleaned_data = list(collection.aggregate(pipeline))
    client.close()

    df = pd.DataFrame(cleaned_data)
    return df
