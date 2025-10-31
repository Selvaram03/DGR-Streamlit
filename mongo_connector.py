from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None, live_mode=False):
    """
    Fetch data from MongoDB for a specific date range.
    If live_mode=True → fetch only the latest timestamp row.
    """
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    coll = client[DATABASE_NAME][collection_name]

    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},
        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$and": [
                                        {"$eq": [{"$type": "$timestamp"}, "string"]},
                                        {
                                            "$regexMatch": {
                                                "input": "$timestamp",
                                                "regex": "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}$",
                                            }
                                        },
                                    ]
                                },
                                "then": {
                                    "$dateFromString": {
                                        "dateString": "$timestamp",
                                        "format": "%Y-%m-%d %H:%M",
                                    }
                                },
                            }
                        ],
                        "default": None,
                    }
                }
            }
        },
        {"$match": {"ts": {"$ne": None}}},
        {"$sort": {"ts": -1}},
        {"$limit": 1 if live_mode else 200000},  # only latest in live mode
    ]

    raw_data = list(coll.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(raw_data)
    client.close()
    return df
