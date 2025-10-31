# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):

    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]

    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},

        # ✅ NORMALIZE TIMESTAMP (handle ISODate, Full date-time string, ignore "HH:mm")
        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            {
                                # Case A: timestamp is ISO date
                                "case": { "$eq": [ { "$type": "$timestamp" }, "date" ] },
                                "then": "$timestamp"
                            },
                            {
                                # Case B: timestamp is "YYYY-MM-DD HH:MM"
                                "case": {
                                    "$and": [
                                        { "$eq": [ { "$type": "$timestamp" }, "string" ] },
                                        { "$regexMatch": {
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
                            }
                        ],
                        # Case C: invalid format ("16:52") → discard
                        "default": None
                    }
                }
            }
        },

        # ✅ Remove invalid timestamps
        {"$match": {"ts": {"$ne": None}}},

        # ✅ Extract day
        {"$addFields": {"day": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}}}},

        # ✅ Filter by day
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},

        # ✅ Sort inside day
        {"$sort": {"ts": -1}},
    ]

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
