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

    # ✅ Base timestamp normalization
    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},

        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$eq": [{"$type": "$timestamp"}, "date"]},
                                "then": "$timestamp",
                            },
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
                            },
                        ],
                        "default": None,
                    }
                }
            }
        },

        {"$match": {"ts": {"$ne": None}}},

        {
            "$addFields": {
                "day": {
                    "$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}
                }
            }
        },

        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},
    ]

    # ✅ For normal customers → pick latest record of each day
    if customer != "PGCIL":
        pipeline = base_pipeline + [
            {"$sort": {"ts": -1}},   # sort only filtered records
            {
                "$group": {
                    "_id": "$day",
                    "record": {"$first": "$$ROOT"}   # pick latest safely
                }
            },
            {"$replaceRoot": {"newRoot": "$record"}},
            {"$sort": {"day": 1}}
        ]

    # ✅ For PGCIL → pick 10th latest record of each day
    else:
        pipeline = base_pipeline + [
            {"$sort": {"ts": -1}},   # small sort after filtering
            {
                "$group": {
                    "_id": "$day",
                    "records": {"$push": "$$ROOT"}   # store all sorted
                }
            },
            {
                "$project": {
                    "record": {"$arrayElemAt": ["$records", 9]}  # 10th record
                }
            },
            {"$replaceRoot": {"newRoot": "$record"}},
            {"$sort": {"day": 1}}
        ]

    cleaned_data = list(collection.aggregate(pipeline, allowDiskUse=True))
    client.close()

    return pd.DataFrame(cleaned_data)
