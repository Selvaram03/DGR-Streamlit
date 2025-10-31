# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st
import logging

logger = logging.getLogger(__name__)

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):

    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    client = MongoClient(MONGO_URI)
    coll = client[DATABASE_NAME][collection_name]

    logger.info(f"Fetching data for {collection_name} from {start_date} → {end_date}")

    # ✅ NO SORTING IN MONGO (removes memory limit error)
    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},
        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            { "case": { "$eq": [ { "$type": "$timestamp" }, "date" ] },
                              "then": "$timestamp" },

                            { "case": {
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
                              }},
                        ],
                        "default": None
                    }
                }
            }
        },
        {"$match": {"ts": {"$ne": None}}},
        {"$addFields": {"day": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}}}},
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},

        # ✅ Just return matching docs, NO SORT here
        {"$limit": 200000}  # safety limit
    ]

    raw_data = list(coll.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(raw_data)

    logger.info(f"✅ Filtered docs returned: {df.shape[0]}")

    # ✅ SORT LOCALLY (safe)
    if "ts" in df.columns:
        df = df.sort_values("ts")

    client.close()
    return df
