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

    logger.info("--------------------------------------------------------")
    logger.info(f"🔍 FETCH REQUEST: collection={collection_name}, customer={customer}")
    logger.info(f"📅 Input Start={start_date_str}, End={end_date_str}")
    logger.info(f"✅ Normalized Start={start_date}, End={end_date}")

    total_docs = coll.count_documents({})
    logger.info(f"📦 Total Docs in Collection '{collection_name}': {total_docs}")

    # ✅ 1: FILTER first (before sorting)
    pipeline = [
        {"$match": {"timestamp": {"$exists": True, "$ne": None}}},

        # ✅ Convert all string timestamps
        {
            "$addFields": {
                "ts": {
                    "$switch": {
                        "branches": [
                            {
                                "case": { "$eq": [ { "$type": "$timestamp" }, "date" ] },
                                "then": "$timestamp"
                            },
                            {
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
                        "default": None
                    }
                }
            }
        },

        {"$match": {"ts": {"$ne": None}}},

        {"$addFields": {"day": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}}}},

        # ✅ FILTER by day before sorting → reduces dataset massively
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},

        # ✅ LIMIT to avoid large memory usage
        {"$limit": 100000},

        # ✅ SORT at end (after filtering) → safe
        {"$sort": {"ts": -1}},
    ]

    raw_data = list(coll.aggregate(pipeline, allowDiskUse=True))
    df = pd.DataFrame(raw_data)

    logger.info(f"✅ Records returned after pipeline: {len(df)}")
    logger.info(f"📄 Final DataFrame Rows: {df.shape[0]}")
    logger.info("--------------------------------------------------------")

    client.close()
    return df
