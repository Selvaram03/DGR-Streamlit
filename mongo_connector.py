# mongo_connector.py
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import streamlit as st
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGO_URI = st.secrets["MONGO_URI"]
DATABASE_NAME = "scada_db"


def fetch_cleaned_data(collection_name: str, start_date_str: str, end_date_str: str, customer: str = None):
    """
    Fetches data from MongoDB with full timestamp normalization and debugging.
    """

    logger.info("--------------------------------------------------------")
    logger.info(f"🔍 FETCH REQUEST: collection={collection_name}, customer={customer}")
    logger.info(f"📅 Input Start={start_date_str}, End={end_date_str}")

    # Convert to YYYY-MM-DD
    start_date = datetime.strptime(start_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%d-%b-%Y").strftime("%Y-%m-%d")

    logger.info(f"✅ Normalized Start={start_date}, End={end_date}")

    client = MongoClient(MONGO_URI)
    collection = client[DATABASE_NAME][collection_name]

    # --------- DEBUG: Count total records ---------
    total_docs = collection.count_documents({})
    logger.info(f"📦 Total Docs in Collection '{collection_name}': {total_docs}")

    # --------- Core Pipeline ---------
    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},
        {"$project": {
            "_id": 1,
            "timestamp": 1,
            # Normalize timestamp:
            "ts": {
                "$switch": {
                    "branches": [
                        # Case A: timestamp stored as actual ISODate → just use it
                        {
                            "case": {"$eq": [{"$type": "$timestamp"}, "date"]},
                            "then": "$timestamp"
                        },
                        # Case B: timestamp stored as "YYYY-MM-DD HH:MM"
                        {
                            "case": {
                                "$and": [
                                    {"$eq": [{"$type": "$timestamp"}, "string"]},
                                    {"$regexMatch": {
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
        }},
        {"$match": {"ts": {"$ne": None}}},  # Remove invalid timestamps

        # Extract day
        {"$addFields": {"day": {"$dateToString": {"date": "$ts", "format": "%Y-%m-%d"}}}},

        # Filter between start and end date
        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},

        {"$sort": {"ts": -1}},
    ]

    # ---- DEBUG: Show extracted days BEFORE grouping ----
    debug_day_pipeline = base_pipeline + [
        {"$group": {"_id": "$day", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]

    try:
        debug_days = list(collection.aggregate(debug_day_pipeline, allowDiskUse=True))
        logger.info("📅 DEBUG → Available days in DB for selected range:")
        for d in debug_days:
            logger.info(f"   👉 Day={d['_id']}  Count={d['count']}")
        if not debug_days:
            logger.warning("⚠ No days matched in the date range!")
    except Exception as e:
        logger.error(f"❌ DEBUG Day Check Failed: {e}")

    # --------- Choose grouping logic ---------
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

    # ---------- RUN PIPELINE ----------
    logger.info("🚀 Running final MongoDB pipeline...")
    try:
        cleaned_data = list(collection.aggregate(pipeline, allowDiskUse=True))
        logger.info(f"✅ Records returned after pipeline: {len(cleaned_data)}")
    except Exception as e:
        logger.error(f"❌ Aggregation Failure: {e}")
        cleaned_data = []

    client.close()

    df = pd.DataFrame(cleaned_data)
    logger.info(f"📄 Final DataFrame Rows: {len(df)}")
    logger.info("--------------------------------------------------------")

    return df
