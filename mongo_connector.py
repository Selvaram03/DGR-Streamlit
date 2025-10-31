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

    # ✅ Base timestamp normalization (your logic kept exactly)
    base_pipeline = [
        {"$match": {"timestamp": {"$ne": None}}},

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

        {"$match": {"day": {"$gte": start_date, "$lte": end_date}}},
    ]

    # ✅ Heavy sort removed — replaced with lightweight max timestamp per day
    # ✅ Prevents 32MB memory crash
    group_stage = {
        "$group": {
            "_id": "$day",
            "max_ts": {"$max": "$ts"}
        }
    }

    pipeline = base_pipeline + [group_stage, {"$sort": {"_id": 1}}]

    grouped = list(collection.aggregate(pipeline))
    if not grouped:
        client.close()
        return pd.DataFrame()

    final_docs = []

    # ✅ Normal customer: fetch only 1 (latest) record per day
    if customer != "PGCIL":
        for row in grouped:
            doc = collection.find_one({"ts": row["max_ts"]})
            if doc:
                final_docs.append(doc)

    # ✅ PGCIL: fetch the 10th latest
    else:
        for row in grouped:
            # Get latest 10 for the day safely, but sorted only inside the day
            docs = list(
                collection.find({"day": row["_id"]})
                .sort("ts", -1)
                .limit(10)
            )

            if len(docs) >= 10:
                final_docs.append(docs[9])  # 10th record
            elif docs:
                final_docs.append(docs[-1])  # fallback to last available

    client.close()
    return pd.DataFrame(final_docs)
