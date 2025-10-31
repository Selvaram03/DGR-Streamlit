# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz
import os

from streamlit_autorefresh import st_autorefresh
from mongo_connector import fetch_cleaned_data, fetch_latest_row
from dgr_generator import clean_dataframe, get_daily_monthly_data, calculate_kpis

IST = pytz.timezone("Asia/Kolkata")
st.set_page_config(page_title="DGR Dashboard", layout="wide")


# ---------------------------------------
# Customer mapping
# ---------------------------------------
CUSTOMER_TABLES = {
    "Imagica": "opcua_data",
    "BEL2": "BEL2",
    "Caspro": "Caspro",
    "Dunung": "Dunung",
    "Kasturi": "Kasturi",
    "Mauryaa": "Mauryaa",
    "Paranjape": "Paranjape",
    "BEL1": "Rajgir",
    "Vinathi_2": "PSS",
    "Vinathi_3": "Vinathi_3",
    "Vinathi_4": "Vinathi_4",
    "TMD": "TMD",
    "PGCIL": "PGCIL"
}

# ---------------------------------------
# Sidebar
# ---------------------------------------
if "page" not in st.session_state:
    st.session_state.page = "report"

with st.sidebar:
    st.image("enrich_logo.png")
    st.title("DGR Dashboard Controls")

    if st.button("📊 Customer Report"):
        st.session_state.page = "report"

    if st.button("🔴 Live Data"):
        st.session_state.page = "live"


today_date = datetime.now().date()

if st.session_state.page == "report":
    report_date = st.date_input("Select Report Date", today_date)
else:
    report_date = today_date + timedelta(days=1)

month_start = report_date.replace(day=1)


# ---------------------------------------------------------
# REPORT PAGE
# ---------------------------------------------------------
if st.session_state.page == "report":

    customer = st.selectbox("Select Customer", list(CUSTOMER_TABLES.keys()))
    coll = CUSTOMER_TABLES[customer]

    start = month_start.strftime("%d-%b-%Y")
    end = report_date.strftime("%d-%b-%Y")

    st.info(f"Fetching data for {customer} from {start} to {end}...")

    df = fetch_cleaned_data(coll, start, end, customer)

    if df.empty:
        st.error("No data found.")
        st.stop()

    df, inv_cols, irr_col = clean_dataframe(df, customer)

    final_df, daily, monthly, daily_irr, month_irr = get_daily_monthly_data(
        df, inv_cols, month_start, report_date, irr_col, customer
    )

    total_daily, total_monthly, plf = calculate_kpis(customer, daily, monthly)

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Daily (kWh)", f"{total_daily:.2f}")
    c2.metric("PLF (%)", f"{plf:.2f}")
    c3.metric("Monthly Total (kWh)", f"{total_monthly:.2f}")

    st.dataframe(final_df)


# ---------------------------------------------------------
# ✅ LIVE PAGE — ALWAYS FETCH LATEST ROW
# ---------------------------------------------------------
else:
    st.title("🔴 Live Generation Data (All Customers)")
    st.caption("Always shows the LATEST row from each plant.")

    st_autorefresh(interval=60000, key="live_refresh")

    summary = []
    total_all = 0

    for customer, coll in CUSTOMER_TABLES.items():

        df = fetch_latest_row(coll)

        if df.empty:
            summary.append({
                "Plant": customer,
                "Total Daily Generation (kWh)": 0,
                "PLF (%)": 0,
                "Irradiation (kWh/m²)": 0
            })
            continue

        df, inv_cols, irr_col = clean_dataframe(df, customer)

        final_df, daily, monthly, irr, monthirr = get_daily_monthly_data(
            df, inv_cols, month_start, report_date, irr_col, customer
        )

        total_daily, total_monthly, plf = calculate_kpis(customer, daily, monthly)

        total_all += total_daily

        summary.append({
            "Plant": customer,
            "Total Daily Generation (kWh)": round(total_daily, 2),
            "PLF (%)": round(plf, 2),
            "Irradiation (kWh/m²)": round(irr, 2)
        })

    st.metric("Total Generation (All Plants)", f"{round(total_all, 2)} kWh")

    st.dataframe(pd.DataFrame(summary))

    st.caption(f"Last Updated: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
