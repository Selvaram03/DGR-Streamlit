# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz
import logging
import os

from streamlit_autorefresh import st_autorefresh
from mongo_connector import fetch_cleaned_data
from dgr_generator import clean_dataframe, get_daily_monthly_data, calculate_kpis

# ✅ Setup
IST = pytz.timezone("Asia/Kolkata")

os.environ["STREAMLIT_SERVER_RUN_ON_SAVE"] = "false"
os.environ["STREAMLIT_WATCHDOG"] = "false"

st.set_page_config(page_title="DGR Generation Dashboard", layout="wide")

# -------------------------------------------------------
# ✅ Customer Table Mapping
# -------------------------------------------------------
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

# -------------------------------------------------------
# ✅ Sidebar Navigation
# -------------------------------------------------------
if "page" not in st.session_state:
    st.session_state.page = "report"

with st.sidebar:
    st.image("enrich_logo.png")
    st.title("DGR Dashboard Controls")

    if st.button("📊 DGR Customer Report"):
        st.session_state.page = "report"
    if st.button("🔴 Live Generation Data"):
        st.session_state.page = "live"

# -------------------------------------------------------
# ✅ Date Controls
# -------------------------------------------------------
today_date = datetime.now(IST).date()

if st.session_state.page == "report":
    # User chooses the 'report_date' (we will compute data_day = report_date - 1)
    report_date = st.date_input("Select Report Date", today_date)
else:
    # For live, align report_date to tomorrow so data_day = today
    report_date = today_date + timedelta(days=1)

# Keep month_start only for Mongo fetch window (get_daily_monthly_data ignores it internally)
month_start = report_date.replace(day=1)

# -------------------------------------------------------
# ✅ Customer Report Page
# -------------------------------------------------------
if st.session_state.page == "report":

    customer = st.selectbox("Select Customer", list(CUSTOMER_TABLES.keys()))
    collection_name = CUSTOMER_TABLES[customer]

    start_str = month_start.strftime("%d-%b-%Y")
    end_str = report_date.strftime("%d-%b-%Y")

    st.info(f"Fetching data for **{customer}** from {start_str} to {end_str}...")

    with st.spinner("Fetching data from MongoDB..."):
        df = fetch_cleaned_data(collection_name, start_str, end_str, customer)

    if df.empty:
        st.error("❌ No data returned from MongoDB.")
        st.stop()

    # ----------------- CLEAN DATA -----------------
    df, inverter_cols, irradiation_col = clean_dataframe(df, customer)

    if df.empty:
        st.error("❌ clean_dataframe() removed all rows.")
        st.stop()

    # ----------------- DAILY/MONTHLY COMPUTATION -----------------
    final_df, daily_generation, monthly_generation, daily_irradiation, monthly_avg_irradiation = \
        get_daily_monthly_data(
            df, inverter_cols, month_start, report_date, irradiation_col, customer
        )

    if final_df.empty:
        st.error("❌ get_daily_monthly_data() returned an empty dataframe.")
        st.stop()

    # ----------------- KPIs -----------------
    total_daily_gen, total_monthly_gen, plf_percent = calculate_kpis(
        customer, daily_generation, monthly_generation
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Daily Generation (kWh)", f"{total_daily_gen:.2f}")
    col2.metric("PLF % (Yesterday)", f"{plf_percent:.2f}%")
    col3.metric("Total Monthly Generation (kWh)", f"{total_monthly_gen:.2f}")

    # Use 'is not None' so 0.00 still displays
    if (daily_irradiation is not None) or (monthly_avg_irradiation is not None):
        col4, col5 = st.columns(2)
        if daily_irradiation is not None:
            col4.metric("Daily Irradiation (kWh/m²)", f"{float(daily_irradiation):.2f}")
        if monthly_avg_irradiation is not None:
            col5.metric("Avg Monthly Irradiation (kWh/m²)", f"{float(monthly_avg_irradiation):.2f}")

    # ----------------- TABLE -----------------
    st.subheader("Inverter-wise Generation Details")
    st.dataframe(final_df.style.format({
        "Daily Generation (kWh)": "{:.2f}",
        "Monthly Generation (kWh)": "{:.2f}"
    }))

    # ----------------- EXPORT -----------------
    data_day_str = (pd.to_datetime(report_date) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    output_file = f"{customer}_DGR_Report_{data_day_str}.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Generation Report")

    with open(output_file, "rb") as f:
        st.download_button(
            "📥 Download DGR Report (Excel)",
            data=f,
            file_name=output_file,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# -------------------------------------------------------
# ✅ LIVE PAGE
# -------------------------------------------------------
else:
    st.title("🔴 Live Generation Data (All Customers)")
    st.caption("Latest generation and irradiation data for all customers.")

    st_autorefresh(interval=60_000, limit=None, key="live_refresh")

    summary_list = []
    total_daily_all = 0

    # Today window for live
    today_start_date = datetime.now(IST).date()
    today_end_date = today_start_date + timedelta(days=1)

    today_start_str = today_start_date.strftime("%d-%b-%Y")
    today_end_str = today_end_date.strftime("%d-%b-%Y")

    for cust, coll in CUSTOMER_TABLES.items():
        df = fetch_cleaned_data(
            coll,
            today_start_str,  # start = today
            today_end_str,    # end = today + 1 day (so data_day = today)
            cust
        )

        if df.empty:
            summary_list.append({
                "Plant": cust,
                "Total Daily Generation (kWh)": 0,
                "PLF (%)": 0,
                "Irradiation (kWh/m²)": 0
            })
            continue

        df, inverter_cols, irradiation_col = clean_dataframe(df, cust)

        if cust == "PGCIL":
            # Keep latest row; daily calc will pick today's row anyway
            df = df.tail(1)

        _, daily_gen, monthly_gen, daily_irr, _ = get_daily_monthly_data(
            df, inverter_cols, month_start, today_end_date, irradiation_col, cust
        )

        total_daily, _, plf = calculate_kpis(cust, daily_gen, monthly_gen)
        total_daily_all += total_daily

        summary_list.append({
            "Plant": cust,
            "Total Daily Generation (kWh)": round(total_daily, 2),
            "PLF (%)": round(plf, 2),
            "Irradiation (kWh/m²)": round(float(daily_irr or 0), 2)
        })

    st.metric("Total Generation of All Customers (kWh)", f"{round(total_daily_all, 2)}")

    summary_df = pd.DataFrame(summary_list)
    st.dataframe(summary_df)
    st.caption(f"Last refreshed at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
