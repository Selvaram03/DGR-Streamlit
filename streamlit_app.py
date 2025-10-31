# streamlit_app.py
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import pytz

from streamlit_autorefresh import st_autorefresh
from mongo_connector import fetch_cleaned_data
from dgr_generator import clean_dataframe, get_daily_monthly_data, calculate_kpis

IST = pytz.timezone("Asia/Kolkata")

import os
os.environ["STREAMLIT_SERVER_RUN_ON_SAVE"] = "false"
os.environ["STREAMLIT_WATCHDOG"] = "false"

# ---------------- Logging ----------------
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("✅ App started successfully")

# ---------------- Customer Mapping ----------------
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

# ---------------- UI Setup ----------------
st.set_page_config(page_title="DGR Generation Dashboard", layout="wide")

# Sidebar Navigation
if "page" not in st.session_state:
    st.session_state.page = "report"

with st.sidebar:
    st.image("enrich_logo.png", width="stretch")
    st.title("DGR Dashboard Controls")

    if st.button("📊 DGR Customer Report"):
        st.session_state.page = "report"

    if st.button("🔴 Live Generation Data"):
        st.session_state.page = "live"

# ---------------- Common Date Logic ----------------
today_date = datetime.now().date()

if st.session_state.page == "report":
    report_date = st.date_input("Select Report Date", today_date)

elif st.session_state.page == "live":
    report_date = today_date + timedelta(days=1)

month_start = report_date.replace(day=1)

# =====================================================================
# ✅ REPORT PAGE
# =====================================================================
if st.session_state.page == "report":

    customer = st.selectbox("Select Customer", list(CUSTOMER_TABLES.keys()))
    collection_name = CUSTOMER_TABLES[customer]

    start_str = month_start.strftime("%d-%b-%Y")
    end_str = report_date.strftime("%d-%b-%Y")

    st.info(f"Fetching data for **{customer}** from {start_str} to {end_str}...")

    # ✅ Log fetch details
    logger.info(f"🔍 Fetching MongoDB data → Customer={customer}, Collection={collection_name}, Start={start_str}, End={end_str}")

    with st.spinner("Fetching data from MongoDB..."):
        df = fetch_cleaned_data(collection_name, start_str, end_str, customer)

    if df.empty:
        logger.warning("⚠️ No data returned from MongoDB")
        st.error("No data found for this range.")
        st.stop()

    # Process dataframe
    df, inverter_cols, irradiation_col = clean_dataframe(df, customer)

    final_df, daily_generation, monthly_generation, daily_irradiation, monthly_avg_irradiation = get_daily_monthly_data(
        df, inverter_cols, month_start, report_date, irradiation_col, customer
    )

    total_daily_gen, total_monthly_gen, plf_percent = calculate_kpis(
        customer, daily_generation, monthly_generation
    )

    # KPI Row
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Daily Generation (kWh)", f"{total_daily_gen:.2f}")
    col2.metric("PLF % (Yesterday)", f"{plf_percent:.2f}%")
    col3.metric("Total Monthly Generation (kWh)", f"{total_monthly_gen:.2f}")

    # Irradiation KPIs
    if daily_irradiation or monthly_avg_irradiation:
        col4, col5 = st.columns(2)
        if daily_irradiation:
            col4.metric("Daily Irradiation (kWh/m²)", f"{daily_irradiation:.2f}")
        if monthly_avg_irradiation:
            col5.metric("Avg Monthly Irradiation (kWh/m²)", f"{monthly_avg_irradiation:.2f}")

    # Table
    st.subheader("Inverter-wise Generation Details")
    st.dataframe(
        final_df.style.format({
            "Daily Generation (kWh)": "{:.2f}",
            "Monthly Generation (kWh)": "{:.2f}"
        }),
        width="stretch"
    )

    # Excel Export
    safe_day_str = (report_date - timedelta(days=1)).strftime("%Y-%m-%d")
    output_file = f"{customer}_DGR_Report_{safe_day_str}.xlsx"

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        final_df.to_excel(writer, index=False, sheet_name="Generation Report")

        summary_data = {
            "Metric": ["Customer", "Report Date (Yesterday)", "Total Daily Generation (kWh)",
                       "Total Monthly Generation (kWh)", "PLF (%)"],
            "Value": [customer, safe_day_str, round(total_daily_gen, 2),
                      round(total_monthly_gen, 2), round(plf_percent, 2)]
        }

        if daily_irradiation is not None:
            summary_data["Metric"].append("Daily Irradiation (kWh/m²)")
            summary_data["Value"].append(round(daily_irradiation, 2))

        if monthly_avg_irradiation is not None:
            summary_data["Metric"].append("Avg Monthly Irradiation (kWh/m²)")
            summary_data["Value"].append(round(monthly_avg_irradiation, 2))

        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, index=False, sheet_name="Summary")

    with open(output_file, "rb") as f:
        st.download_button(
            "📥 Download DGR Report (Excel)",
            data=f,
            file_name=output_file,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# =====================================================================
# ✅ LIVE PAGE
# =====================================================================
elif st.session_state.page == "live":

    st.title("🔴 Live Generation Data (All Customers)")
    st.caption("Latest generation and irradiation data for all customers.")

    st_autorefresh(interval=60_000, limit=None, key="live_autorefresh")

    summary_list = []
    total_daily_all = 0

    for cust, coll in CUSTOMER_TABLES.items():

        start_str = month_start.strftime("%d-%b-%Y")
        end_str = report_date.strftime("%d-%b-%Y")

        logger.info(f"⏱ Live fetch → {cust}, Collection={coll}")

        df = fetch_cleaned_data(coll, start_str, end_str, cust)

        if df.empty:
            summary_list.append({
                "Plant": cust,
                "Total Daily Generation (kWh)": 0,
                "PLF (%)": 0,
                "Irradiation (kWh/m²)": 0
            })
            continue

        df, inverter_cols, irradiation_col = clean_dataframe(df, cust)

        # PGCIL only latest row
        if cust == "PGCIL":
            df = df.tail(1)

        _, daily_gen, monthly_gen, daily_irr, _ = get_daily_monthly_data(
            df, inverter_cols, month_start, report_date, irradiation_col, cust
        )

        total_daily, _, plf = calculate_kpis(cust, daily_gen, monthly_gen)
        total_daily_all += total_daily

        summary_list.append({
            "Plant": cust,
            "Total Daily Generation (kWh)": round(total_daily, 2),
            "PLF (%)": round(plf, 2),
            "Irradiation (kWh/m²)": round(daily_irr or 0, 2)
        })

    # Total KPI
    st.metric("Total Generation of All Customers (kWh)", f"{round(total_daily_all, 2)}")

    st.dataframe(pd.DataFrame(summary_list), width="stretch")
    st.caption(f"Last refreshed at {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} IST")
